const std = @import("std");
const os = @import("os.zig");

/// Rollback Journal for ACID compliance.
///
/// Before modifying any database page, the original content is saved to a
/// journal file (`<db_path>-journal`). On commit the database is fsynced and
/// the journal deleted. On crash recovery (hot journal detected at startup)
/// the original pages are restored from the journal.
///
/// Journal file format:
///   [Header: 24 bytes]
///     magic:     "ZQLJ" (4 bytes)
///     page_count: u32 — number of page records in journal
///     page_size:  u32 — database page size
///     db_pages:   u32 — original database page count
///     reserved:   12 bytes (zeroed)
///
///   [Page records: repeated page_count times]
///     page_id:   u32
///     page_data: [page_size]u8

// ═══════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════

const JOURNAL_MAGIC = "ZQLJ";
const JOURNAL_HEADER_SIZE: usize = 24;
const PAGE_RECORD_HEADER: usize = 4; // page_id u32

// ═══════════════════════════════════════════════════════════════════════════
// Errors
// ═══════════════════════════════════════════════════════════════════════════

pub const JournalError = error{
    JournalOpenFailed,
    JournalWriteFailed,
    JournalReadFailed,
    JournalSyncFailed,
    JournalCorrupt,
    NotInTransaction,
    AlreadyInTransaction,
};

// ═══════════════════════════════════════════════════════════════════════════
// Journal
// ═══════════════════════════════════════════════════════════════════════════

pub const Journal = struct {
    db_path: []const u8,
    journal_path_buf: [4112]u8, // db_path + "-journal\0"
    journal_path_len: usize,
    page_size: u32,
    db_file: *os.FileHandle,

    // Transaction state
    active: bool,
    journal_fd: ?std.posix.fd_t,
    journaled_pages: std.AutoHashMap(u32, void),
    page_count: u32, // number of pages written to journal
    original_db_pages: u32, // db page count at begin()

    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, db_path: []const u8, page_size: u32, db_file: *os.FileHandle) Self {
        var path_buf: [4112]u8 = undefined;
        const suffix = "-journal";
        const jpath_len = db_path.len + suffix.len;
        @memcpy(path_buf[0..db_path.len], db_path);
        @memcpy(path_buf[db_path.len..][0..suffix.len], suffix);
        path_buf[jpath_len] = 0;

        return Self{
            .db_path = db_path,
            .journal_path_buf = path_buf,
            .journal_path_len = jpath_len,
            .page_size = page_size,
            .db_file = db_file,
            .active = false,
            .journal_fd = null,
            .journaled_pages = std.AutoHashMap(u32, void).init(allocator),
            .page_count = 0,
            .original_db_pages = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.active) {
            self.rollback() catch {};
        }
        self.journaled_pages.deinit();
    }

    fn journalPath(self: *const Self) [*:0]const u8 {
        return @ptrCast(self.journal_path_buf[0..self.journal_path_len]);
    }

    // ─── Transaction lifecycle ──────────────────────────────────────

    /// Begin a new transaction: create journal file and write header.
    pub fn begin(self: *Self) JournalError!void {
        if (self.active) return JournalError.AlreadyInTransaction;

        // Create journal file
        const flags: std.posix.O = .{
            .ACCMODE = .RDWR,
            .CREAT = true,
            .TRUNC = true,
        };
        const fd = std.posix.openZ(self.journalPath(), flags, 0o644) catch
            return JournalError.JournalOpenFailed;

        self.journal_fd = fd;
        self.page_count = 0;
        self.original_db_pages = self.db_file.pageCount();
        self.journaled_pages.clearRetainingCapacity();

        // Write header
        var header: [JOURNAL_HEADER_SIZE]u8 = std.mem.zeroes([JOURNAL_HEADER_SIZE]u8);
        @memcpy(header[0..4], JOURNAL_MAGIC);
        std.mem.writeInt(u32, header[4..8], 0, .little); // page_count (updated on commit)
        std.mem.writeInt(u32, header[8..12], self.page_size, .little);
        std.mem.writeInt(u32, header[12..16], self.original_db_pages, .little);

        _ = os.doPwrite(fd, &header, 0) catch return JournalError.JournalWriteFailed;

        // Fsync the journal header
        std.posix.fdatasync(fd) catch return JournalError.JournalSyncFailed;

        self.active = true;
    }

    /// Save the original content of a page before it is modified.
    /// No-op if the page has already been journaled in this transaction.
    pub fn journalPage(self: *Self, page_id: u32, original_data: []const u8) JournalError!void {
        if (!self.active) return JournalError.NotInTransaction;

        // Skip if already journaled
        if (self.journaled_pages.contains(page_id)) return;

        const fd = self.journal_fd orelse return JournalError.NotInTransaction;

        // Write page record: [page_id: u32][page_data: page_size bytes]
        const record_offset: u64 = JOURNAL_HEADER_SIZE +
            @as(u64, self.page_count) * (@as(u64, PAGE_RECORD_HEADER) + @as(u64, self.page_size));

        // Build combined write via scatter-gather (single syscall)
        var id_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &id_buf, page_id, .little);

        const iovecs = [_]std.posix.iovec_const{
            .{ .base = &id_buf, .len = 4 },
            .{ .base = original_data.ptr, .len = original_data.len },
        };
        _ = os.doPwritev(fd, &iovecs, record_offset) catch return JournalError.JournalWriteFailed;

        self.page_count += 1;
        self.journaled_pages.put(page_id, {}) catch return JournalError.JournalWriteFailed;
    }

    /// Commit: fsync journal, update header page count, fsync journal again,
    /// then fsync database, then delete journal.
    pub fn commit(self: *Self) JournalError!void {
        if (!self.active) return JournalError.NotInTransaction;

        const fd = self.journal_fd orelse return JournalError.NotInTransaction;

        // Update header with final page count
        var count_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &count_buf, self.page_count, .little);
        _ = os.doPwrite(fd, &count_buf, 4) catch return JournalError.JournalWriteFailed;

        // Fsync journal (ensures all journal data is durable before we modify db)
        std.posix.fdatasync(fd) catch return JournalError.JournalSyncFailed;

        // Flush dirty pages to database and fsync
        self.db_file.sync() catch return JournalError.JournalSyncFailed;

        // Delete journal (transaction is committed)
        self.deleteJournal();

        self.active = false;
    }

    /// Rollback: restore original pages from journal, delete journal.
    pub fn rollback(self: *Self) JournalError!void {
        if (!self.active) return JournalError.NotInTransaction;

        const fd = self.journal_fd orelse return JournalError.NotInTransaction;

        // Read each journaled page and write it back to the database
        var i: u32 = 0;
        while (i < self.page_count) : (i += 1) {
            const record_offset: u64 = JOURNAL_HEADER_SIZE +
                @as(u64, i) * (@as(u64, PAGE_RECORD_HEADER) + @as(u64, self.page_size));

            // Read page_id
            var id_buf: [4]u8 = undefined;
            _ = os.doPread(fd, &id_buf, record_offset) catch return JournalError.JournalReadFailed;
            const page_id = std.mem.readInt(u32, &id_buf, .little);

            // Read page data
            const page_buf = os.allocAlignedPage(self.page_size) catch return JournalError.JournalReadFailed;
            defer os.freeAlignedPage(page_buf);
            _ = os.doPread(fd, page_buf, record_offset + PAGE_RECORD_HEADER) catch
                return JournalError.JournalReadFailed;

            // Write original page back to database
            self.db_file.writePage(page_id, page_buf) catch return JournalError.JournalWriteFailed;
        }

        // Truncate database to original size if we added pages
        if (self.db_file.pageCount() > self.original_db_pages) {
            self.db_file.truncate(self.original_db_pages) catch {};
        }

        // Fsync database and delete journal
        self.db_file.sync() catch {};
        self.deleteJournal();

        self.active = false;
    }

    /// Check for a hot journal (leftover from crash) and recover.
    /// Call this at startup before any reads/writes.
    pub fn hotJournalRecovery(self: *Self) JournalError!bool {
        // Try to open existing journal file
        const flags: std.posix.O = .{ .ACCMODE = .RDONLY };
        const fd = std.posix.openZ(self.journalPath(), flags, 0) catch return false;
        defer std.posix.close(fd);

        // Read and validate header
        var header: [JOURNAL_HEADER_SIZE]u8 = undefined;
        _ = os.doPread(fd, &header, 0) catch return false;

        if (!std.mem.eql(u8, header[0..4], JOURNAL_MAGIC)) return false;

        const jpage_count = std.mem.readInt(u32, header[4..8], .little);
        const jpage_size = std.mem.readInt(u32, header[8..12], .little);
        const db_pages_orig = std.mem.readInt(u32, header[12..16], .little);

        if (jpage_size != self.page_size) return JournalError.JournalCorrupt;
        if (jpage_count == 0) {
            // Empty journal — just delete it
            os.deleteFile(self.journal_path_buf[0..self.journal_path_len]);
            return false;
        }

        // Replay: restore each journaled page
        var i: u32 = 0;
        while (i < jpage_count) : (i += 1) {
            const record_offset: u64 = JOURNAL_HEADER_SIZE +
                @as(u64, i) * (@as(u64, PAGE_RECORD_HEADER) + @as(u64, jpage_size));

            var id_buf: [4]u8 = undefined;
            _ = os.doPread(fd, &id_buf, record_offset) catch return JournalError.JournalReadFailed;
            const page_id = std.mem.readInt(u32, &id_buf, .little);

            const page_buf = os.allocAlignedPage(jpage_size) catch return JournalError.JournalReadFailed;
            defer os.freeAlignedPage(page_buf);
            _ = os.doPread(fd, page_buf, record_offset + PAGE_RECORD_HEADER) catch
                return JournalError.JournalReadFailed;

            self.db_file.writePage(page_id, page_buf) catch return JournalError.JournalWriteFailed;
        }

        // Truncate to original size
        if (self.db_file.pageCount() > db_pages_orig) {
            self.db_file.truncate(db_pages_orig) catch {};
        }

        // Fsync and delete journal
        self.db_file.sync() catch {};
        os.deleteFile(self.journal_path_buf[0..self.journal_path_len]);

        return true; // recovery was performed
    }

    // ─── Internal ───────────────────────────────────────────────────

    fn deleteJournal(self: *Self) void {
        if (self.journal_fd) |fd| {
            std.posix.close(fd);
            self.journal_fd = null;
        }
        os.deleteFile(self.journal_path_buf[0..self.journal_path_len]);
        self.journaled_pages.clearRetainingCapacity();
        self.page_count = 0;
    }
};
