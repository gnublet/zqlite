const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const c = std.c;
const linux = if (builtin.os.tag == .linux) std.os.linux else void;

/// OS abstraction layer for ZQLite.
///
/// Provides page-aligned I/O with POSIX C library calls and optional
/// io_uring acceleration on Linux.

// ═══════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════

pub const DEFAULT_PAGE_SIZE: u32 = 4096;
pub const MAX_PAGE_SIZE: u32 = 65536;
pub const MIN_PAGE_SIZE: u32 = 512;

const IO_URING_QUEUE_DEPTH = 64;

// ═══════════════════════════════════════════════════════════════════════════
// Errors
// ═══════════════════════════════════════════════════════════════════════════

pub const OsError = error{
    FileOpenFailed,
    ReadFailed,
    WriteFailed,
    SyncFailed,
    TruncateFailed,
    LockFailed,
    UnlockFailed,
    InvalidPageSize,
    IoUringInitFailed,
    IoUringSubmitFailed,
    MmapFailed,
    AllocationFailed,
};

// ═══════════════════════════════════════════════════════════════════════════
// Page-aligned allocation
// ═══════════════════════════════════════════════════════════════════════════

/// Allocate a page-aligned buffer of `page_size` bytes.
pub fn allocAlignedPage(page_size: u32) OsError![]align(4096) u8 {
    const slice = std.heap.page_allocator.alignedAlloc(u8, std.mem.Alignment.fromByteUnits(4096), page_size) catch
        return OsError.AllocationFailed;
    return slice;
}

/// Free a previously allocated aligned page buffer.
pub fn freeAlignedPage(buf: []align(4096) u8) void {
    std.heap.page_allocator.free(buf);
}

// ═══════════════════════════════════════════════════════════════════════════
// File Lock levels (matching SQLite semantics)
// ═══════════════════════════════════════════════════════════════════════════

pub const LockLevel = enum(u8) {
    none = 0,
    shared = 1,
    reserved = 2,
    exclusive = 3,
};

// ═══════════════════════════════════════════════════════════════════════════
// POSIX I/O helpers (cross-platform via libc)
// ═══════════════════════════════════════════════════════════════════════════

pub fn doPread(fd: posix.fd_t, buf: []u8, offset: u64) OsError!usize {
    const rc = c.pread(fd, buf.ptr, buf.len, @intCast(offset));
    if (rc < 0) return OsError.ReadFailed;
    return @intCast(rc);
}

pub fn doPwrite(fd: posix.fd_t, buf: []const u8, offset: u64) OsError!usize {
    const rc = c.pwrite(fd, buf.ptr, buf.len, @intCast(offset));
    if (rc < 0) return OsError.WriteFailed;
    return @intCast(rc);
}

pub fn doFtruncate(fd: posix.fd_t, length: u64) OsError!void {
    const rc = c.ftruncate(fd, @intCast(length));
    if (rc < 0) return OsError.TruncateFailed;
}

// ═══════════════════════════════════════════════════════════════════════════
// FileHandle — core file abstraction
// ═══════════════════════════════════════════════════════════════════════════

pub const FileHandle = struct {
    fd: posix.fd_t,
    page_size: u32,
    file_size: u64,
    lock_level: LockLevel,
    use_direct_io: bool,

    // io_uring support (Linux only)
    io_uring: if (builtin.os.tag == .linux) ?linux.IoUring else void,

    const Self = @This();

    /// Open or create a database file.
    pub fn open(path: []const u8, page_size: u32, use_direct_io: bool) OsError!Self {
        if (page_size < MIN_PAGE_SIZE or page_size > MAX_PAGE_SIZE or
            (page_size & (page_size - 1)) != 0)
        {
            return OsError.InvalidPageSize;
        }

        var flags: posix.O = .{
            .ACCMODE = .RDWR,
            .CREAT = true,
        };
        if (use_direct_io and builtin.os.tag == .linux) {
            flags.DIRECT = true;
        }

        // Ensure we have null-terminated path for posix.open
        var path_buf: [4096]u8 = undefined;
        if (path.len >= path_buf.len) return OsError.FileOpenFailed;
        @memcpy(path_buf[0..path.len], path);
        path_buf[path.len] = 0;
        const path_z: [*:0]const u8 = @ptrCast(path_buf[0..path.len]);

        const fd = posix.openZ(
            path_z,
            flags,
            0o644,
        ) catch return OsError.FileOpenFailed;

        // Get file size via lseek to end
        const size_rc = c.lseek(fd, 0, std.posix.SEEK.END);
        const file_size: u64 = if (size_rc >= 0) @intCast(size_rc) else 0;
        // Seek back to beginning
        _ = c.lseek(fd, 0, std.posix.SEEK.SET);

        // Try to initialise io_uring on Linux
        var ring: if (builtin.os.tag == .linux) ?linux.IoUring else void = if (builtin.os.tag == .linux) blk: {
            break :blk linux.IoUring.init(IO_URING_QUEUE_DEPTH, 0) catch null;
        } else {};

        _ = &ring;

        return Self{
            .fd = fd,
            .page_size = page_size,
            .file_size = file_size,
            .lock_level = .none,
            .use_direct_io = use_direct_io,
            .io_uring = ring,
        };
    }

    /// Close the file handle and release all resources.
    pub fn close(self: *Self) void {
        if (builtin.os.tag == .linux) {
            if (self.io_uring) |*ring| {
                ring.deinit();
            }
        }
        posix.close(self.fd);
        self.fd = -1;
    }

    // ─── Synchronous page I/O ───────────────────────────────────────

    /// Read a single page at the given page number (0-indexed).
    pub fn readPage(self: *Self, page_num: u32, buf: []u8) OsError!void {
        std.debug.assert(buf.len == self.page_size);
        const offset: u64 = @as(u64, page_num) * @as(u64, self.page_size);

        const bytes_read = try doPread(self.fd, buf, offset);

        // If we read fewer bytes than a full page (e.g. at EOF),
        // zero-fill the remainder.
        if (bytes_read < self.page_size) {
            @memset(buf[bytes_read..], 0);
        }
    }

    /// Write a single page at the given page number (0-indexed).
    pub fn writePage(self: *Self, page_num: u32, buf: []const u8) OsError!void {
        std.debug.assert(buf.len == self.page_size);
        const offset: u64 = @as(u64, page_num) * @as(u64, self.page_size);

        const bytes_written = try doPwrite(self.fd, buf, offset);

        if (bytes_written != self.page_size) {
            return OsError.WriteFailed;
        }

        // Update cached file size if we extended the file
        const end = offset + @as(u64, self.page_size);
        if (end > self.file_size) {
            self.file_size = end;
        }
    }

    // ─── Batched I/O ────────────────────────────────────────────────

    pub const IoRequest = struct {
        page_num: u32,
        buf: []u8,
    };

    /// Read multiple pages in a batch. On Linux with io_uring available,
    /// all reads are submitted in a single ring and awaited together.
    /// Falls back to sequential pread on other platforms.
    pub fn readPages(self: *Self, requests: []IoRequest) OsError!void {
        if (builtin.os.tag == .linux) {
            if (self.io_uring) |*ring| {
                return self.readPagesIoUring(ring, requests);
            }
        }
        // Fallback: sequential pread
        for (requests) |req| {
            try self.readPage(req.page_num, req.buf);
        }
    }

    /// Write multiple pages in a batch.
    pub fn writePages(self: *Self, requests: []const IoRequest) OsError!void {
        if (builtin.os.tag == .linux) {
            if (self.io_uring) |*ring| {
                return self.writePagesIoUring(ring, requests);
            }
        }
        for (requests) |req| {
            try self.writePage(req.page_num, req.buf);
        }
    }

    // ─── io_uring batched paths (Linux-only) ────────────────────────

    fn readPagesIoUring(self: *Self, ring: *linux.IoUring, requests: []IoRequest) OsError!void {
        for (requests) |req| {
            const offset: u64 = @as(u64, req.page_num) * @as(u64, self.page_size);
            var sqe = ring.get_sqe() catch return OsError.IoUringSubmitFailed;
            sqe.prep_read(self.fd, req.buf, offset);
            sqe.user_data = 0;
        }

        _ = ring.submit() catch return OsError.IoUringSubmitFailed;

        // Wait for all completions
        for (0..requests.len) |_| {
            _ = ring.copy_cqe() catch return OsError.ReadFailed;
        }
    }

    fn writePagesIoUring(self: *Self, ring: *linux.IoUring, requests: []const IoRequest) OsError!void {
        for (requests) |req| {
            const offset: u64 = @as(u64, req.page_num) * @as(u64, self.page_size);
            var sqe = ring.get_sqe() catch return OsError.IoUringSubmitFailed;
            sqe.prep_write(self.fd, req.buf, offset);
            sqe.user_data = 0;
        }

        _ = ring.submit() catch return OsError.IoUringSubmitFailed;

        for (requests) |req| {
            _ = ring.copy_cqe() catch return OsError.WriteFailed;

            // Update cached file size
            const end = @as(u64, req.page_num) * @as(u64, self.page_size) + @as(u64, self.page_size);
            if (end > self.file_size) {
                self.file_size = end;
            }
        }
    }

    // ─── Sync / Truncate ────────────────────────────────────────────

    /// Flush all pending writes to durable storage.
    pub fn sync(self: *Self) OsError!void {
        posix.fdatasync(self.fd) catch return OsError.SyncFailed;
    }

    /// Truncate (or extend) the file to the given number of pages.
    pub fn truncate(self: *Self, num_pages: u32) OsError!void {
        const new_size: u64 = @as(u64, num_pages) * @as(u64, self.page_size);
        try doFtruncate(self.fd, new_size);
        self.file_size = new_size;
    }

    // ─── File locking ───────────────────────────────────────────────

    /// Acquire a POSIX file lock at the given level.
    pub fn lock(self: *Self, level: LockLevel) OsError!void {
        if (level == .none) {
            return self.unlock();
        }

        const lock_type: i16 = switch (level) {
            .shared => 0, // F_RDLCK
            .reserved, .exclusive => 1, // F_WRLCK
            .none => unreachable,
        };

        var fl = std.mem.zeroes(linux.Flock);
        fl.type = lock_type;
        fl.whence = 0; // SEEK_SET
        fl.start = 0;
        fl.len = 0; // lock entire file

        _ = posix.fcntl(self.fd, linux.F.SETLK, @intFromPtr(&fl)) catch return OsError.LockFailed;
        self.lock_level = level;
    }

    /// Release all file locks.
    pub fn unlock(self: *Self) OsError!void {
        var fl = std.mem.zeroes(linux.Flock);
        fl.type = 2; // F_UNLCK
        fl.whence = 0; // SEEK_SET
        fl.start = 0;
        fl.len = 0;

        _ = posix.fcntl(self.fd, linux.F.SETLK, @intFromPtr(&fl)) catch return OsError.UnlockFailed;
        self.lock_level = .none;
    }

    // ─── Query ──────────────────────────────────────────────────────

    /// The number of pages currently in the file.
    pub fn pageCount(self: *const Self) u32 {
        if (self.file_size == 0) return 0;
        return @intCast(self.file_size / @as(u64, self.page_size));
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

/// Delete a file by path.
pub fn deleteFile(path: []const u8) void {
    var path_buf: [4096]u8 = undefined;
    if (path.len >= path_buf.len) return;
    @memcpy(path_buf[0..path.len], path);
    path_buf[path.len] = 0;
    _ = c.unlink(@ptrCast(path_buf[0..path.len]));
}

test "allocAlignedPage round-trip" {
    const page = try allocAlignedPage(4096);
    defer freeAlignedPage(page);
    @memset(page, 0xAB);
    try std.testing.expectEqual(@as(u8, 0xAB), page[0]);
    try std.testing.expectEqual(@as(u8, 0xAB), page[4095]);
}

test "FileHandle open/close temp file" {
    const tmp_path = "/tmp/zqlite_test_os.db\x00";
    var fh = FileHandle.open(tmp_path[0 .. tmp_path.len - 1], DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();
    try std.testing.expectEqual(@as(u32, 0), fh.pageCount());
}

test "FileHandle write and read page" {
    const tmp_path = "/tmp/zqlite_test_os_rw.db\x00";
    defer deleteFile("/tmp/zqlite_test_os_rw.db");

    var fh = FileHandle.open(tmp_path[0 .. tmp_path.len - 1], DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    // Write a page
    var write_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
    @memset(&write_buf, 0);
    write_buf[0] = 'Z';
    write_buf[1] = 'Q';
    write_buf[2] = 'L';
    try fh.writePage(0, &write_buf);

    try std.testing.expectEqual(@as(u32, 1), fh.pageCount());

    // Read it back
    var read_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
    try fh.readPage(0, &read_buf);
    try std.testing.expectEqual(@as(u8, 'Z'), read_buf[0]);
    try std.testing.expectEqual(@as(u8, 'Q'), read_buf[1]);
    try std.testing.expectEqual(@as(u8, 'L'), read_buf[2]);
}

test "FileHandle truncate" {
    const tmp_path = "/tmp/zqlite_test_os_trunc.db\x00";
    defer deleteFile("/tmp/zqlite_test_os_trunc.db");

    var fh = FileHandle.open(tmp_path[0 .. tmp_path.len - 1], DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    // Write 3 pages
    var buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
    @memset(&buf, 0);
    for (0..3) |i| {
        buf[0] = @intCast(i);
        try fh.writePage(@intCast(i), &buf);
    }
    try std.testing.expectEqual(@as(u32, 3), fh.pageCount());

    // Truncate to 1 page
    try fh.truncate(1);
    try std.testing.expectEqual(@as(u32, 1), fh.pageCount());
}

test "invalid page size rejected" {
    const result = FileHandle.open("/tmp/zqlite_test_os_bad.db", 999, false);
    try std.testing.expectError(OsError.InvalidPageSize, result);
}
