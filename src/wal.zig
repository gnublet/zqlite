const std = @import("std");
const os = @import("os.zig");

/// Write-Ahead Log (WAL) implementation for ZQLite.
///
/// The WAL ensures durability: changes are first written to the WAL file,
/// then lazily checkpointed back to the main database file. This allows
/// concurrent readers during writes.
///
/// WAL frame format:
///   page_id  : u32  (4 bytes)
///   db_size  : u32  (4 bytes)  — total DB size in pages at commit
///   checksum : u64  (8 bytes)  — running checksum for corruption detection
///   page_data: [page_size]u8

// ═══════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════

pub const WAL_HEADER_SIZE: u32 = 32;
pub const FRAME_HEADER_SIZE: u32 = 16;

const WAL_MAGIC: u32 = 0x5A514C57; // "ZQLW"

// ═══════════════════════════════════════════════════════════════════════════
// Errors
// ═══════════════════════════════════════════════════════════════════════════

pub const WalError = error{
    OpenFailed,
    WriteFailed,
    ReadFailed,
    CorruptedFrame,
    ChecksumMismatch,
    CheckpointFailed,
    AllocationFailed,
};

// ═══════════════════════════════════════════════════════════════════════════
// WAL Frame header
// ═══════════════════════════════════════════════════════════════════════════

pub const FrameHeader = struct {
    page_id: u32,
    db_size: u32,
    checksum: u64,

    pub fn encode(self: FrameHeader) [FRAME_HEADER_SIZE]u8 {
        var buf: [FRAME_HEADER_SIZE]u8 = undefined;
        std.mem.writeInt(u32, buf[0..4], self.page_id, .big);
        std.mem.writeInt(u32, buf[4..8], self.db_size, .big);
        std.mem.writeInt(u64, buf[8..16], self.checksum, .big);
        return buf;
    }

    pub fn decode(buf: *const [FRAME_HEADER_SIZE]u8) FrameHeader {
        return .{
            .page_id = std.mem.readInt(u32, buf[0..4], .big),
            .db_size = std.mem.readInt(u32, buf[4..8], .big),
            .checksum = std.mem.readInt(u64, buf[8..16], .big),
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// WAL file header
// ═══════════════════════════════════════════════════════════════════════════

pub const WalHeader = struct {
    magic: u32,
    page_size: u32,
    checkpoint_seq: u32,
    salt1: u32,
    salt2: u32,
    frame_count: u32,
    _reserved: [8]u8,

    pub fn encode(self: WalHeader) [WAL_HEADER_SIZE]u8 {
        var buf: [WAL_HEADER_SIZE]u8 = undefined;
        std.mem.writeInt(u32, buf[0..4], self.magic, .big);
        std.mem.writeInt(u32, buf[4..8], self.page_size, .big);
        std.mem.writeInt(u32, buf[8..12], self.checkpoint_seq, .big);
        std.mem.writeInt(u32, buf[12..16], self.salt1, .big);
        std.mem.writeInt(u32, buf[16..20], self.salt2, .big);
        std.mem.writeInt(u32, buf[20..24], self.frame_count, .big);
        @memcpy(buf[24..32], &self._reserved);
        return buf;
    }

    pub fn decode(buf: *const [WAL_HEADER_SIZE]u8) WalHeader {
        return .{
            .magic = std.mem.readInt(u32, buf[0..4], .big),
            .page_size = std.mem.readInt(u32, buf[4..8], .big),
            .checkpoint_seq = std.mem.readInt(u32, buf[8..12], .big),
            .salt1 = std.mem.readInt(u32, buf[12..16], .big),
            .salt2 = std.mem.readInt(u32, buf[16..20], .big),
            .frame_count = std.mem.readInt(u32, buf[20..24], .big),
            ._reserved = buf[24..32].*,
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Checksum (fast FNV-1a based rolling checksum)
// ═══════════════════════════════════════════════════════════════════════════

pub fn computeChecksum(data: []const u8, prev: u64) u64 {
    var hash = prev;
    if (hash == 0) hash = 0xcbf29ce484222325; // FNV offset basis
    for (data) |byte| {
        hash ^= byte;
        hash *%= 0x100000001b3; // FNV prime
    }
    return hash;
}

// ═══════════════════════════════════════════════════════════════════════════
// WAL — main struct
// ═══════════════════════════════════════════════════════════════════════════

pub const Wal = struct {
    wal_file: os.FileHandle,
    page_size: u32,
    frame_count: u32,
    running_checksum: u64,
    checkpoint_seq: u32,

    /// Index: page_id → most recent frame number (1-based)
    frame_index: std.AutoHashMap(u32, u32),
    allocator: std.mem.Allocator,

    const Self = @This();

    /// Open or create a WAL file.
    pub fn open(allocator: std.mem.Allocator, wal_path: []const u8, page_size: u32) WalError!Self {
        const wal_fh = os.FileHandle.open(wal_path, page_size, false) catch
            return WalError.OpenFailed;

        var wal = Self{
            .wal_file = wal_fh,
            .page_size = page_size,
            .frame_count = 0,
            .running_checksum = 0,
            .checkpoint_seq = 0,
            .frame_index = std.AutoHashMap(u32, u32).init(allocator),
            .allocator = allocator,
        };

        // If WAL file exists and has data, recover
        if (wal_fh.file_size >= WAL_HEADER_SIZE) {
            wal.recover() catch {
                // Recovery failed — start fresh
                wal.frame_count = 0;
                wal.running_checksum = 0;
                wal.frame_index.clearAndFree();
            };
        } else {
            // Write initial header
            wal.writeHeader() catch return WalError.WriteFailed;
        }

        return wal;
    }

    /// Close the WAL.
    pub fn close(self: *Self) void {
        self.wal_file.close();
        self.frame_index.deinit();
    }

    /// Append a frame to the WAL.
    pub fn writeFrame(self: *Self, page_id: u32, db_size: u32, page_data: []const u8) WalError!void {
        std.debug.assert(page_data.len == self.page_size);

        // Compute running checksum over frame header + page data
        var header_bytes: [FRAME_HEADER_SIZE]u8 = undefined;
        std.mem.writeInt(u32, header_bytes[0..4], page_id, .big);
        std.mem.writeInt(u32, header_bytes[4..8], db_size, .big);

        var cksum = computeChecksum(header_bytes[0..8], self.running_checksum);
        cksum = computeChecksum(page_data, cksum);

        const frame_header = FrameHeader{
            .page_id = page_id,
            .db_size = db_size,
            .checksum = cksum,
        };
        const encoded_header = frame_header.encode();

        // Calculate file offset for this frame
        const frame_offset: u64 = @as(u64, WAL_HEADER_SIZE) +
            @as(u64, self.frame_count) * (@as(u64, FRAME_HEADER_SIZE) + @as(u64, self.page_size));

        // Write frame header
        _ = os.doPwrite(self.wal_file.fd, &encoded_header, frame_offset) catch
            return WalError.WriteFailed;

        // Write page data
        _ = os.doPwrite(self.wal_file.fd, page_data, frame_offset + FRAME_HEADER_SIZE) catch
            return WalError.WriteFailed;

        self.frame_count += 1;
        self.running_checksum = cksum;

        // Update index
        self.frame_index.put(page_id, self.frame_count) catch
            return WalError.AllocationFailed;

        // Update header's frame count
        self.writeHeader() catch return WalError.WriteFailed;
    }

    /// Read a page from the WAL if it exists there.
    /// Returns true if found, false if not in WAL.
    pub fn readPage(self: *Self, page_id: u32, buf: []u8) WalError!bool {
        const frame_num = self.frame_index.get(page_id) orelse return false;

        // Frame numbers are 1-based
        const frame_offset: u64 = @as(u64, WAL_HEADER_SIZE) +
            @as(u64, frame_num - 1) * (@as(u64, FRAME_HEADER_SIZE) + @as(u64, self.page_size));

        _ = os.doPread(self.wal_file.fd, buf[0..self.page_size], frame_offset + FRAME_HEADER_SIZE) catch return WalError.ReadFailed;

        return true;
    }

    /// Checkpoint: write all WAL'd pages back to the main database file,
    /// then reset the WAL.
    pub fn checkpoint(self: *Self, db_file: *os.FileHandle) WalError!void {
        const page_buf = os.allocAlignedPage(self.page_size) catch
            return WalError.AllocationFailed;
        defer os.freeAlignedPage(page_buf);

        // For each unique page in the index, read the latest frame and write to DB
        var it = self.frame_index.iterator();
        while (it.next()) |entry| {
            const page_id = entry.key_ptr.*;

            const found = self.readPage(page_id, page_buf) catch
                return WalError.ReadFailed;
            if (!found) continue;

            db_file.writePage(page_id, page_buf) catch
                return WalError.CheckpointFailed;
        }

        // Sync the database file
        db_file.sync() catch return WalError.CheckpointFailed;

        // Reset WAL
        self.frame_count = 0;
        self.running_checksum = 0;
        self.checkpoint_seq += 1;
        self.frame_index.clearAndFree();

        // Truncate WAL file and write fresh header
        self.wal_file.truncate(0) catch {};
        self.wal_file.file_size = 0;
        self.writeHeader() catch return WalError.WriteFailed;
    }

    /// Recover frames from an existing WAL file on open.
    fn recover(self: *Self) WalError!void {
        // Read WAL header
        var header_buf: [WAL_HEADER_SIZE]u8 = undefined;
        _ = os.doPread(self.wal_file.fd, &header_buf, 0) catch
            return WalError.ReadFailed;

        const header = WalHeader.decode(&header_buf);
        if (header.magic != WAL_MAGIC) return WalError.CorruptedFrame;
        if (header.page_size != self.page_size) return WalError.CorruptedFrame;

        self.checkpoint_seq = header.checkpoint_seq;

        // Read each frame, verifying checksums
        var frame_num: u32 = 0;
        var running_cksum: u64 = 0;
        var frame_header_buf: [FRAME_HEADER_SIZE]u8 = undefined;
        var page_buf = os.allocAlignedPage(self.page_size) catch
            return WalError.AllocationFailed;
        defer os.freeAlignedPage(page_buf);

        while (frame_num < header.frame_count) : (frame_num += 1) {
            const frame_offset: u64 = @as(u64, WAL_HEADER_SIZE) +
                @as(u64, frame_num) * (@as(u64, FRAME_HEADER_SIZE) + @as(u64, self.page_size));

            // Read frame header
            const hdr_read = os.doPread(self.wal_file.fd, &frame_header_buf, frame_offset) catch
                break;
            if (hdr_read < FRAME_HEADER_SIZE) break;

            // Read page data
            const pg_read = os.doPread(self.wal_file.fd, page_buf, frame_offset + FRAME_HEADER_SIZE) catch break;
            if (pg_read < self.page_size) break;

            const fh = FrameHeader.decode(&frame_header_buf);

            // Verify checksum
            var expected_cksum = computeChecksum(frame_header_buf[0..8], running_cksum);
            expected_cksum = computeChecksum(page_buf[0..self.page_size], expected_cksum);
            if (fh.checksum != expected_cksum) break; // stop recovery at first bad frame

            running_cksum = fh.checksum;

            self.frame_index.put(fh.page_id, frame_num + 1) catch
                return WalError.AllocationFailed;
        }

        self.frame_count = frame_num;
        self.running_checksum = running_cksum;
    }

    /// Write the WAL file header.
    fn writeHeader(self: *Self) WalError!void {
        const header = WalHeader{
            .magic = WAL_MAGIC,
            .page_size = self.page_size,
            .checkpoint_seq = self.checkpoint_seq,
            .salt1 = 0,
            .salt2 = 0,
            .frame_count = self.frame_count,
            ._reserved = [_]u8{0} ** 8,
        };
        const encoded = header.encode();
        _ = os.doPwrite(self.wal_file.fd, &encoded, 0) catch
            return WalError.WriteFailed;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

test "WAL write and read frame" {
    const wal_path = "/tmp/zqlite_test_wal.wal";
    defer os.deleteFile(wal_path);

    var wal = Wal.open(std.testing.allocator, wal_path, os.DEFAULT_PAGE_SIZE) catch
        return error.SkipZigTest;
    defer wal.close();

    var page_data: [os.DEFAULT_PAGE_SIZE]u8 = undefined;
    @memset(&page_data, 0xAA);
    page_data[0] = 0x42;

    try wal.writeFrame(5, 10, &page_data);
    try std.testing.expectEqual(@as(u32, 1), wal.frame_count);

    // Read back
    var read_buf: [os.DEFAULT_PAGE_SIZE]u8 = undefined;
    const found = try wal.readPage(5, &read_buf);
    try std.testing.expect(found);
    try std.testing.expectEqual(@as(u8, 0x42), read_buf[0]);
    try std.testing.expectEqual(@as(u8, 0xAA), read_buf[1]);
}

test "WAL checkpoint" {
    const db_path = "/tmp/zqlite_test_wal_ckpt.db";
    const wal_path = "/tmp/zqlite_test_wal_ckpt.wal";
    defer os.deleteFile(db_path);
    defer os.deleteFile(wal_path);

    var db_fh = os.FileHandle.open(db_path, os.DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer db_fh.close();

    var wal = Wal.open(std.testing.allocator, wal_path, os.DEFAULT_PAGE_SIZE) catch
        return error.SkipZigTest;
    defer wal.close();

    // Write a frame to WAL
    var page_data: [os.DEFAULT_PAGE_SIZE]u8 = undefined;
    @memset(&page_data, 0);
    page_data[0] = 0xBE;
    page_data[1] = 0xEF;
    try wal.writeFrame(0, 1, &page_data);

    // Checkpoint → should write to DB file
    try wal.checkpoint(&db_fh);
    try std.testing.expectEqual(@as(u32, 0), wal.frame_count);

    // Verify in DB file
    var read_buf: [os.DEFAULT_PAGE_SIZE]u8 = undefined;
    db_fh.readPage(0, &read_buf) catch return;
    try std.testing.expectEqual(@as(u8, 0xBE), read_buf[0]);
    try std.testing.expectEqual(@as(u8, 0xEF), read_buf[1]);
}

test "checksum deterministic" {
    const data = "hello ZQLite WAL";
    const c1 = computeChecksum(data, 0);
    const c2 = computeChecksum(data, 0);
    try std.testing.expectEqual(c1, c2);
    try std.testing.expect(c1 != 0);
}
