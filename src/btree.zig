const std = @import("std");
const pager = @import("pager.zig");
const record = @import("record.zig");

/// B-tree engine for ZQLite.
///
/// Implements B+ trees for tables (data in leaves, integer rowid keys)
/// and B-trees for indexes (arbitrary keys in all nodes).
///
/// Page layout:
///   [Page header: 8 bytes leaf / 12 bytes interior]
///   [Cell pointer array: 2 bytes per cell]
///   [Free space]
///   [Cell content area: grows from end of page backwards]
///
/// Page header format:
///   byte 0   : page type (0x0d=table leaf, 0x05=table interior,
///                          0x0a=index leaf, 0x02=index interior)
///   bytes 1-2: first free block offset (0 if none)
///   bytes 3-4: number of cells on this page
///   bytes 5-6: offset to start of cell content area
///   byte  7  : number of fragmented free bytes
///   bytes 8-11 (interior only): right-most child page number

// ═══════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════

pub const PAGE_TYPE_TABLE_LEAF: u8 = 0x0d;
pub const PAGE_TYPE_TABLE_INTERIOR: u8 = 0x05;
pub const PAGE_TYPE_INDEX_LEAF: u8 = 0x0a;
pub const PAGE_TYPE_INDEX_INTERIOR: u8 = 0x02;

pub const LEAF_HEADER_SIZE: u16 = 8;
pub const INTERIOR_HEADER_SIZE: u16 = 12;

// ═══════════════════════════════════════════════════════════════════════════
// Errors
// ═══════════════════════════════════════════════════════════════════════════

pub const BtreeError = error{
    PageFull,
    KeyNotFound,
    DuplicateKey,
    CorruptPage,
    InvalidPageType,
    PagerError,
    AllocationFailed,
    Overflow,
};

// ═══════════════════════════════════════════════════════════════════════════
// Cell — a key/value entry on a page
// ═══════════════════════════════════════════════════════════════════════════

pub const Cell = struct {
    /// For table B-trees: rowid. For index B-trees: first 8 bytes of key hash.
    key: i64,
    /// For table leaves: serialized record data. For index: serialized key record.
    payload: []const u8,
    /// For interior nodes: left child page number.
    left_child: ?u32,
};

// ═══════════════════════════════════════════════════════════════════════════
// BtreePage — operations on a single B-tree page
// ═══════════════════════════════════════════════════════════════════════════

pub const BtreePage = struct {
    page: *pager.Page,
    page_size: u32,
    pool: ?*pager.BufferPool,

    const Self = @This();

    /// Get the page type byte.
    pub fn pageType(self: *const Self) u8 {
        return self.page.data[0];
    }

    /// Is this a leaf page?
    pub fn isLeaf(self: *const Self) bool {
        const pt = self.pageType();
        return pt == PAGE_TYPE_TABLE_LEAF or pt == PAGE_TYPE_INDEX_LEAF;
    }

    /// Is this an interior page?
    pub fn isInterior(self: *const Self) bool {
        return !self.isLeaf();
    }

    /// Header size for this page type.
    pub fn headerSize(self: *const Self) u16 {
        return if (self.isInterior()) INTERIOR_HEADER_SIZE else LEAF_HEADER_SIZE;
    }

    /// Number of cells on this page.
    pub fn cellCount(self: *const Self) u16 {
        return std.mem.readInt(u16, self.page.data[3..5], .big);
    }

    /// Set the cell count.
    fn setCellCount(self: *Self, count: u16) void {
        std.mem.writeInt(u16, self.page.data[3..5], count, .big);
    }

    /// Get the offset to the cell content area.
    pub fn cellContentOffset(self: *const Self) u16 {
        const raw = std.mem.readInt(u16, self.page.data[5..7], .big);
        return if (raw == 0) @intCast(self.page_size) else raw;
    }

    /// Set the cell content area offset.
    fn setCellContentOffset(self: *Self, offset: u16) void {
        std.mem.writeInt(u16, self.page.data[5..7], offset, .big);
    }

    /// Get the right-most child page (interior nodes only).
    pub fn rightChild(self: *const Self) u32 {
        std.debug.assert(self.isInterior());
        return std.mem.readInt(u32, self.page.data[8..12], .big);
    }

    /// Set the right-most child page.
    pub fn setRightChild(self: *Self, child: u32) void {
        std.debug.assert(self.isInterior());
        std.mem.writeInt(u32, self.page.data[8..12], child, .big);
    }

    /// Get the cell pointer at index `i`.
    pub fn cellPointer(self: *const Self, i: u16) u16 {
        const ptr_offset = self.headerSize() + i * 2;
        return std.mem.readInt(u16, self.page.data[ptr_offset..][0..2], .big);
    }

    /// Set the cell pointer at index `i`.
    fn setCellPointer(self: *Self, i: u16, offset: u16) void {
        const ptr_offset = self.headerSize() + i * 2;
        std.mem.writeInt(u16, self.page.data[ptr_offset..][0..2], offset, .big);
    }

    /// Initialize a new empty page of the given type.
    pub fn initPage(self: *Self, page_type: u8) void {
        @memset(self.page.data, 0);
        self.page.data[0] = page_type;
        self.setCellCount(0);
        self.setCellContentOffset(@intCast(self.page_size));
        if (self.pool) |p| p.markPageDirty(self.page) else self.page.markDirty();
    }

    /// Read the key (rowid) from a cell at a given offset on a table leaf/interior.
    pub fn readCellKey(self: *const Self, cell_offset: u16) BtreeError!i64 {
        const data = self.page.data;
        const off: usize = cell_offset;

        if (self.pageType() == PAGE_TYPE_TABLE_LEAF) {
            // Table leaf cell: varint payload_size, varint rowid, payload
            const payload_info = record.getVarint(data[off..]) catch return BtreeError.CorruptPage;
            const rowid_info = record.getVarint(data[off + payload_info.bytes ..]) catch return BtreeError.CorruptPage;
            return @intCast(@as(i64, @bitCast(rowid_info.value)));
        } else if (self.pageType() == PAGE_TYPE_TABLE_INTERIOR) {
            // Table interior cell: u32 left_child, varint rowid
            const rowid_info = record.getVarint(data[off + 4 ..]) catch return BtreeError.CorruptPage;
            return @intCast(@as(i64, @bitCast(rowid_info.value)));
        }

        return BtreeError.InvalidPageType;
    }

    /// Read a full table leaf cell (payload + rowid).
    pub fn readTableLeafCell(self: *const Self, cell_offset: u16) BtreeError!Cell {
        const data = self.page.data;
        const off: usize = cell_offset;

        const payload_info = record.getVarint(data[off..]) catch return BtreeError.CorruptPage;
        const payload_size: usize = @intCast(payload_info.value);
        const rowid_info = record.getVarint(data[off + payload_info.bytes ..]) catch return BtreeError.CorruptPage;
        const rowid: i64 = @bitCast(rowid_info.value);
        const payload_start = off + payload_info.bytes + rowid_info.bytes;

        if (payload_start + payload_size > self.page_size) return BtreeError.CorruptPage;

        return Cell{
            .key = rowid,
            .payload = data[payload_start .. payload_start + payload_size],
            .left_child = null,
        };
    }

    /// Insert a table leaf cell.
    /// Cell format: varint(payload_size) + varint(rowid) + payload
    pub fn insertTableLeafCell(self: *Self, rowid: i64, payload: []const u8) BtreeError!void {
        // Encode cell
        var cell_buf: [4096]u8 = undefined;
        var pos: usize = 0;

        // payload size varint
        const ps_len = record.putVarint(&cell_buf, @intCast(payload.len)) catch return BtreeError.Overflow;
        pos += ps_len;

        // rowid varint
        const rid_len = record.putVarint(cell_buf[pos..], @bitCast(rowid)) catch return BtreeError.Overflow;
        pos += rid_len;

        // payload
        if (pos + payload.len > cell_buf.len) return BtreeError.Overflow;
        @memcpy(cell_buf[pos .. pos + payload.len], payload);
        pos += payload.len;

        const cell_size: u16 = @intCast(pos);

        // Find insertion point (binary search by rowid)
        const count = self.cellCount();
        var insert_idx: u16 = count;

        // Binary search for correct position
        if (count > 0) {
            var lo: u16 = 0;
            var hi: u16 = count;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                const mid_key = self.readCellKey(self.cellPointer(mid)) catch return BtreeError.CorruptPage;
                if (mid_key < rowid) {
                    lo = mid + 1;
                } else if (mid_key > rowid) {
                    hi = mid;
                } else {
                    return BtreeError.DuplicateKey;
                }
            }
            insert_idx = lo;
        }

        // Check if we have space
        const cell_content_start = self.cellContentOffset();
        const ptr_array_end = self.headerSize() + (count + 1) * 2;
        const new_content_start = cell_content_start - cell_size;

        if (new_content_start < ptr_array_end) {
            return BtreeError.PageFull;
        }

        // Write cell content (grows backwards from end of page)
        @memcpy(self.page.data[new_content_start..cell_content_start], cell_buf[0..cell_size]);

        // Shift cell pointers to make room
        var i = count;
        while (i > insert_idx) : (i -= 1) {
            self.setCellPointer(i, self.cellPointer(i - 1));
        }

        // Insert pointer to new cell
        self.setCellPointer(insert_idx, new_content_start);
        self.setCellCount(count + 1);
        self.setCellContentOffset(new_content_start);
        if (self.pool) |p| p.markPageDirty(self.page) else self.page.markDirty();
    }

    /// Search for a rowid on a table leaf page. Returns the cell index if found.
    pub fn searchTableLeaf(self: *const Self, rowid: i64) BtreeError!?u16 {
        const count = self.cellCount();
        if (count == 0) return null;

        // Binary search
        var lo: u16 = 0;
        var hi: u16 = count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const mid_key = try self.readCellKey(self.cellPointer(mid));
            if (mid_key < rowid) {
                lo = mid + 1;
            } else if (mid_key > rowid) {
                hi = mid;
            } else {
                return mid;
            }
        }
        return null;
    }

    /// Calculate free space on this page.
    pub fn freeSpace(self: *const Self) u16 {
        const ptr_array_end = self.headerSize() + self.cellCount() * 2;
        const content_start = self.cellContentOffset();
        if (content_start <= ptr_array_end) return 0;
        return content_start - ptr_array_end;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Btree — high-level B-tree operations backed by the pager
// ═══════════════════════════════════════════════════════════════════════════

pub const Btree = struct {
    pool: *pager.BufferPool,
    root_page_id: u32,
    page_size: u32,

    const Self = @This();

    /// Create a new B-tree with an empty root leaf page.
    pub fn create(pool: *pager.BufferPool, page_type: u8) BtreeError!Self {
        const root = pool.allocatePage() catch return BtreeError.PagerError;
        const root_id = root.page_id;
        var bp = BtreePage{ .page = root, .page_size = pool.page_size, .pool = pool };
        bp.initPage(page_type);
        pool.releasePage(root);

        return Self{
            .pool = pool,
            .root_page_id = root_id,
            .page_size = pool.page_size,
        };
    }

    /// Open an existing B-tree.
    pub fn open(pool: *pager.BufferPool, root_page_id: u32) Self {
        return Self{
            .pool = pool,
            .root_page_id = root_page_id,
            .page_size = pool.page_size,
        };
    }

    /// Insert a row into a table B-tree.
    pub fn insert(self: *Self, rowid: i64, payload: []const u8) BtreeError!void {
        const page = self.pool.fetchPage(self.root_page_id) catch return BtreeError.PagerError;
        defer self.pool.releasePage(page);

        var bp = BtreePage{ .page = page, .page_size = self.page_size, .pool = self.pool };

        // For now, only support single-page leaf (splitting comes later)
        if (bp.isLeaf()) {
            bp.insertTableLeafCell(rowid, payload) catch |err| {
                switch (err) {
                    BtreeError.PageFull => {
                        // TODO: implement page splitting
                        return BtreeError.PageFull;
                    },
                    else => return err,
                }
            };
        }
    }

    /// Search for a rowid. Returns the payload if found.
    pub fn search(self: *Self, rowid: i64) BtreeError!?Cell {
        const page = self.pool.fetchPage(self.root_page_id) catch return BtreeError.PagerError;
        defer self.pool.releasePage(page);

        var bp = BtreePage{ .page = page, .page_size = self.page_size, .pool = self.pool };

        if (bp.isLeaf()) {
            const idx = try bp.searchTableLeaf(rowid);
            if (idx) |i| {
                return try bp.readTableLeafCell(bp.cellPointer(i));
            }
        }

        return null;
    }

    /// Delete a rowid from a table B-tree.
    /// Currently only supports single-leaf deletion (no rebalancing).
    /// Defragments the page after deletion to reclaim cell content space.
    pub fn delete(self: *Self, rowid: i64) BtreeError!bool {
        const page = self.pool.fetchPage(self.root_page_id) catch return BtreeError.PagerError;
        defer self.pool.releasePage(page);

        var bp = BtreePage{ .page = page, .page_size = self.page_size, .pool = self.pool };

        if (bp.isLeaf()) {
            const idx = try bp.searchTableLeaf(rowid);
            if (idx) |i| {
                // Remove cell by shifting pointers
                const count = bp.cellCount();
                var j = i;
                while (j < count - 1) : (j += 1) {
                    bp.setCellPointer(j, bp.cellPointer(j + 1));
                }
                const new_count = count - 1;
                bp.setCellCount(new_count);

                // Defragment: compact all surviving cells to reclaim freed space.
                // Two-pass: first read all cell data, then write back compacted.
                const data = bp.page.data;
                const psize: usize = @intCast(bp.page_size);

                // Pass 1: collect all cell content into a temp buffer
                // Cell content can't exceed page size. Cell count can't exceed page_size/4.
                var tmp_buf: [65536]u8 = undefined; // max page size
                const tmp = tmp_buf[0..psize];
                var sizes_buf: [4096]u16 = undefined; // max ~4096 cells
                var tmp_pos: usize = 0;
                var cell_count: usize = 0;

                var k: u16 = 0;
                while (k < new_count) : (k += 1) {
                    const cell_off: usize = bp.cellPointer(k);
                    const pinfo = record.getVarint(data[cell_off..]) catch return BtreeError.CorruptPage;
                    const rinfo = record.getVarint(data[cell_off + pinfo.bytes ..]) catch return BtreeError.CorruptPage;
                    const payload_size: usize = @intCast(pinfo.value);
                    const cs: u16 = @intCast(pinfo.bytes + rinfo.bytes + payload_size);
                    sizes_buf[cell_count] = cs;
                    @memcpy(tmp[tmp_pos..][0..cs], data[cell_off..][0..cs]);
                    tmp_pos += cs;
                    cell_count += 1;
                }

                // Pass 2: write cells back compacted from end of page
                var write_pos: u16 = @intCast(bp.page_size);
                tmp_pos = 0;
                k = 0;
                while (k < new_count) : (k += 1) {
                    const cs = sizes_buf[k];
                    write_pos -= cs;
                    @memcpy(data[write_pos..][0..cs], tmp[tmp_pos..][0..cs]);
                    bp.setCellPointer(k, write_pos);
                    tmp_pos += cs;
                }
                bp.setCellContentOffset(write_pos);

                bp.page.markDirty();
                return true;
            }
        }
        return false;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

test "BtreePage init and insert" {
    const os = @import("os.zig");
    const tmp_path = "/tmp/zqlite_test_btree.db";
    defer os.deleteFile(tmp_path);

    var fh = os.FileHandle.open(tmp_path, os.DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    var pool = pager.BufferPool.init(std.testing.allocator, &fh, 4) catch
        return error.SkipZigTest;
    defer pool.deinit();

    var bt = try Btree.create(&pool, PAGE_TYPE_TABLE_LEAF);

    // Insert a few rows
    const payload1 = "hello";
    const payload2 = "world";
    try bt.insert(1, payload1);
    try bt.insert(2, payload2);
    try bt.insert(0, "zero");

    // Search
    const result = try bt.search(1);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(i64, 1), result.?.key);

    const result2 = try bt.search(2);
    try std.testing.expect(result2 != null);

    // Not found
    const result3 = try bt.search(99);
    try std.testing.expect(result3 == null);
}

test "Btree delete" {
    const os = @import("os.zig");
    const tmp_path = "/tmp/zqlite_test_btree_del.db";
    defer os.deleteFile(tmp_path);

    var fh = os.FileHandle.open(tmp_path, os.DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    var pool = pager.BufferPool.init(std.testing.allocator, &fh, 4) catch
        return error.SkipZigTest;
    defer pool.deinit();

    var bt = try Btree.create(&pool, PAGE_TYPE_TABLE_LEAF);

    try bt.insert(10, "ten");
    try bt.insert(20, "twenty");
    try bt.insert(30, "thirty");

    // Delete middle key
    const deleted = try bt.delete(20);
    try std.testing.expect(deleted);

    // Verify it's gone
    const result = try bt.search(20);
    try std.testing.expect(result == null);

    // Others still there
    try std.testing.expect((try bt.search(10)) != null);
    try std.testing.expect((try bt.search(30)) != null);
}

test "duplicate key rejected" {
    const os = @import("os.zig");
    const tmp_path = "/tmp/zqlite_test_btree_dup.db";
    defer os.deleteFile(tmp_path);

    var fh = os.FileHandle.open(tmp_path, os.DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    var pool = pager.BufferPool.init(std.testing.allocator, &fh, 4) catch
        return error.SkipZigTest;
    defer pool.deinit();

    var bt = try Btree.create(&pool, PAGE_TYPE_TABLE_LEAF);
    try bt.insert(1, "first");
    try std.testing.expectError(BtreeError.DuplicateKey, bt.insert(1, "second"));
}
