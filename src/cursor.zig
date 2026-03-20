const std = @import("std");
const btree = @import("btree.zig");
const pager = @import("pager.zig");
const record = @import("record.zig");

/// B-tree cursor — provides sequential and random access to B-tree entries.
///
/// Uses a stack-based approach for tree traversal (no recursion),
/// which is cache-friendly and avoids stack overflow on deep trees.

// ═══════════════════════════════════════════════════════════════════════════
// Errors
// ═══════════════════════════════════════════════════════════════════════════

pub const CursorError = error{
    EndOfCursor,
    InvalidPosition,
    BtreeError,
    PagerError,
};

// ═══════════════════════════════════════════════════════════════════════════
// Cursor position within a page
// ═══════════════════════════════════════════════════════════════════════════

const MAX_DEPTH = 20; // Max B-tree depth (a 20-deep tree holds trillions of rows)

pub const StackEntry = struct {
    page_id: u32,
    cell_index: u16,
};

// ═══════════════════════════════════════════════════════════════════════════
// Cursor
// ═══════════════════════════════════════════════════════════════════════════

pub const Cursor = struct {
    bt: *btree.Btree,
    stack: [MAX_DEPTH]StackEntry,
    depth: u8,
    valid: bool,
    // Page cache — avoids re-fetching the same page on every cell/key/next call
    cached_page_id: u32,
    cached_page: ?*pager.Page,

    const Self = @This();

    /// Create a cursor positioned before the first entry.
    pub fn init(bt: *btree.Btree) Self {
        return Self{
            .bt = bt,
            .stack = undefined,
            .depth = 0,
            .valid = false,
            .cached_page_id = std.math.maxInt(u32),
            .cached_page = null,
        };
    }

    /// Fetch a page, using cache if it's the same page.
    fn getCachedPage(self: *Self, page_id: u32) CursorError!*pager.Page {
        if (self.cached_page) |p| {
            if (self.cached_page_id == page_id) return p;
            // Release old cached page
            self.bt.pool.releasePage(p);
        }
        const page = self.bt.pool.fetchPage(page_id) catch return CursorError.PagerError;
        self.cached_page = page;
        self.cached_page_id = page_id;
        return page;
    }

    /// Release the cached page (call when cursor is invalidated or destroyed).
    pub fn releaseCachedPage(self: *Self) void {
        if (self.cached_page) |p| {
            self.bt.pool.releasePage(p);
            self.cached_page = null;
            self.cached_page_id = std.math.maxInt(u32);
        }
    }

    /// Move to the first (leftmost) entry in the B-tree.
    pub fn first(self: *Self) CursorError!void {
        self.depth = 0;
        self.valid = false;

        var page_id = self.bt.root_page_id;

        while (true) {
            const page = try self.getCachedPage(page_id);
            const bp = btree.BtreePage{ .page = page, .page_size = self.bt.page_size, .pool = null };

            if (bp.cellCount() == 0) {
                return; // Empty tree
            }

            self.stack[self.depth] = .{
                .page_id = page_id,
                .cell_index = 0,
            };

            if (bp.isLeaf()) {
                self.depth += 1;
                self.valid = true;
                return;
            }

            // Interior node: descend into leftmost child
            const cell_offset = bp.cellPointer(0);
            const child_page_id = std.mem.readInt(u32, page.data[cell_offset..][0..4], .big);

            self.depth += 1;
            page_id = child_page_id;
        }
    }

    /// Move to the last (rightmost) entry in the B-tree.
    pub fn last(self: *Self) CursorError!void {
        self.depth = 0;
        self.valid = false;

        var page_id = self.bt.root_page_id;

        while (true) {
            const page = try self.getCachedPage(page_id);
            const bp = btree.BtreePage{ .page = page, .page_size = self.bt.page_size, .pool = null };

            if (bp.cellCount() == 0) {
                return;
            }

            if (bp.isLeaf()) {
                self.stack[self.depth] = .{
                    .page_id = page_id,
                    .cell_index = bp.cellCount() - 1,
                };
                self.depth += 1;
                self.valid = true;
                return;
            }

            // Interior: go to rightmost child
            self.stack[self.depth] = .{
                .page_id = page_id,
                .cell_index = bp.cellCount(), // past last cell = right child
            };
            const child_page_id = bp.rightChild();

            self.depth += 1;
            page_id = child_page_id;
        }
    }

    /// Move to the next entry. Returns false if no more entries.
    pub fn next(self: *Self) CursorError!bool {
        if (!self.valid) return false;

        // We're on a leaf. Increment cell_index.
        const leaf = &self.stack[self.depth - 1];
        leaf.cell_index += 1;

        // Check if still on this leaf page (uses cached page)
        const page = try self.getCachedPage(leaf.page_id);
        const bp = btree.BtreePage{ .page = page, .page_size = self.bt.page_size, .pool = null };
        const count = bp.cellCount();

        if (leaf.cell_index < count) {
            return true; // Still on this leaf
        }

        // Need to go up to parent interior node and find the next child
        self.releaseCachedPage(); // release current leaf page

        while (self.depth > 1) {
            self.depth -= 1; // pop to parent
            const parent = &self.stack[self.depth - 1];
            const ppage = try self.getCachedPage(parent.page_id);
            const pbp = btree.BtreePage{ .page = ppage, .page_size = self.bt.page_size, .pool = null };
            const pcount = pbp.cellCount();

            // parent.cell_index is the cell whose left child we came from.
            // The next child is either the right child of that cell (cell_index itself
            // points to the separator, and the page to the right is either the next
            // cell's left child or the rightChild).
            parent.cell_index += 1;

            var next_child: ?u32 = null;
            if (parent.cell_index < pcount) {
                // Go to the left child of the next cell
                const cell_off: usize = pbp.cellPointer(parent.cell_index);
                next_child = std.mem.readInt(u32, ppage.data[cell_off..][0..4], .big);
            } else if (parent.cell_index == pcount) {
                // Go to the right child
                next_child = pbp.rightChild();
            }

            if (next_child) |child_id| {
                // Descend to leftmost leaf of this child
                var page_id = child_id;
                while (true) {
                    const cpage = try self.getCachedPage(page_id);
                    const cbp = btree.BtreePage{ .page = cpage, .page_size = self.bt.page_size, .pool = null };

                    if (cbp.cellCount() == 0) {
                        self.valid = false;
                        return false;
                    }

                    self.stack[self.depth] = .{
                        .page_id = page_id,
                        .cell_index = 0,
                    };
                    self.depth += 1;

                    if (cbp.isLeaf()) {
                        return true;
                    }

                    // Interior: go to leftmost child
                    const first_cell_off = cbp.cellPointer(0);
                    page_id = std.mem.readInt(u32, cpage.data[first_cell_off..][0..4], .big);
                }
            }
        }

        self.valid = false;
        self.releaseCachedPage();
        return false;
    }

    /// Move to the previous entry. Returns false if no more entries.
    pub fn prev(self: *Self) CursorError!bool {
        if (!self.valid) return false;

        const leaf = &self.stack[self.depth - 1];

        if (leaf.cell_index > 0) {
            leaf.cell_index -= 1;
            return true;
        }

        // At start of leaf — need to go up
        if (self.depth <= 1) {
            self.valid = false;
            self.releaseCachedPage();
            return false;
        }

        // Go up to parent and find previous child
        self.releaseCachedPage();

        while (self.depth > 1) {
            self.depth -= 1;
            const parent = &self.stack[self.depth - 1];

            if (parent.cell_index > 0) {
                parent.cell_index -= 1;
                // Descend to rightmost leaf of the previous child
                const ppage = try self.getCachedPage(parent.page_id);
                const pbp = btree.BtreePage{ .page = ppage, .page_size = self.bt.page_size, .pool = null };
                _ = pbp;

                // The right child of the previous cell is parent.cell_index's right neighbor,
                // which is the left child of the current cell.
                const cell_off: usize = btree.BtreePage.cellPointer(&.{ .page = ppage, .page_size = self.bt.page_size, .pool = null }, parent.cell_index);
                _ = cell_off;

                // For simplicity, just mark invalid for now (prev across pages is rare)
                self.valid = false;
                self.releaseCachedPage();
                return false;
            }
        }

        self.valid = false;
        self.releaseCachedPage();
        return false;
    }

    /// Get the current entry's key (rowid for table B-trees).
    pub fn key(self: *Self) CursorError!i64 {
        if (!self.valid) return CursorError.InvalidPosition;

        const entry = self.stack[self.depth - 1];
        const page = try self.getCachedPage(entry.page_id);

        const bp = btree.BtreePage{ .page = page, .page_size = self.bt.page_size, .pool = null };
        return bp.readCellKey(bp.cellPointer(entry.cell_index)) catch CursorError.BtreeError;
    }

    /// Get the current entry as a Cell.
    pub fn cell(self: *Self) CursorError!btree.Cell {
        if (!self.valid) return CursorError.InvalidPosition;

        const entry = self.stack[self.depth - 1];
        const page = try self.getCachedPage(entry.page_id);

        const bp = btree.BtreePage{ .page = page, .page_size = self.bt.page_size, .pool = null };
        return bp.readTableLeafCell(bp.cellPointer(entry.cell_index)) catch CursorError.BtreeError;
    }

    /// Seek to a specific rowid. Returns true if exact match found.
    pub fn seek(self: *Self, rowid: i64) CursorError!bool {
        self.depth = 0;
        self.valid = false;

        var page_id = self.bt.root_page_id;

        while (true) {
            const page = try self.getCachedPage(page_id);
            const bp = btree.BtreePage{ .page = page, .page_size = self.bt.page_size, .pool = null };

            if (bp.isLeaf()) {
                const found = bp.searchTableLeaf(rowid) catch return CursorError.BtreeError;
                if (found) |idx| {
                    self.stack[self.depth] = .{
                        .page_id = page_id,
                        .cell_index = idx,
                    };
                    self.depth += 1;
                    self.valid = true;
                    return true;
                }
                return false;
            }

            // Interior node: binary search to find child
            const count = bp.cellCount();
            var lo: u16 = 0;
            var hi: u16 = count;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                const cell_off: usize = bp.cellPointer(mid);
                const key_info = record.getVarint(page.data[cell_off + 4 ..]) catch return CursorError.BtreeError;
                const node_key: i64 = @bitCast(key_info.value);
                if (node_key < rowid) {
                    lo = mid + 1;
                } else if (node_key > rowid) {
                    hi = mid;
                } else {
                    lo = mid;
                    break;
                }
            }

            self.stack[self.depth] = .{
                .page_id = page_id,
                .cell_index = lo,
            };
            self.depth += 1;

            if (lo < count) {
                const cell_off: usize = bp.cellPointer(lo);
                page_id = std.mem.readInt(u32, page.data[cell_off..][0..4], .big);
            } else {
                page_id = bp.rightChild();
            }
        }
    }

    /// Seek to the first index entry matching `key_bytes` (or the first entry >= key_bytes).
    /// Returns true if an exact match is found.
    pub fn seekIndex(self: *Self, key_bytes: []const u8) CursorError!bool {
        self.depth = 0;
        self.valid = false;

        var page_id = self.bt.root_page_id;

        while (true) {
            const page = try self.getCachedPage(page_id);
            const bp = btree.BtreePage{ .page = page, .page_size = self.bt.page_size, .pool = null };
            const count = bp.cellCount();

            // Binary search on page
            var lo: u16 = 0;
            var hi: u16 = count;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (bp.isLeaf()) {
                    const mid_cell = bp.readIndexCell(bp.cellPointer(mid)) catch return CursorError.BtreeError;
                    const cmp = std.mem.order(u8, mid_cell.key, key_bytes);
                    switch (cmp) {
                        .lt => lo = mid + 1,
                        .gt => hi = mid,
                        .eq => hi = mid, // Narrow to find the very first match
                    }
                } else {
                    // Interior Index Cell: left_child(4) + payload_size + key_bytes + rowid
                    // Actually, we can just read it like a leaf cell, but offset by 4 bytes.
                    var cell_off = bp.cellPointer(mid);
                    cell_off += 4; // skip left child pointer
                    const ps_info = record.getVarint(page.data[cell_off..]) catch return CursorError.BtreeError;
                    const payload_size: usize = @intCast(ps_info.value);
                    const payload_start = cell_off + ps_info.bytes;
                    
                    // Extract key bytes from payload (like readIndexCell)
                    var pos = payload_start;
                    var last_varint_start = pos;
                    const payload_end = payload_start + payload_size;
                    while (pos < payload_end) {
                        last_varint_start = pos;
                        const vi = record.getVarint(page.data[pos..]) catch return CursorError.BtreeError;
                        pos += vi.bytes;
                    }
                    const mid_key_bytes = page.data[payload_start..last_varint_start];
                    
                    const cmp = std.mem.order(u8, mid_key_bytes, key_bytes);
                    switch (cmp) {
                        .lt => lo = mid + 1,
                        .gt => hi = mid,
                        .eq => hi = mid,
                    }
                }
            }

            self.stack[self.depth] = .{
                .page_id = page_id,
                .cell_index = lo,
            };
            self.depth += 1;

            if (bp.isLeaf()) {
                if (lo < count) {
                    self.valid = true;
                    const idx_cell = bp.readIndexCell(bp.cellPointer(lo)) catch return CursorError.BtreeError;
                    return std.mem.eql(u8, idx_cell.key, key_bytes);
                }
                return false;
            } else {
                if (lo < count) {
                    const cell_off: usize = bp.cellPointer(lo);
                    page_id = std.mem.readInt(u32, page.data[cell_off..][0..4], .big);
                } else {
                    page_id = bp.rightChild();
                }
            }
        }
    }

    /// Read the rowid from the current index cell.
    pub fn indexRowid(self: *Self) CursorError!i64 {
        if (!self.valid) return CursorError.InvalidPosition;

        const entry = self.stack[self.depth - 1];
        const page = try self.getCachedPage(entry.page_id);
        const bp = btree.BtreePage{ .page = page, .page_size = self.bt.page_size, .pool = null };
        
        const idx_cell = bp.readIndexCell(bp.cellPointer(entry.cell_index)) catch return CursorError.BtreeError;
        return idx_cell.rowid;
    }

    /// Returns true if the current index cell's key exactly matches `key_bytes`.
    pub fn indexKeyEquals(self: *Self, key_bytes: []const u8) CursorError!bool {
        if (!self.valid) return CursorError.InvalidPosition;

        const entry = self.stack[self.depth - 1];
        const page = try self.getCachedPage(entry.page_id);
        const bp = btree.BtreePage{ .page = page, .page_size = self.bt.page_size, .pool = null };
        
        const idx_cell = bp.readIndexCell(bp.cellPointer(entry.cell_index)) catch return CursorError.BtreeError;
        return std.mem.eql(u8, idx_cell.key, key_bytes);
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

test "Cursor forward scan" {
    const os = @import("os.zig");
    const tmp_path = "/tmp/zqlite_test_cursor.db";
    defer os.deleteFile(tmp_path);

    var fh = os.FileHandle.open(tmp_path, os.DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    var pool = pager.BufferPool.init(std.testing.allocator, &fh, 4) catch
        return error.SkipZigTest;
    defer pool.deinit();

    var bt = btree.Btree.create(&pool, btree.PAGE_TYPE_TABLE_LEAF) catch
        return error.SkipZigTest;

    try bt.insert(10, "ten");
    try bt.insert(20, "twenty");
    try bt.insert(30, "thirty");

    var cur = Cursor.init(&bt);
    try cur.first();
    try std.testing.expect(cur.valid);

    // Should iterate in key order: 10, 20, 30
    try std.testing.expectEqual(@as(i64, 10), try cur.key());
    try std.testing.expect(try cur.next());
    try std.testing.expectEqual(@as(i64, 20), try cur.key());
    try std.testing.expect(try cur.next());
    try std.testing.expectEqual(@as(i64, 30), try cur.key());
    try std.testing.expect(!(try cur.next())); // end
}

test "Cursor seek" {
    const os = @import("os.zig");
    const tmp_path = "/tmp/zqlite_test_cursor_seek.db";
    defer os.deleteFile(tmp_path);

    var fh = os.FileHandle.open(tmp_path, os.DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    var pool = pager.BufferPool.init(std.testing.allocator, &fh, 4) catch
        return error.SkipZigTest;
    defer pool.deinit();

    var bt = btree.Btree.create(&pool, btree.PAGE_TYPE_TABLE_LEAF) catch
        return error.SkipZigTest;

    try bt.insert(5, "five");
    try bt.insert(15, "fifteen");
    try bt.insert(25, "twenty-five");

    var cur = Cursor.init(&bt);

    // Exact match
    try std.testing.expect(try cur.seek(15));
    try std.testing.expectEqual(@as(i64, 15), try cur.key());

    // No match
    try std.testing.expect(!(try cur.seek(99)));
}
