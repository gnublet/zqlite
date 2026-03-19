const std = @import("std");
const btree = @import("btree.zig");
const pager = @import("pager.zig");

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

    const Self = @This();

    /// Create a cursor positioned before the first entry.
    pub fn init(bt: *btree.Btree) Self {
        return Self{
            .bt = bt,
            .stack = undefined,
            .depth = 0,
            .valid = false,
        };
    }

    /// Move to the first (leftmost) entry in the B-tree.
    pub fn first(self: *Self) CursorError!void {
        self.depth = 0;
        self.valid = false;

        var page_id = self.bt.root_page_id;

        while (true) {
            const page = self.bt.pool.fetchPage(page_id) catch return CursorError.PagerError;
            const bp = btree.BtreePage{ .page = page, .page_size = self.bt.page_size, .pool = null };

            if (bp.cellCount() == 0) {
                self.bt.pool.releasePage(page);
                return; // Empty tree
            }

            self.stack[self.depth] = .{
                .page_id = page_id,
                .cell_index = 0,
            };

            if (bp.isLeaf()) {
                self.depth += 1;
                self.valid = true;
                self.bt.pool.releasePage(page);
                return;
            }

            // Interior node: descend into leftmost child
            // For table interior: cell format is u32 left_child + varint rowid
            // The leftmost child is the left_child of cell 0
            const cell_offset = bp.cellPointer(0);
            const child_page_id = std.mem.readInt(u32, page.data[cell_offset..][0..4], .big);
            self.bt.pool.releasePage(page);

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
            const page = self.bt.pool.fetchPage(page_id) catch return CursorError.PagerError;
            const bp = btree.BtreePage{ .page = page, .page_size = self.bt.page_size, .pool = null };

            if (bp.cellCount() == 0) {
                self.bt.pool.releasePage(page);
                return;
            }

            if (bp.isLeaf()) {
                self.stack[self.depth] = .{
                    .page_id = page_id,
                    .cell_index = bp.cellCount() - 1,
                };
                self.depth += 1;
                self.valid = true;
                self.bt.pool.releasePage(page);
                return;
            }

            // Interior: go to rightmost child
            self.stack[self.depth] = .{
                .page_id = page_id,
                .cell_index = bp.cellCount(), // past last cell = right child
            };
            const child_page_id = bp.rightChild();
            self.bt.pool.releasePage(page);

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

        // Check if still on this leaf page
        const page = self.bt.pool.fetchPage(leaf.page_id) catch return CursorError.PagerError;
        const bp = btree.BtreePage{ .page = page, .page_size = self.bt.page_size, .pool = null };
        const count = bp.cellCount();
        self.bt.pool.releasePage(page);

        if (leaf.cell_index < count) {
            return true; // Still on this leaf
        }

        // Need to go up and then right
        // For single-level (leaf-only) trees, we're done
        if (self.depth <= 1) {
            self.valid = false;
            return false;
        }

        // TODO: multi-level traversal for interior nodes
        self.valid = false;
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
            return false;
        }

        // TODO: multi-level traversal
        self.valid = false;
        return false;
    }

    /// Get the current entry's key (rowid for table B-trees).
    pub fn key(self: *Self) CursorError!i64 {
        if (!self.valid) return CursorError.InvalidPosition;

        const entry = self.stack[self.depth - 1];
        const page = self.bt.pool.fetchPage(entry.page_id) catch return CursorError.PagerError;
        defer self.bt.pool.releasePage(page);

        const bp = btree.BtreePage{ .page = page, .page_size = self.bt.page_size, .pool = null };
        return bp.readCellKey(bp.cellPointer(entry.cell_index)) catch CursorError.BtreeError;
    }

    /// Get the current entry as a Cell.
    pub fn cell(self: *Self) CursorError!btree.Cell {
        if (!self.valid) return CursorError.InvalidPosition;

        const entry = self.stack[self.depth - 1];
        const page = self.bt.pool.fetchPage(entry.page_id) catch return CursorError.PagerError;
        defer self.bt.pool.releasePage(page);

        const bp = btree.BtreePage{ .page = page, .page_size = self.bt.page_size, .pool = null };
        return bp.readTableLeafCell(bp.cellPointer(entry.cell_index)) catch CursorError.BtreeError;
    }

    /// Seek to a specific rowid. Returns true if exact match found.
    pub fn seek(self: *Self, rowid: i64) CursorError!bool {
        self.depth = 0;
        self.valid = false;

        const page = self.bt.pool.fetchPage(self.bt.root_page_id) catch return CursorError.PagerError;
        defer self.bt.pool.releasePage(page);

        const bp = btree.BtreePage{ .page = page, .page_size = self.bt.page_size, .pool = null };

        if (bp.cellCount() == 0) return false;

        // Binary search on the leaf
        const found = bp.searchTableLeaf(rowid) catch return CursorError.BtreeError;

        if (found) |idx| {
            self.stack[0] = .{
                .page_id = self.bt.root_page_id,
                .cell_index = idx,
            };
            self.depth = 1;
            self.valid = true;
            return true;
        }

        return false;
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
