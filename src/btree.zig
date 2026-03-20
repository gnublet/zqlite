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
    pub fn setCellCount(self: *Self, count: u16) void {
        std.mem.writeInt(u16, self.page.data[3..5], count, .big);
    }

    /// Get the offset to the cell content area.
    pub fn cellContentOffset(self: *const Self) u16 {
        const raw = std.mem.readInt(u16, self.page.data[5..7], .big);
        return if (raw == 0) @intCast(self.page_size) else raw;
    }

    /// Set the cell content area offset.
    pub fn setCellContentOffset(self: *Self, offset: u16) void {
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
    pub fn setCellPointer(self: *Self, i: u16, offset: u16) void {
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

        // Fast-path: if new key > last key, append at end (common for sequential inserts)
        if (count > 0) {
            const last_key = self.readCellKey(self.cellPointer(count - 1)) catch return BtreeError.CorruptPage;
            if (rowid > last_key) {
                insert_idx = count; // append at end, skip binary search
            } else if (rowid == last_key) {
                return BtreeError.DuplicateKey;
            } else {
                // Binary search for correct position
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

    /// Insert an interior node cell: [left_child: u32][rowid: varint]
    /// After insertion, updates right_child to point to new_right_child.
    pub fn insertInteriorCell(self: *Self, key: i64, left_child: u32, new_right_child: u32) BtreeError!void {
        // Encode cell: u32 left_child + varint rowid
        var cell_buf: [20]u8 = undefined;
        std.mem.writeInt(u32, cell_buf[0..4], left_child, .big);
        const key_len = record.putVarint(cell_buf[4..], @bitCast(key)) catch return BtreeError.Overflow;
        const cell_size: u16 = @intCast(4 + key_len);

        // Find insertion point (binary search)
        const count = self.cellCount();
        var insert_idx: u16 = count;
        const data = self.page.data;

        if (count > 0) {
            var lo: u16 = 0;
            var hi: u16 = count;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                const mid_off: usize = self.cellPointer(mid);
                const mid_key_info = record.getVarint(data[mid_off + 4 ..]) catch return BtreeError.CorruptPage;
                const mid_key: i64 = @bitCast(mid_key_info.value);
                if (mid_key < key) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            insert_idx = lo;
        }

        // Check space
        const content_start = self.cellContentOffset();
        const ptr_array_end = self.headerSize() + (count + 1) * 2;
        const new_content_start = content_start - cell_size;
        if (new_content_start < ptr_array_end) return BtreeError.PageFull;

        // Write cell content
        @memcpy(self.page.data[new_content_start..content_start], cell_buf[0..cell_size]);

        // Shift cell pointers
        var i = count;
        while (i > insert_idx) : (i -= 1) {
            self.setCellPointer(i, self.cellPointer(i - 1));
        }

        self.setCellPointer(insert_idx, new_content_start);
        self.setCellCount(count + 1);
        self.setCellContentOffset(new_content_start);

        // Update right child if inserting at the rightmost position
        if (new_right_child != 0) {
            // The new cell's left_child points to left_child.
            // The right child carries the new_right_child if this is a split insertion.
            // Find if we need to update right child:
            // If the new key is greater than the old rightmost key, update right child.
            if (insert_idx == count) {
                self.setRightChild(new_right_child);
            }
        }

        if (self.pool) |p| p.markPageDirty(self.page) else self.page.markDirty();
    }

    /// Truncate this page to keep only the first `keep` cells.
    /// Compacts the remaining cells to reclaim space.
    pub fn truncateCells(self: *Self, keep: u16) void {
        const current = self.cellCount();
        if (keep >= current) return;

        // Read cell data for cells we want to keep
        const data = self.page.data;
        const psize: usize = @intCast(self.page_size);
        var tmp_buf: [65536]u8 = undefined;
        const tmp = tmp_buf[0..psize];
        var sizes_buf: [4096]u16 = undefined;
        var tmp_pos: usize = 0;

        const is_leaf = self.isLeaf();

        var k: u16 = 0;
        while (k < keep) : (k += 1) {
            const cell_off: usize = self.cellPointer(k);
            var cs: u16 = 0;

            if (is_leaf) {
                // Leaf cell: varint(payload_size) + varint(rowid) + payload
                const pinfo = record.getVarint(data[cell_off..]) catch break;
                const rinfo = record.getVarint(data[cell_off + pinfo.bytes ..]) catch break;
                const payload_size: usize = @intCast(pinfo.value);
                cs = @intCast(pinfo.bytes + rinfo.bytes + payload_size);
            } else {
                // Interior cell: u32(left_child) + varint(rowid)
                const kinfo = record.getVarint(data[cell_off + 4 ..]) catch break;
                cs = @intCast(4 + kinfo.bytes);
            }

            sizes_buf[k] = cs;
            @memcpy(tmp[tmp_pos..][0..cs], data[cell_off..][0..cs]);
            tmp_pos += cs;
        }

        // Rewrite compacted from end of page
        var write_pos: u16 = @intCast(self.page_size);
        tmp_pos = 0;
        k = 0;
        while (k < keep) : (k += 1) {
            const cs = sizes_buf[k];
            write_pos -= cs;
            @memcpy(data[write_pos..][0..cs], tmp[tmp_pos..][0..cs]);
            self.setCellPointer(k, write_pos);
            tmp_pos += cs;
        }
        self.setCellCount(keep);
        self.setCellContentOffset(write_pos);
        if (self.pool) |p| p.markPageDirty(self.page) else self.page.markDirty();
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

    /// Result of a page split: the new sibling page and the promoted key.
    const SplitResult = struct {
        new_page_id: u32,
        median_key: i64,
    };

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

    /// Insert a row into a table B-tree with automatic page splitting.
    pub fn insert(self: *Self, rowid: i64, payload: []const u8) BtreeError!void {
        const split = try self.insertRecursive(self.root_page_id, rowid, payload);

        // If the root page split, create a new root interior node
        if (split) |s| {
            try self.createNewRoot(s.median_key, s.new_page_id);
        }
    }

    /// Recursive insert: descend to the correct leaf, insert, split if needed.
    /// Returns SplitResult if this page was split, null otherwise.
    fn insertRecursive(self: *Self, page_id: u32, rowid: i64, payload: []const u8) BtreeError!?SplitResult {
        const page = self.pool.fetchPage(page_id) catch return BtreeError.PagerError;
        defer self.pool.releasePage(page);

        var bp = BtreePage{ .page = page, .page_size = self.page_size, .pool = self.pool };

        if (bp.isLeaf()) {
            // Try to insert into this leaf
            bp.insertTableLeafCell(rowid, payload) catch |err| {
                if (err == BtreeError.PageFull) {
                    return try self.splitLeafAndInsert(page_id, rowid, payload);
                }
                return err;
            };
            return null;
        }

        // Interior node: find correct child to descend into
        const child_page_id = try self.findChildPage(&bp, rowid);
        const child_split = try self.insertRecursive(child_page_id, rowid, payload);

        // If the child page split, insert the separator key here
        if (child_split) |cs| {
            // Interior cell format: [left_child=child_page_id] [key=median]
            // right_child is updated to cs.new_page_id
            bp.insertInteriorCell(cs.median_key, child_page_id, cs.new_page_id) catch |err| {
                if (err == BtreeError.PageFull) {
                    return try self.splitInteriorAndInsert(page_id, cs.median_key, child_page_id, cs.new_page_id);
                }
                return err;
            };
        }
        return null;
    }

    /// Find which child page to descend into for a given rowid.
    fn findChildPage(self: *Self, bp: *BtreePage, rowid: i64) BtreeError!u32 {
        _ = self;
        const count = bp.cellCount();
        const data = bp.page.data;

        // Binary search: find first cell with key >= rowid
        var lo: u16 = 0;
        var hi: u16 = count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const cell_off: usize = bp.cellPointer(mid);
            const key_info = record.getVarint(data[cell_off + 4 ..]) catch return BtreeError.CorruptPage;
            const key: i64 = @bitCast(key_info.value);
            if (rowid < key) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }

        if (lo < count) {
            // Go to the left child of cell[lo]
            const cell_off: usize = bp.cellPointer(lo);
            return std.mem.readInt(u32, data[cell_off..][0..4], .big);
        } else {
            // rowid is greater than all keys — go to right child
            return bp.rightChild();
        }
    }

    /// Split a full leaf page and insert the new cell into the correct half.
    fn splitLeafAndInsert(self: *Self, page_id: u32, rowid: i64, payload: []const u8) BtreeError!SplitResult {
        const page = self.pool.fetchPage(page_id) catch return BtreeError.PagerError;
        defer self.pool.releasePage(page);
        var bp = BtreePage{ .page = page, .page_size = self.page_size, .pool = self.pool };

        // Allocate new right sibling
        const new_page = self.pool.allocatePage() catch return BtreeError.PagerError;
        defer self.pool.releasePage(new_page);
        var new_bp = BtreePage{ .page = new_page, .page_size = self.page_size, .pool = self.pool };
        new_bp.initPage(PAGE_TYPE_TABLE_LEAF);

        const count = bp.cellCount();
        const mid = count / 2;

        // Read the median key (promoted to parent)
        const median_key = bp.readCellKey(bp.cellPointer(mid)) catch return BtreeError.CorruptPage;

        // Copy cells [mid..count) to new page
        var i: u16 = mid;
        while (i < count) : (i += 1) {
            const cell = bp.readTableLeafCell(bp.cellPointer(i)) catch return BtreeError.CorruptPage;
            new_bp.insertTableLeafCell(cell.key, cell.payload) catch return BtreeError.PagerError;
        }

        // Truncate original page to [0..mid)
        bp.truncateCells(mid);

        // Insert new cell into the correct half
        if (rowid < median_key) {
            bp.insertTableLeafCell(rowid, payload) catch return BtreeError.PageFull;
        } else {
            new_bp.insertTableLeafCell(rowid, payload) catch return BtreeError.PageFull;
        }

        return SplitResult{
            .new_page_id = new_page.page_id,
            .median_key = median_key,
        };
    }

    /// Split a full interior page and insert a separator.
    fn splitInteriorAndInsert(self: *Self, page_id: u32, key: i64, left_child: u32, right_child: u32) BtreeError!SplitResult {
        const page = self.pool.fetchPage(page_id) catch return BtreeError.PagerError;
        defer self.pool.releasePage(page);
        var bp = BtreePage{ .page = page, .page_size = self.page_size, .pool = self.pool };

        // Allocate right sibling
        const new_page = self.pool.allocatePage() catch return BtreeError.PagerError;
        defer self.pool.releasePage(new_page);
        var new_bp = BtreePage{ .page = new_page, .page_size = self.page_size, .pool = self.pool };
        new_bp.initPage(PAGE_TYPE_TABLE_INTERIOR);

        const count = bp.cellCount();
        const mid = count / 2;
        const data = bp.page.data;

        // Median cell's key is promoted
        const mid_off: usize = bp.cellPointer(mid);
        const mid_key_info = record.getVarint(data[mid_off + 4 ..]) catch return BtreeError.CorruptPage;
        const promoted_key: i64 = @bitCast(mid_key_info.value);

        // Copy cells [mid+1..count) to new page
        var i: u16 = mid + 1;
        while (i < count) : (i += 1) {
            const cell_off: usize = bp.cellPointer(i);
            const child = std.mem.readInt(u32, data[cell_off..][0..4], .big);
            const k_info = record.getVarint(data[cell_off + 4 ..]) catch return BtreeError.CorruptPage;
            const k: i64 = @bitCast(k_info.value);
            new_bp.insertInteriorCell(k, child, 0) catch return BtreeError.PagerError;
        }

        // New page's right child = old page's right child
        new_bp.setRightChild(bp.rightChild());

        // Old page's right child = median cell's left child
        const median_left = std.mem.readInt(u32, data[mid_off..][0..4], .big);
        bp.setRightChild(median_left);

        // Truncate old page to [0..mid)
        bp.truncateCells(mid);

        // Insert new separator into correct half
        if (key < promoted_key) {
            bp.insertInteriorCell(key, left_child, right_child) catch return BtreeError.PageFull;
        } else {
            new_bp.insertInteriorCell(key, left_child, right_child) catch return BtreeError.PageFull;
        }

        return SplitResult{
            .new_page_id = new_page.page_id,
            .median_key = promoted_key,
        };
    }

    /// Create a new root interior node after the root page splits.
    /// Copies old root content to a new left child, then reinitializes
    /// the root as an interior node pointing to both children.
    fn createNewRoot(self: *Self, median_key: i64, right_page_id: u32) BtreeError!void {
        // Allocate new page to hold old root's content (becomes left child)
        const new_left = self.pool.allocatePage() catch return BtreeError.PagerError;
        const new_left_id = new_left.page_id;

        // Copy old root data to new left child
        const root_page = self.pool.fetchPage(self.root_page_id) catch {
            self.pool.releasePage(new_left);
            return BtreeError.PagerError;
        };

        @memcpy(new_left.data[0..self.page_size], root_page.data[0..self.page_size]);
        new_left.page_id = new_left_id;
        self.pool.markPageDirty(new_left);
        self.pool.releasePage(new_left);

        // Reinitialize root as interior node
        var root_bp = BtreePage{ .page = root_page, .page_size = self.page_size, .pool = self.pool };
        root_bp.initPage(PAGE_TYPE_TABLE_INTERIOR);
        root_bp.setRightChild(right_page_id);

        // Write single separator cell: left_child=new_left_id, key=median_key
        var cell_buf: [20]u8 = undefined;
        std.mem.writeInt(u32, cell_buf[0..4], new_left_id, .big);
        const key_len = record.putVarint(cell_buf[4..], @bitCast(median_key)) catch return BtreeError.Overflow;
        const cell_size: u16 = @intCast(4 + key_len);

        const content_start = root_bp.cellContentOffset() - cell_size;
        @memcpy(root_page.data[content_start..][0..cell_size], cell_buf[0..cell_size]);
        root_bp.setCellPointer(0, content_start);
        root_bp.setCellCount(1);
        root_bp.setCellContentOffset(content_start);

        self.pool.markPageDirty(root_page);
        self.pool.releasePage(root_page);
    }

    /// Search for a rowid, traversing interior nodes to find the correct leaf.
    pub fn search(self: *Self, rowid: i64) BtreeError!?Cell {
        var page_id = self.root_page_id;

        while (true) {
            const page = self.pool.fetchPage(page_id) catch return BtreeError.PagerError;
            defer self.pool.releasePage(page);

            var bp = BtreePage{ .page = page, .page_size = self.page_size, .pool = self.pool };

            if (bp.isLeaf()) {
                const idx = try bp.searchTableLeaf(rowid);
                if (idx) |i| {
                    return try bp.readTableLeafCell(bp.cellPointer(i));
                }
                return null;
            }

            // Interior: descend
            page_id = try self.findChildPage(&bp, rowid);
        }
    }

    /// Delete a rowid, traversing interior nodes to find the correct leaf.
    /// Defragments the leaf page after deletion.
    pub fn delete(self: *Self, rowid: i64) BtreeError!bool {
        var page_id = self.root_page_id;

        while (true) {
            const page = self.pool.fetchPage(page_id) catch return BtreeError.PagerError;
            defer self.pool.releasePage(page);

            var bp = BtreePage{ .page = page, .page_size = self.page_size, .pool = self.pool };

            if (bp.isLeaf()) {
                const idx = try bp.searchTableLeaf(rowid);
                if (idx) |i| {
                    const count = bp.cellCount();
                    var j = i;
                    while (j < count - 1) : (j += 1) {
                        bp.setCellPointer(j, bp.cellPointer(j + 1));
                    }
                    const new_count = count - 1;
                    bp.setCellCount(new_count);

                    // Defragment
                    const data = bp.page.data;
                    const psize: usize = @intCast(bp.page_size);
                    var tmp_buf: [65536]u8 = undefined;
                    const tmp = tmp_buf[0..psize];
                    var sizes_buf: [4096]u16 = undefined;
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
                return false;
            }

            // Interior: descend
            page_id = try self.findChildPage(&bp, rowid);
        }
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
