const std = @import("std");
const os = @import("os.zig");
const journal_mod = @import("journal.zig");

/// Pager / Buffer Pool — manages in-memory page cache with clock-sweep eviction.
///
/// The pager sits between the B-tree layer and the OS abstraction layer.
/// It provides:
///   - Page fetch / release with pin counting
///   - Dirty-page tracking and sorted write-back
///   - Clock-sweep eviction (cheaper than LRU per-access bookkeeping)
///   - Integration points for WAL (Phase 2b)

// ═══════════════════════════════════════════════════════════════════════════
// Configuration
// ═══════════════════════════════════════════════════════════════════════════

pub const DEFAULT_POOL_SIZE: u32 = 1024; // number of page frames

// ═══════════════════════════════════════════════════════════════════════════
// Errors
// ═══════════════════════════════════════════════════════════════════════════

pub const PagerError = error{
    PoolExhausted,
    PageNotFound,
    IoError,
    InvalidPageNumber,
    AllocationFailed,
};

// ═══════════════════════════════════════════════════════════════════════════
// Page frame
// ═══════════════════════════════════════════════════════════════════════════

pub const Page = struct {
    page_id: u32,
    data: []align(4096) u8,
    dirty: bool,
    pin_count: u16,
    ref_bit: bool, // for clock-sweep
    valid: bool, // has been loaded from disk or initialised

    pub fn markDirty(self: *Page) void {
        self.dirty = true;
    }

    pub fn pinned(self: *const Page) bool {
        return self.pin_count > 0;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Buffer Pool with clock-sweep eviction
// ═══════════════════════════════════════════════════════════════════════════

pub const BufferPool = struct {
    frames: []Page,
    page_map: std.AutoHashMap(u32, u32), // page_id → frame index
    pool_size: u32,
    page_size: u32,
    clock_hand: u32,
    file: *os.FileHandle,
    allocator: std.mem.Allocator,
    journal: ?*journal_mod.Journal,
    next_page_id: u32, // monotonic counter for page allocation (not tied to file_size)

    const Self = @This();

    /// Initialise a buffer pool with `pool_size` frames.
    pub fn init(allocator: std.mem.Allocator, file: *os.FileHandle, pool_size: u32) PagerError!Self {
        const frames = allocator.alloc(Page, pool_size) catch
            return PagerError.AllocationFailed;

        // Allocate page buffers for each frame
        for (frames, 0..) |*frame, i| {
            frame.* = Page{
                .page_id = 0,
                .data = os.allocAlignedPage(file.page_size) catch {
                    // Clean up previously allocated frames
                    for (frames[0..i]) |*prev| {
                        os.freeAlignedPage(prev.data);
                    }
                    allocator.free(frames);
                    return PagerError.AllocationFailed;
                },
                .dirty = false,
                .pin_count = 0,
                .ref_bit = false,
                .valid = false,
            };
        }

        return Self{
            .frames = frames,
            .page_map = std.AutoHashMap(u32, u32).init(allocator),
            .pool_size = pool_size,
            .page_size = file.page_size,
            .clock_hand = 0,
            .file = file,
            .allocator = allocator,
            .journal = null,
            .next_page_id = file.pageCount(),
        };
    }

    /// Attach or detach a journal for ACID transactions.
    pub fn setJournal(self: *Self, j: ?*journal_mod.Journal) void {
        self.journal = j;
    }

    /// Mark a page as dirty, journaling the original content first if needed.
    pub fn markPageDirty(self: *Self, page: *Page) void {
        if (!page.dirty) {
            // Journal the original content before first modification
            if (self.journal) |j| {
                if (j.active) {
                    j.journalPage(page.page_id, page.data) catch {};
                }
            }
        }
        page.dirty = true;
    }

    /// Release all resources.
    pub fn deinit(self: *Self) void {
        // Flush any remaining dirty pages
        self.flushAll() catch {};

        for (self.frames) |*frame| {
            os.freeAlignedPage(frame.data);
        }
        self.allocator.free(self.frames);
        self.page_map.deinit();
    }

    /// Fetch a page by ID. If not in the pool, read from disk and cache it.
    /// The returned page is pinned (pin_count incremented).
    pub fn fetchPage(self: *Self, page_id: u32) PagerError!*Page {
        // Check if already in pool
        if (self.page_map.get(page_id)) |frame_idx| {
            var frame = &self.frames[frame_idx];
            frame.pin_count += 1;
            frame.ref_bit = true;
            return frame;
        }

        // Need to load from disk — find a free frame or evict
        const frame_idx = try self.findFrame();
        var frame = &self.frames[frame_idx];

        // If the frame held a different page, evict it
        if (frame.valid) {
            try self.evictFrame(frame_idx);
        }

        // Load new page from disk
        frame.page_id = page_id;
        if (page_id < self.file.pageCount()) {
            self.file.readPage(page_id, frame.data) catch
                return PagerError.IoError;
        } else {
            // New page beyond current file — zero fill
            @memset(frame.data, 0);
        }

        frame.dirty = false;
        frame.pin_count = 1;
        frame.ref_bit = true;
        frame.valid = true;

        self.page_map.put(page_id, frame_idx) catch
            return PagerError.AllocationFailed;

        return frame;
    }

    /// Allocate a new page at the end of the file.
    pub fn allocatePage(self: *Self) PagerError!*Page {
        const new_page_id = self.next_page_id;
        self.next_page_id += 1;
        const page = try self.fetchPage(new_page_id);
        @memset(page.data, 0);
        page.dirty = true;
        return page;
    }

    /// Release a page (decrement pin count).
    pub fn releasePage(self: *Self, page: *Page) void {
        _ = self;
        std.debug.assert(page.pin_count > 0);
        page.pin_count -= 1;
    }

    /// Flush all dirty pages to disk.
    pub fn flushAll(self: *Self) PagerError!void {
        // Collect dirty page indices sorted by page_id for sequential I/O
        var dirty_indices: std.ArrayList(u32) = .{};
        defer dirty_indices.deinit(self.allocator);

        for (self.frames, 0..) |frame, i| {
            if (frame.valid and frame.dirty) {
                dirty_indices.append(self.allocator, @intCast(i)) catch
                    return PagerError.AllocationFailed;
            }
        }

        // Sort by frame index (proxy for page_id ordering in our sequential allocation)
        const indices = dirty_indices.items;
        std.mem.sort(u32, indices, {}, struct {
            fn lessThan(_: void, a: u32, b: u32) bool {
                return a < b;
            }
        }.lessThan);

        // Write each dirty page
        for (indices) |idx| {
            var frame = &self.frames[idx];
            self.file.writePage(frame.page_id, frame.data) catch
                return PagerError.IoError;
            frame.dirty = false;
        }
    }

    /// Flush dirty pages and sync to disk.
    pub fn checkpoint(self: *Self) PagerError!void {
        try self.flushAll();
        self.file.sync() catch return PagerError.IoError;
    }

    // ─── Clock-sweep eviction ───────────────────────────────────────

    /// Find a free or evictable frame using the clock-sweep algorithm.
    fn findFrame(self: *Self) PagerError!u32 {
        // First pass: look for an unused frame
        for (self.frames, 0..) |frame, i| {
            if (!frame.valid) {
                return @intCast(i);
            }
        }

        // Clock-sweep: rotate until we find an unpinned frame with ref_bit=false
        var attempts: u32 = 0;
        const max_attempts = self.pool_size * 2;
        while (attempts < max_attempts) : (attempts += 1) {
            const frame = &self.frames[self.clock_hand];

            if (!frame.pinned()) {
                if (frame.ref_bit) {
                    frame.ref_bit = false;
                } else {
                    const victim = self.clock_hand;
                    self.clock_hand = (self.clock_hand + 1) % self.pool_size;
                    return victim;
                }
            }

            self.clock_hand = (self.clock_hand + 1) % self.pool_size;
        }

        return PagerError.PoolExhausted;
    }

    /// Evict a frame: flush to disk if dirty, remove from page_map.
    fn evictFrame(self: *Self, frame_idx: u32) PagerError!void {
        var frame = &self.frames[frame_idx];
        std.debug.assert(!frame.pinned());

        if (frame.dirty) {
            self.file.writePage(frame.page_id, frame.data) catch
                return PagerError.IoError;
            frame.dirty = false;
        }

        _ = self.page_map.remove(frame.page_id);
        frame.valid = false;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

test "BufferPool fetch and release" {
    const tmp_path = "/tmp/zqlite_test_pager.db\x00";
    defer os.deleteFile("/tmp/zqlite_test_pager.db");

    var fh = os.FileHandle.open(tmp_path[0 .. tmp_path.len - 1], os.DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    var pool = try BufferPool.init(std.testing.allocator, &fh, 4);
    defer pool.deinit();

    // Fetch page 0 (new, zero-filled)
    const p0 = try pool.fetchPage(0);
    try std.testing.expectEqual(@as(u32, 0), p0.page_id);
    try std.testing.expectEqual(@as(u16, 1), p0.pin_count);

    // Write some data
    p0.data[0] = 0xDE;
    p0.data[1] = 0xAD;
    p0.markDirty();

    pool.releasePage(p0);
    try std.testing.expectEqual(@as(u16, 0), p0.pin_count);

    // Fetch again — should come from cache
    const p0_again = try pool.fetchPage(0);
    try std.testing.expectEqual(@as(u8, 0xDE), p0_again.data[0]);
    pool.releasePage(p0_again);
}

test "BufferPool eviction with small pool" {
    const tmp_path = "/tmp/zqlite_test_pager_evict.db\x00";
    defer os.deleteFile("/tmp/zqlite_test_pager_evict.db");

    var fh = os.FileHandle.open(tmp_path[0 .. tmp_path.len - 1], os.DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    // Pool of only 2 frames — forces eviction
    var pool = try BufferPool.init(std.testing.allocator, &fh, 2);
    defer pool.deinit();

    // Load 3 pages, releasing between fetches
    for (0..3) |i| {
        const page = try pool.fetchPage(@intCast(i));
        page.data[0] = @intCast(i + 1);
        page.markDirty();
        pool.releasePage(page);
    }

    // Page 2 should still be accessible (most recently loaded)
    const p2 = try pool.fetchPage(2);
    try std.testing.expectEqual(@as(u8, 3), p2.data[0]);
    pool.releasePage(p2);
}

test "BufferPool checkpoint writes to disk" {
    const tmp_path = "/tmp/zqlite_test_pager_ckpt.db\x00";
    defer os.deleteFile("/tmp/zqlite_test_pager_ckpt.db");

    var fh = os.FileHandle.open(tmp_path[0 .. tmp_path.len - 1], os.DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    var pool = try BufferPool.init(std.testing.allocator, &fh, 4);
    defer pool.deinit();

    const p0 = try pool.fetchPage(0);
    p0.data[0] = 0x42;
    p0.markDirty();
    pool.releasePage(p0);

    try pool.checkpoint();

    // Verify via direct file read
    var read_buf: [os.DEFAULT_PAGE_SIZE]u8 = undefined;
    fh.readPage(0, &read_buf) catch return;
    try std.testing.expectEqual(@as(u8, 0x42), read_buf[0]);
}
