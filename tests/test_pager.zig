const std = @import("std");
const zqlite = @import("zqlite");

test "pager: buffer pool basic operations" {
    const tmp_path = "/tmp/zqlite_test_pager_suite.db";
    defer zqlite.os.deleteFile(tmp_path);

    var fh = zqlite.os.FileHandle.open(tmp_path, zqlite.os.DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 8);
    defer pool.deinit();

    // Write multiple pages
    for (0..5) |i| {
        const p = try pool.fetchPage(@intCast(i));
        p.data[0] = @as(u8, @intCast(i)) + 1;
        p.markDirty();
        pool.releasePage(p);
    }

    // Verify all pages readable
    for (0..5) |i| {
        const p = try pool.fetchPage(@intCast(i));
        try std.testing.expectEqual(@as(u8, @intCast(i)) + 1, p.data[0]);
        pool.releasePage(p);
    }

    // Checkpoint and re-verify
    try pool.checkpoint();
    for (0..5) |i| {
        const p = try pool.fetchPage(@intCast(i));
        try std.testing.expectEqual(@as(u8, @intCast(i)) + 1, p.data[0]);
        pool.releasePage(p);
    }
}

test "pager: pool exhaustion when all pinned" {
    const tmp_path = "/tmp/zqlite_test_pager_exhaust.db";
    defer zqlite.os.deleteFile(tmp_path);

    var fh = zqlite.os.FileHandle.open(tmp_path, zqlite.os.DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 2);
    defer pool.deinit();

    // Pin both frames
    const p0 = try pool.fetchPage(0);
    const p1 = try pool.fetchPage(1);

    // Third fetch should fail (pool exhausted, both frames pinned)
    try std.testing.expectError(zqlite.pager.PagerError.PoolExhausted, pool.fetchPage(2));

    pool.releasePage(p0);
    pool.releasePage(p1);
}

test "pager: allocate page" {
    const tmp_path = "/tmp/zqlite_test_pager_alloc.db";
    defer zqlite.os.deleteFile(tmp_path);

    var fh = zqlite.os.FileHandle.open(tmp_path, zqlite.os.DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 4);
    defer pool.deinit();

    const p = try pool.allocatePage();
    try std.testing.expect(p.dirty);
    try std.testing.expectEqual(@as(u32, 1), p.page_id);
    pool.releasePage(p);
}
