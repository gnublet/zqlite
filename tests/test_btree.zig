const std = @import("std");
const zqlite = @import("zqlite");

test "btree: insert, search, and delete multiple rows" {
    const tmp_path = "/tmp/zqlite_test_btree_suite.db";
    defer zqlite.os.deleteFile(tmp_path);

    var fh = zqlite.os.FileHandle.open(tmp_path, zqlite.os.DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 8);
    defer pool.deinit();

    var bt = try zqlite.btree.Btree.create(&pool, zqlite.btree.PAGE_TYPE_TABLE_LEAF);

    // Insert rows in random order
    const keys = [_]i64{ 50, 20, 80, 10, 30, 70, 90, 40, 60 };
    for (keys) |k| {
        var payload: [10]u8 = undefined;
        @memset(&payload, @as(u8, @intCast(@as(u64, @bitCast(k)) % 256)));
        try bt.insert(k, &payload);
    }

    // Search for each key
    for (keys) |k| {
        const result = try bt.search(k);
        try std.testing.expect(result != null);
        try std.testing.expectEqual(k, result.?.key);
    }

    // Delete a few keys
    try std.testing.expect(try bt.delete(20));
    try std.testing.expect(try bt.delete(80));
    try std.testing.expect(try bt.delete(50));

    // Verify deleted
    try std.testing.expect((try bt.search(20)) == null);
    try std.testing.expect((try bt.search(80)) == null);
    try std.testing.expect((try bt.search(50)) == null);

    // Remaining keys still present
    try std.testing.expect((try bt.search(10)) != null);
    try std.testing.expect((try bt.search(30)) != null);
    try std.testing.expect((try bt.search(90)) != null);
}

test "btree: ordered cursor iteration" {
    const tmp_path = "/tmp/zqlite_test_btree_cursor.db";
    defer zqlite.os.deleteFile(tmp_path);

    var fh = zqlite.os.FileHandle.open(tmp_path, zqlite.os.DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 4);
    defer pool.deinit();

    var bt = try zqlite.btree.Btree.create(&pool, zqlite.btree.PAGE_TYPE_TABLE_LEAF);

    // Insert in disorder
    try bt.insert(30, "thirty");
    try bt.insert(10, "ten");
    try bt.insert(20, "twenty");

    // Iterate in order
    var cur = zqlite.cursor.Cursor.init(&bt);
    try cur.first();

    var expected = [_]i64{ 10, 20, 30 };
    for (&expected) |exp| {
        try std.testing.expect(cur.valid);
        try std.testing.expectEqual(exp, try cur.key());
        _ = try cur.next();
    }
}
