const std = @import("std");
const zqlite = @import("zqlite");

test "btree: insert beyond single page capacity" {
    const os = zqlite.os;
    const pager = zqlite.pager;
    const btree = zqlite.btree;

    const tmp_path = "/tmp/zqlite_test_split.db";
    defer os.deleteFile(tmp_path);

    var fh = os.FileHandle.open(tmp_path, os.DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    var pool = pager.BufferPool.init(std.testing.allocator, &fh, 64) catch
        return error.SkipZigTest;
    defer pool.deinit();

    var bt = try btree.Btree.create(&pool, btree.PAGE_TYPE_TABLE_LEAF);

    // Insert enough rows to force multiple page splits
    const N = 200;
    var i: i64 = 0;
    while (i < N) : (i += 1) {
        // Create a small payload for each row
        var payload: [32]u8 = undefined;
        @memset(&payload, @intCast(i & 0xFF));
        bt.insert(i, &payload) catch |err| {
            std.debug.print("Insert failed at key {}: {}\n", .{ i, err });
            return err;
        };
    }

    // Verify all rows can be found
    i = 0;
    while (i < N) : (i += 1) {
        const result = bt.search(i) catch |err| {
            std.debug.print("Search failed at key {}: {}\n", .{ i, err });
            return err;
        };
        if (result == null) {
            std.debug.print("Key {} not found!\n", .{i});
            return error.TestUnexpectedResult;
        }
        try std.testing.expectEqual(i, result.?.key);
    }

    // Flush dirty pages to disk so file_size reflects actual content
    pool.flushAll() catch {};

    // Verify file has more than 1 page
    const page_count = pool.next_page_id;
    std.debug.print("Inserted {} rows, file has {} pages ({} bytes)\n", .{ N, page_count, page_count * fh.page_size });
    try std.testing.expect(page_count > 1);
}

test "executor+journal: insert beyond page capacity" {
    const os = zqlite.os;
    const pager = zqlite.pager;

    const tmp_path = "/tmp/zqlite_test_split_exec.db";
    defer os.deleteFile(tmp_path);

    var fh = os.FileHandle.open(tmp_path, os.DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    var pool = pager.BufferPool.init(std.testing.allocator, &fh, 64) catch
        return error.SkipZigTest;
    defer pool.deinit();

    var journal = zqlite.journal.Journal.init(std.testing.allocator, tmp_path, fh.page_size, &fh);
    defer journal.deinit();
    pool.setJournal(&journal);

    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var schema_store = zqlite.schema.Schema.init(exec_arena.allocator());
    defer schema_store.deinit();

    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);
    exec.setJournal(&journal);
    exec.setFile(&fh);

    // Create table
    {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        var parser = zqlite.parser.Parser.init(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);",
            arena.allocator(),
        );
        const stmt = try parser.parseStatement();
        _ = try exec.execute(stmt, arena.allocator());
    }

    // Insert 200 rows (enough to trigger splits)
    var i: i64 = 0;
    while (i < 200) : (i += 1) {
        var buf: [64]u8 = undefined;
        const sql = std.fmt.bufPrint(&buf, "INSERT INTO t VALUES ({}, {});", .{ i, i * 7 }) catch unreachable;
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        var parser = zqlite.parser.Parser.init(sql, arena.allocator());
        const stmt = parser.parseStatement() catch |err| {
            std.debug.print("Parse failed at row {}: {}\n", .{ i, err });
            return err;
        };
        _ = exec.execute(stmt, arena.allocator()) catch |err| {
            std.debug.print("Exec failed at row {}: {}\n", .{ i, err });
            return err;
        };
    }

    // Search for rows via executor
    {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        var parser = zqlite.parser.Parser.init(
            "SELECT * FROM t WHERE id = 25;",
            arena.allocator(),
        );
        const stmt = try parser.parseStatement();
        const result = try exec.execute(stmt, arena.allocator());
        std.debug.print("SELECT id=25: {} rows\n", .{result.rows.len});
        try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    }

    // Verify file grew beyond 1 page
    const page_count = pool.next_page_id;
    std.debug.print("Executor+journal: {} pages allocated\n", .{page_count});
    try std.testing.expect(page_count > 1);
}
