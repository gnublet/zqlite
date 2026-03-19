const std = @import("std");
const zqlite = @import("zqlite");

// End-to-end integration tests: SQL string → parsed → compiled → executed → result rows.

test "integration: SELECT constant expression" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = zqlite.parser.Parser.init("SELECT 1 + 2 * 3;", arena.allocator());
    const stmt = try p.parseStatement();

    var compiler = zqlite.codegen.Compiler.init(std.testing.allocator);
    defer compiler.deinit();
    const program = try compiler.compile(stmt);

    var vm_inst = try zqlite.vm.VM.init(std.testing.allocator, program);
    defer vm_inst.deinit();
    try vm_inst.execute();

    try std.testing.expectEqual(@as(usize, 1), vm_inst.results.items.len);
    // 1 + 2 * 3 => with left-to-right parsing: (1 + 2) * 3 = 9 or
    // with proper precedence: 1 + (2 * 3) = 7
    const val = vm_inst.results.items[0].values[0].integer;
    try std.testing.expectEqual(@as(i64, 7), val);
}

test "integration: SELECT string literal" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = zqlite.parser.Parser.init("SELECT 'hello world';", arena.allocator());
    const stmt = try p.parseStatement();

    var compiler = zqlite.codegen.Compiler.init(std.testing.allocator);
    defer compiler.deinit();
    const program = try compiler.compile(stmt);

    var vm_inst = try zqlite.vm.VM.init(std.testing.allocator, program);
    defer vm_inst.deinit();
    try vm_inst.execute();

    try std.testing.expectEqualStrings("hello world", vm_inst.results.items[0].values[0].text);
}

test "integration: SELECT multiple expressions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = zqlite.parser.Parser.init("SELECT 42, 'zqlite', 1 + 1;", arena.allocator());
    const stmt = try p.parseStatement();

    var compiler = zqlite.codegen.Compiler.init(std.testing.allocator);
    defer compiler.deinit();
    const program = try compiler.compile(stmt);

    var vm_inst = try zqlite.vm.VM.init(std.testing.allocator, program);
    defer vm_inst.deinit();
    try vm_inst.execute();

    const row = vm_inst.results.items[0];
    try std.testing.expectEqual(@as(i64, 42), row.values[0].integer);
    try std.testing.expectEqualStrings("zqlite", row.values[1].text);
    try std.testing.expectEqual(@as(i64, 2), row.values[2].integer);
}

test "integration: SELECT with negation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = zqlite.parser.Parser.init("SELECT -42;", arena.allocator());
    const stmt = try p.parseStatement();

    var compiler = zqlite.codegen.Compiler.init(std.testing.allocator);
    defer compiler.deinit();
    const program = try compiler.compile(stmt);

    var vm_inst = try zqlite.vm.VM.init(std.testing.allocator, program);
    defer vm_inst.deinit();
    try vm_inst.execute();

    try std.testing.expectEqual(@as(i64, -42), vm_inst.results.items[0].values[0].integer);
}

test "integration: btree + record round-trip" {
    const tmp_path = "/tmp/zqlite_test_integration_btree.db";
    defer zqlite.os.deleteFile(tmp_path);

    var fh = zqlite.os.FileHandle.open(tmp_path, zqlite.os.DEFAULT_PAGE_SIZE, false) catch
        return error.SkipZigTest;
    defer fh.close();

    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 8);
    defer pool.deinit();

    var bt = try zqlite.btree.Btree.create(&pool, zqlite.btree.PAGE_TYPE_TABLE_LEAF);

    // Serialize a record and insert into B-tree
    const values = [_]zqlite.record.Value{
        .{ .integer = 1 },
        .{ .text = "Alice" },
        .{ .integer = 30 },
    };
    var buf: [256]u8 = undefined;
    const rec_size = try zqlite.record.serializeRecord(&values, &buf);

    try bt.insert(1, buf[0..rec_size]);

    // Retrieve and deserialize
    const result = try bt.search(1);
    try std.testing.expect(result != null);

    const decoded = try zqlite.record.deserializeRecord(result.?.payload, std.testing.allocator);
    defer std.testing.allocator.free(decoded);

    try std.testing.expectEqual(@as(i64, 1), decoded[0].asInteger().?);
    try std.testing.expectEqualStrings("Alice", decoded[1].asText().?);
    try std.testing.expectEqual(@as(i64, 30), decoded[2].asInteger().?);
}
