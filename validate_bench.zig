const std = @import("std");
const zqlite = @import("src/root.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const tmp_path = "/tmp/zqlite_bench_idx_val.db";
    _ = zqlite.os.deleteFile(tmp_path);

    var fh = try zqlite.os.FileHandle.open(tmp_path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();

    var pool = try zqlite.pager.BufferPool.init(allocator, &fh, 2048);
    defer pool.deinit();

    var schema_store = zqlite.schema.Schema.init(allocator);
    defer schema_store.deinit();
    var exec = zqlite.executor.Executor.init(allocator, &pool, &schema_store);
    defer exec.deinit();
    exec.setFile(&fh);

    var parse_arena = std.heap.ArenaAllocator.init(allocator);
    defer parse_arena.deinit();
    var p0 = zqlite.parser.Parser.init("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, category INTEGER)", parse_arena.allocator());
    _ = try exec.execute(p0.parseStatement() catch unreachable, parse_arena.allocator());

    var i: u32 = 0;
    while (i < 200) : (i += 1) {
        var buf: [128]u8 = undefined;
        const sql = try std.fmt.bufPrint(&buf, "INSERT INTO items VALUES ({}, 'item', {});", .{i, i % 10});
        var pa = std.heap.ArenaAllocator.init(allocator);
        defer pa.deinit();
        var p = zqlite.parser.Parser.init(sql, pa.allocator());
        _ = try exec.execute(p.parseStatement() catch unreachable, pa.allocator());
    }

    var pa2 = std.heap.ArenaAllocator.init(allocator);
    defer pa2.deinit();
    var p2 = zqlite.parser.Parser.init("CREATE INDEX idx_category ON items (category)", pa2.allocator());
    _ = try exec.execute(p2.parseStatement() catch unreachable, pa2.allocator());

    var pa3 = std.heap.ArenaAllocator.init(allocator);
    defer pa3.deinit();
    var p3 = zqlite.parser.Parser.init("SELECT * FROM items WHERE category = ?", pa3.allocator());
    var prepared = try exec.prepare(p3.parseStatement() catch unreachable, null);
    defer prepared.deinit();

    var rows: usize = 0;
    const params = [_]zqlite.record.Value{ .{ .integer = 5 } };
    prepared.bindParams(&params);
    while (try prepared.step()) |_| {
        rows += 1;
    }
    std.debug.print("Index lookup returned {} rows\n", .{rows});
}
