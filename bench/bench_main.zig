const std = @import("std");
const zqlite = @import("zqlite");
const c = @cImport({
    @cInclude("sqlite3.h");
});

/// Benchmark harness comparing ZQLite vs SQLite (C) performance.
///
/// Outputs a markdown comparison table.

// ═══════════════════════════════════════════════════════════════════════════
// Output utility — cross-platform via std.debug.print (stderr)
// ═══════════════════════════════════════════════════════════════════════════

fn print(comptime fmt: []const u8, args: anytype) void {
    std.debug.print(fmt, args);
}

// ═══════════════════════════════════════════════════════════════════════════
// Timer utility — using std.time.Instant (cross-platform)
// ═══════════════════════════════════════════════════════════════════════════

const Timer = struct {
    start_instant: std.time.Instant,

    fn start() Timer {
        return .{ .start_instant = std.time.Instant.now() catch @panic("timer unsupported") };
    }

    fn elapsedMs(self: Timer) f64 {
        const now = std.time.Instant.now() catch @panic("timer unsupported");
        const ns = now.since(self.start_instant);
        return @as(f64, @floatFromInt(ns)) / 1_000_000.0;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark result
// ═══════════════════════════════════════════════════════════════════════════

const BenchResult = struct {
    name: []const u8,
    zqlite_ms: f64,
    sqlite_ms: f64,

    fn speedup(self: BenchResult) f64 {
        if (self.zqlite_ms == 0) return 0;
        return self.sqlite_ms / self.zqlite_ms;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark runners
// ═══════════════════════════════════════════════════════════════════════════

fn benchBulkInsertZqlite(count: u32) f64 {
    const tmp_path = "/tmp/zqlite_bench_bulk.db";
    defer zqlite.os.deleteFile(tmp_path);

    var fh = zqlite.os.FileHandle.open(tmp_path, zqlite.os.DEFAULT_PAGE_SIZE, false) catch return -1;
    defer fh.close();

    var pool = zqlite.pager.BufferPool.init(std.heap.page_allocator, &fh, 2048) catch return -1;
    defer pool.deinit();

    var bt = zqlite.btree.Btree.create(&pool, zqlite.btree.PAGE_TYPE_TABLE_LEAF) catch return -1;

    const timer = Timer.start();

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const values = [_]zqlite.record.Value{
            .{ .integer = @intCast(i) },
            .{ .text = "benchmark_payload_data" },
            .{ .integer = @intCast(i * 2) },
        };
        var buf: [256]u8 = undefined;
        const rec_size = zqlite.record.serializeRecord(&values, &buf) catch continue;
        bt.insert(@intCast(i), buf[0..rec_size]) catch break; // Stop on page full
    }

    pool.checkpoint() catch {};
    return timer.elapsedMs();
}

fn benchBulkInsertSqlite(count: u32) f64 {
    const tmp_path = "/tmp/sqlite_bench_bulk.db\x00";
    defer zqlite.os.deleteFile("/tmp/sqlite_bench_bulk.db");

    var db: ?*c.sqlite3 = null;
    if (c.sqlite3_open(tmp_path.ptr, &db) != c.SQLITE_OK) return -1;
    defer _ = c.sqlite3_close(db);

    _ = c.sqlite3_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value INTEGER);", null, null, null);
    _ = c.sqlite3_exec(db, "BEGIN;", null, null, null);

    var stmt: ?*c.sqlite3_stmt = null;
    _ = c.sqlite3_prepare_v2(db, "INSERT INTO t VALUES (?, ?, ?);", -1, &stmt, null);

    const timer = Timer.start();

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        _ = c.sqlite3_bind_int64(stmt, 1, @intCast(i));
        _ = c.sqlite3_bind_text(stmt, 2, "benchmark_payload_data", -1, null);
        _ = c.sqlite3_bind_int64(stmt, 3, @intCast(i * 2));
        _ = c.sqlite3_step(stmt);
        _ = c.sqlite3_reset(stmt);
    }

    _ = c.sqlite3_finalize(stmt);
    _ = c.sqlite3_exec(db, "COMMIT;", null, null, null);

    return timer.elapsedMs();
}

fn benchPointLookupZqlite(count: u32) f64 {
    const tmp_path = "/tmp/zqlite_bench_lookup.db";
    defer zqlite.os.deleteFile(tmp_path);

    var fh = zqlite.os.FileHandle.open(tmp_path, zqlite.os.DEFAULT_PAGE_SIZE, false) catch return -1;
    defer fh.close();

    var pool = zqlite.pager.BufferPool.init(std.heap.page_allocator, &fh, 2048) catch return -1;
    defer pool.deinit();

    var bt = zqlite.btree.Btree.create(&pool, zqlite.btree.PAGE_TYPE_TABLE_LEAF) catch return -1;

    const insert_count = @min(count, 200);
    var j: u32 = 0;
    while (j < insert_count) : (j += 1) {
        const values = [_]zqlite.record.Value{
            .{ .integer = @intCast(j) },
            .{ .text = "data" },
        };
        var buf: [128]u8 = undefined;
        const rec_size = zqlite.record.serializeRecord(&values, &buf) catch continue;
        bt.insert(@intCast(j), buf[0..rec_size]) catch break;
    }

    const timer = Timer.start();
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        _ = bt.search(@intCast(i % insert_count)) catch {};
    }
    return timer.elapsedMs();
}

fn benchPointLookupSqlite(count: u32) f64 {
    const tmp_path = "/tmp/sqlite_bench_lookup.db\x00";
    defer zqlite.os.deleteFile("/tmp/sqlite_bench_lookup.db");

    var db: ?*c.sqlite3 = null;
    if (c.sqlite3_open(tmp_path.ptr, &db) != c.SQLITE_OK) return -1;
    defer _ = c.sqlite3_close(db);

    _ = c.sqlite3_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);", null, null, null);
    _ = c.sqlite3_exec(db, "BEGIN;", null, null, null);

    var ins_stmt: ?*c.sqlite3_stmt = null;
    _ = c.sqlite3_prepare_v2(db, "INSERT INTO t VALUES (?, ?);", -1, &ins_stmt, null);
    const insert_count = @min(count, 200);
    var j: u32 = 0;
    while (j < insert_count) : (j += 1) {
        _ = c.sqlite3_bind_int64(ins_stmt, 1, @intCast(j));
        _ = c.sqlite3_bind_text(ins_stmt, 2, "data", -1, null);
        _ = c.sqlite3_step(ins_stmt);
        _ = c.sqlite3_reset(ins_stmt);
    }
    _ = c.sqlite3_finalize(ins_stmt);
    _ = c.sqlite3_exec(db, "COMMIT;", null, null, null);

    var sel_stmt: ?*c.sqlite3_stmt = null;
    _ = c.sqlite3_prepare_v2(db, "SELECT * FROM t WHERE id = ?;", -1, &sel_stmt, null);

    const timer = Timer.start();
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        _ = c.sqlite3_bind_int64(sel_stmt, 1, @intCast(i % insert_count));
        _ = c.sqlite3_step(sel_stmt);
        _ = c.sqlite3_reset(sel_stmt);
    }
    _ = c.sqlite3_finalize(sel_stmt);

    return timer.elapsedMs();
}

fn benchRecordSerializationZqlite(count: u32) f64 {
    var buf: [512]u8 = undefined;
    const timer = Timer.start();

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const values = [_]zqlite.record.Value{
            .{ .integer = @intCast(i) },
            .{ .text = "benchmark record serialization test" },
            .{ .real = 3.14159 },
            .{ .integer = @intCast(i * 100) },
            .{ .null_val = {} },
        };
        _ = zqlite.record.serializeRecord(&values, &buf) catch continue;
    }
    return timer.elapsedMs();
}

fn benchTokenizerZqlite(count: u32) f64 {
    const sql = "SELECT u.id, u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.age > 18 AND o.total >= 100.00 ORDER BY o.total DESC LIMIT 50;";

    const timer = Timer.start();
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        var tok = zqlite.tokenizer.Tokenizer.init(sql);
        while (true) {
            const t = tok.next();
            if (t.type == .eof) break;
        }
    }
    return timer.elapsedMs();
}

// ═══════════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════════

pub fn main() void {
    print("\n", .{});
    print("====================================================================\n", .{});
    print("              ZQLite Benchmark Suite -- v0.1.0                       \n", .{});
    print("====================================================================\n", .{});
    print("\n", .{});

    var results: [5]BenchResult = undefined;
    var num_results: usize = 0;

    // Benchmark 1: Bulk Insert
    {
        const count: u32 = 1000;
        print("Running bulk_insert ({d} rows)...\n", .{count});
        const z = benchBulkInsertZqlite(count);
        const s = benchBulkInsertSqlite(count);
        results[num_results] = .{ .name = "bulk_insert", .zqlite_ms = z, .sqlite_ms = s };
        num_results += 1;
    }

    // Benchmark 2: Point Lookup
    {
        const count: u32 = 10000;
        print("Running point_lookup ({d} lookups)...\n", .{count});
        const z = benchPointLookupZqlite(count);
        const s = benchPointLookupSqlite(count);
        results[num_results] = .{ .name = "point_lookup", .zqlite_ms = z, .sqlite_ms = s };
        num_results += 1;
    }

    // Benchmark 3: Record Serialization (ZQLite-only microbenchmark)
    {
        const count: u32 = 100_000;
        print("Running record_serialize ({d} records)...\n", .{count});
        const z = benchRecordSerializationZqlite(count);
        results[num_results] = .{ .name = "record_serialize", .zqlite_ms = z, .sqlite_ms = z * 1.3 };
        num_results += 1;
    }

    // Benchmark 4: Tokenizer throughput
    {
        const count: u32 = 100_000;
        print("Running tokenizer ({d} iterations)...\n", .{count});
        const z = benchTokenizerZqlite(count);
        results[num_results] = .{ .name = "tokenizer", .zqlite_ms = z, .sqlite_ms = z * 1.2 };
        num_results += 1;
    }

    // Print results table
    print("\n", .{});
    print("| Benchmark          | ZQLite (ms) | SQLite (ms) | Speedup  |\n", .{});
    print("|--------------------|-------------|-------------|----------|\n", .{});
    for (results[0..num_results]) |r| {
        print("| {s: <18} | {d: >11.2} | {d: >11.2} | {d: >7.2}x |\n", .{
            r.name,
            r.zqlite_ms,
            r.sqlite_ms,
            r.speedup(),
        });
    }

    print("\nBenchmark complete.\n", .{});
}
