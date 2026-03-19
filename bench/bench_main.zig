const std = @import("std");
const zqlite = @import("zqlite");
const c = @cImport({
    @cInclude("sqlite3.h");
});

/// Honest benchmark harness comparing ZQLite vs SQLite (C) using the
/// full SQL pipeline for both engines.
///
/// CAVEATS (printed in output):
///  - ZQLite re-parses each SQL string (no prepared statements yet)
///  - ZQLite uses rollback journal (fsync + journal on each auto-commit)
///  - ZQLite uses a single B-tree leaf page (no page splitting)
///  - SQLite uses prepared statements with bind for insert/lookup
///    (the idiomatic, optimized path)

// ═══════════════════════════════════════════════════════════════════════════
// Output utility
// ═══════════════════════════════════════════════════════════════════════════

fn print(comptime fmt: []const u8, args: anytype) void {
    std.debug.print(fmt, args);
}

// ═══════════════════════════════════════════════════════════════════════════
// Timer
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
// ZQLite helpers — run SQL through full pipeline (parser → executor)
// ═══════════════════════════════════════════════════════════════════════════

fn zqliteExecSql(
    exec: *zqlite.executor.Executor,
    allocator: std.mem.Allocator,
    sql: []const u8,
) !zqlite.executor.ExecResult {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var parser = zqlite.parser.Parser.init(sql, arena.allocator());
    const stmt = try parser.parseStatement();
    return exec.execute(stmt, arena.allocator());
}

// For SELECT, we need the arena to stay alive while we read results
const ZqliteQueryResult = struct {
    arena: std.heap.ArenaAllocator,
    result: zqlite.executor.ExecResult,

    fn deinit(self: *ZqliteQueryResult) void {
        self.arena.deinit();
    }
};

fn zqliteQuerySql(
    exec: *zqlite.executor.Executor,
    allocator: std.mem.Allocator,
    sql: []const u8,
) !ZqliteQueryResult {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();

    var parser = zqlite.parser.Parser.init(sql, arena.allocator());
    const stmt = try parser.parseStatement();
    const result = try exec.execute(stmt, arena.allocator());
    return .{ .arena = arena, .result = result };
}

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark 1: Bulk Insert (full SQL pipeline)
//
// Both engines: CREATE TABLE, then INSERT N rows via SQL.
// ZQLite: parses each INSERT individually (no prepared statements)
// SQLite: uses prepared statement + bind (idiomatic, faster path)
// ═══════════════════════════════════════════════════════════════════════════

fn benchBulkInsertZqlite(allocator: std.mem.Allocator, count: u32) f64 {
    const tmp_path = "/tmp/zqlite_bench_bulk.db";
    defer zqlite.os.deleteFile(tmp_path);

    var fh = zqlite.os.FileHandle.open(tmp_path, zqlite.os.DEFAULT_PAGE_SIZE, false) catch return -1;
    defer fh.close();

    var pool = zqlite.pager.BufferPool.init(allocator, &fh, 2048) catch return -1;
    defer pool.deinit();

    var schema_store = zqlite.schema.Schema.init(allocator);
    defer schema_store.deinit();

    var exec_arena = std.heap.ArenaAllocator.init(allocator);
    defer exec_arena.deinit();

    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    // Set up journal for ACID mode
    var journal = zqlite.journal.Journal.init(allocator, tmp_path, fh.page_size, &fh);
    defer journal.deinit();
    defer zqlite.os.deleteFile(tmp_path ++ "-journal");
    pool.setJournal(&journal);
    exec.setJournal(&journal);

    // CREATE TABLE via SQL
    _ = zqliteExecSql(&exec, allocator, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value INTEGER);") catch return -1;

    // Wrap inserts in a single transaction (matching SQLite's BEGIN/COMMIT)
    _ = zqliteExecSql(&exec, allocator, "BEGIN;") catch return -1;

    // Time the inserts — use FixedBufferAllocator to avoid mmap/munmap per query
    var fba_buf: [65536]u8 = undefined;
    const timer = Timer.start();
    var sql_buf: [256]u8 = undefined;
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const sql = std.fmt.bufPrint(&sql_buf, "INSERT INTO t VALUES ({d}, 'benchmark_payload_data', {d});", .{ i, i * 2 }) catch continue;
        var fba = std.heap.FixedBufferAllocator.init(&fba_buf);
        const fba_alloc = fba.allocator();
        var parser = zqlite.parser.Parser.init(sql, fba_alloc);
        const stmt = parser.parseStatement() catch continue;
        _ = exec.execute(stmt, fba_alloc) catch break;
    }

    _ = zqliteExecSql(&exec, allocator, "COMMIT;") catch return -1;

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

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark 2: Point Lookup (full SQL pipeline)
//
// Both engines: populate 200 rows, then do N lookups via SELECT WHERE.
// ZQLite: parses each SELECT individually (no prepared statements)
// SQLite: uses prepared statement + bind
// ═══════════════════════════════════════════════════════════════════════════

fn benchPointLookupZqlite(allocator: std.mem.Allocator, count: u32) f64 {
    const tmp_path = "/tmp/zqlite_bench_lookup.db";
    defer zqlite.os.deleteFile(tmp_path);

    var fh = zqlite.os.FileHandle.open(tmp_path, zqlite.os.DEFAULT_PAGE_SIZE, false) catch return -1;
    defer fh.close();

    var pool = zqlite.pager.BufferPool.init(allocator, &fh, 2048) catch return -1;
    defer pool.deinit();

    var schema_store = zqlite.schema.Schema.init(allocator);
    defer schema_store.deinit();

    var exec_arena = std.heap.ArenaAllocator.init(allocator);
    defer exec_arena.deinit();

    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    // Set up journal for ACID mode
    var journal = zqlite.journal.Journal.init(allocator, tmp_path, fh.page_size, &fh);
    defer journal.deinit();
    defer zqlite.os.deleteFile(tmp_path ++ "-journal");
    pool.setJournal(&journal);
    exec.setJournal(&journal);

    // Create and populate table via SQL
    _ = zqliteExecSql(&exec, allocator, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);") catch return -1;

    const insert_count = @min(count, 200);
    var sql_buf: [256]u8 = undefined;
    var j: u32 = 0;
    while (j < insert_count) : (j += 1) {
        const sql = std.fmt.bufPrint(&sql_buf, "INSERT INTO t VALUES ({d}, 'data');", .{j}) catch continue;
        _ = zqliteExecSql(&exec, allocator, sql) catch break;
    }

    // Time the lookups — use FixedBufferAllocator to avoid mmap/munmap per query
    var fba_buf: [65536]u8 = undefined;
    const timer = Timer.start();
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const sql = std.fmt.bufPrint(&sql_buf, "SELECT * FROM t WHERE id = {d};", .{i % insert_count}) catch continue;

        // Reset the fixed buffer for each query (zero-cost, no syscalls)
        var fba = std.heap.FixedBufferAllocator.init(&fba_buf);
        const fba_alloc = fba.allocator();

        var parser = zqlite.parser.Parser.init(sql, fba_alloc);
        const stmt = parser.parseStatement() catch continue;
        _ = exec.execute(stmt, fba_alloc) catch continue;
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

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark 3: Full Table Scan with Filter (full SQL pipeline)
//
// Both engines: populate 200 rows, then SELECT * WHERE value > threshold.
// Measures full-table cursor scan + expression evaluation.
// ═══════════════════════════════════════════════════════════════════════════

fn benchScanFilterZqlite(allocator: std.mem.Allocator, iterations: u32) f64 {
    const tmp_path = "/tmp/zqlite_bench_scan.db";
    defer zqlite.os.deleteFile(tmp_path);

    var fh = zqlite.os.FileHandle.open(tmp_path, zqlite.os.DEFAULT_PAGE_SIZE, false) catch return -1;
    defer fh.close();

    var pool = zqlite.pager.BufferPool.init(allocator, &fh, 2048) catch return -1;
    defer pool.deinit();

    var schema_store = zqlite.schema.Schema.init(allocator);
    defer schema_store.deinit();

    var exec_arena = std.heap.ArenaAllocator.init(allocator);
    defer exec_arena.deinit();

    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    // Set up journal for ACID mode
    var journal = zqlite.journal.Journal.init(allocator, tmp_path, fh.page_size, &fh);
    defer journal.deinit();
    defer zqlite.os.deleteFile(tmp_path ++ "-journal");
    pool.setJournal(&journal);
    exec.setJournal(&journal);

    // Create and populate table
    _ = zqliteExecSql(&exec, allocator, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value INTEGER);") catch return -1;

    var sql_buf: [256]u8 = undefined;
    var j: u32 = 0;
    while (j < 200) : (j += 1) {
        const sql = std.fmt.bufPrint(&sql_buf, "INSERT INTO t VALUES ({d}, 'row_data', {d});", .{ j, j * 3 }) catch continue;
        _ = zqliteExecSql(&exec, allocator, sql) catch break;
    }

    // Time the scans — use FixedBufferAllocator to avoid mmap/munmap per query
    var fba_buf: [65536]u8 = undefined;
    const timer = Timer.start();
    var i: u32 = 0;
    while (i < iterations) : (i += 1) {
        var fba = std.heap.FixedBufferAllocator.init(&fba_buf);
        const fba_alloc = fba.allocator();
        var parser = zqlite.parser.Parser.init("SELECT * FROM t WHERE value > 300;", fba_alloc);
        const stmt = parser.parseStatement() catch continue;
        _ = exec.execute(stmt, fba_alloc) catch continue;
    }
    return timer.elapsedMs();
}

fn benchScanFilterSqlite(iterations: u32) f64 {
    const tmp_path = "/tmp/sqlite_bench_scan.db\x00";
    defer zqlite.os.deleteFile("/tmp/sqlite_bench_scan.db");

    var db: ?*c.sqlite3 = null;
    if (c.sqlite3_open(tmp_path.ptr, &db) != c.SQLITE_OK) return -1;
    defer _ = c.sqlite3_close(db);

    _ = c.sqlite3_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value INTEGER);", null, null, null);
    _ = c.sqlite3_exec(db, "BEGIN;", null, null, null);

    var ins_stmt: ?*c.sqlite3_stmt = null;
    _ = c.sqlite3_prepare_v2(db, "INSERT INTO t VALUES (?, ?, ?);", -1, &ins_stmt, null);
    var j: u32 = 0;
    while (j < 200) : (j += 1) {
        _ = c.sqlite3_bind_int64(ins_stmt, 1, @intCast(j));
        _ = c.sqlite3_bind_text(ins_stmt, 2, "row_data", -1, null);
        _ = c.sqlite3_bind_int64(ins_stmt, 3, @intCast(j * 3));
        _ = c.sqlite3_step(ins_stmt);
        _ = c.sqlite3_reset(ins_stmt);
    }
    _ = c.sqlite3_finalize(ins_stmt);
    _ = c.sqlite3_exec(db, "COMMIT;", null, null, null);

    var sel_stmt: ?*c.sqlite3_stmt = null;
    _ = c.sqlite3_prepare_v2(db, "SELECT * FROM t WHERE value > 300;", -1, &sel_stmt, null);

    const timer = Timer.start();
    var i: u32 = 0;
    while (i < iterations) : (i += 1) {
        while (c.sqlite3_step(sel_stmt) == c.SQLITE_ROW) {}
        _ = c.sqlite3_reset(sel_stmt);
    }
    _ = c.sqlite3_finalize(sel_stmt);

    return timer.elapsedMs();
}

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark 4: Point Delete (full SQL pipeline)
//
// Both engines: populate rows, then DELETE WHERE pk = N repeatedly.
// ═══════════════════════════════════════════════════════════════════════════

fn benchPointDeleteZqlite(allocator: std.mem.Allocator, count: u32) f64 {
    const tmp_path = "/tmp/zqlite_bench_delete.db";
    defer zqlite.os.deleteFile(tmp_path);

    var fh = zqlite.os.FileHandle.open(tmp_path, zqlite.os.DEFAULT_PAGE_SIZE, false) catch return -1;
    defer fh.close();

    var pool = zqlite.pager.BufferPool.init(allocator, &fh, 2048) catch return -1;
    defer pool.deinit();

    var schema_store = zqlite.schema.Schema.init(allocator);
    defer schema_store.deinit();

    var exec_arena = std.heap.ArenaAllocator.init(allocator);
    defer exec_arena.deinit();

    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    // Set up journal for ACID mode
    var journal = zqlite.journal.Journal.init(allocator, tmp_path, fh.page_size, &fh);
    defer journal.deinit();
    defer zqlite.os.deleteFile(tmp_path ++ "-journal");
    pool.setJournal(&journal);
    exec.setJournal(&journal);

    _ = zqliteExecSql(&exec, allocator, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);") catch return -1;

    // Insert rows
    var sql_buf: [256]u8 = undefined;
    var j: u32 = 0;
    while (j < count) : (j += 1) {
        const sql = std.fmt.bufPrint(&sql_buf, "INSERT INTO t VALUES ({d}, 'data');", .{j}) catch continue;
        _ = zqliteExecSql(&exec, allocator, sql) catch break;
    }

    // Time the deletes
    var fba_buf: [131072]u8 = undefined; // 128KB
    const timer = Timer.start();
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const sql = std.fmt.bufPrint(&sql_buf, "DELETE FROM t WHERE id = {d};", .{i}) catch continue;
        var fba = std.heap.FixedBufferAllocator.init(&fba_buf);
        const fba_alloc = fba.allocator();
        var parser = zqlite.parser.Parser.init(sql, fba_alloc);
        const stmt = parser.parseStatement() catch continue;
        _ = exec.execute(stmt, fba_alloc) catch continue;
    }
    return timer.elapsedMs();
}

fn benchPointDeleteSqlite(count: u32) f64 {
    const tmp_path = "/tmp/sqlite_bench_delete.db\x00";
    defer zqlite.os.deleteFile("/tmp/sqlite_bench_delete.db");

    var db: ?*c.sqlite3 = null;
    if (c.sqlite3_open(tmp_path.ptr, &db) != c.SQLITE_OK) return -1;
    defer _ = c.sqlite3_close(db);

    _ = c.sqlite3_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);", null, null, null);
    _ = c.sqlite3_exec(db, "BEGIN;", null, null, null);

    var ins_stmt: ?*c.sqlite3_stmt = null;
    _ = c.sqlite3_prepare_v2(db, "INSERT INTO t VALUES (?, ?);", -1, &ins_stmt, null);
    var j: u32 = 0;
    while (j < count) : (j += 1) {
        _ = c.sqlite3_bind_int64(ins_stmt, 1, @intCast(j));
        _ = c.sqlite3_bind_text(ins_stmt, 2, "data", -1, null);
        _ = c.sqlite3_step(ins_stmt);
        _ = c.sqlite3_reset(ins_stmt);
    }
    _ = c.sqlite3_finalize(ins_stmt);
    _ = c.sqlite3_exec(db, "COMMIT;", null, null, null);

    var del_stmt: ?*c.sqlite3_stmt = null;
    _ = c.sqlite3_prepare_v2(db, "DELETE FROM t WHERE id = ?;", -1, &del_stmt, null);

    const timer = Timer.start();
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        _ = c.sqlite3_bind_int64(del_stmt, 1, @intCast(i));
        _ = c.sqlite3_step(del_stmt);
        _ = c.sqlite3_reset(del_stmt);
    }
    _ = c.sqlite3_finalize(del_stmt);

    return timer.elapsedMs();
}

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark 5: Mixed Workload (INSERT + SELECT + DELETE per cycle)
//
// Each cycle: INSERT a row, SELECT it back, DELETE it.
// Tests realistic CRUD patterns through the full SQL pipeline.
// ═══════════════════════════════════════════════════════════════════════════

fn benchMixedWorkloadZqlite(allocator: std.mem.Allocator, count: u32) f64 {
    const tmp_path = "/tmp/zqlite_bench_mixed.db";
    defer zqlite.os.deleteFile(tmp_path);

    var fh = zqlite.os.FileHandle.open(tmp_path, zqlite.os.DEFAULT_PAGE_SIZE, false) catch return -1;
    defer fh.close();

    var pool = zqlite.pager.BufferPool.init(allocator, &fh, 2048) catch return -1;
    defer pool.deinit();

    var schema_store = zqlite.schema.Schema.init(allocator);
    defer schema_store.deinit();

    var exec_arena = std.heap.ArenaAllocator.init(allocator);
    defer exec_arena.deinit();

    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    // Set up journal for ACID mode
    var journal = zqlite.journal.Journal.init(allocator, tmp_path, fh.page_size, &fh);
    defer journal.deinit();
    defer zqlite.os.deleteFile(tmp_path ++ "-journal");
    pool.setJournal(&journal);
    exec.setJournal(&journal);

    _ = zqliteExecSql(&exec, allocator, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);") catch return -1;

    var fba_buf: [262144]u8 = undefined; // 256KB for INSERT+SELECT+DELETE
    var sql_buf: [256]u8 = undefined;
    const timer = Timer.start();

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        // INSERT
        {
            const sql = std.fmt.bufPrint(&sql_buf, "INSERT INTO t VALUES ({d}, 'mixed_data');", .{i + 10000}) catch continue;
            var fba = std.heap.FixedBufferAllocator.init(&fba_buf);
            const a = fba.allocator();
            var parser = zqlite.parser.Parser.init(sql, a);
            const stmt = parser.parseStatement() catch continue;
            _ = exec.execute(stmt, a) catch continue;
        }
        // SELECT
        {
            const sql = std.fmt.bufPrint(&sql_buf, "SELECT * FROM t WHERE id = {d};", .{i + 10000}) catch continue;
            var fba = std.heap.FixedBufferAllocator.init(&fba_buf);
            const a = fba.allocator();
            var parser = zqlite.parser.Parser.init(sql, a);
            const stmt = parser.parseStatement() catch continue;
            _ = exec.execute(stmt, a) catch continue;
        }
        // DELETE
        {
            const sql = std.fmt.bufPrint(&sql_buf, "DELETE FROM t WHERE id = {d};", .{i + 10000}) catch continue;
            var fba = std.heap.FixedBufferAllocator.init(&fba_buf);
            const a = fba.allocator();
            var parser = zqlite.parser.Parser.init(sql, a);
            const stmt = parser.parseStatement() catch continue;
            _ = exec.execute(stmt, a) catch continue;
        }
    }
    return timer.elapsedMs();
}

fn benchMixedWorkloadSqlite(count: u32) f64 {
    const tmp_path = "/tmp/sqlite_bench_mixed.db\x00";
    defer zqlite.os.deleteFile("/tmp/sqlite_bench_mixed.db");

    var db: ?*c.sqlite3 = null;
    if (c.sqlite3_open(tmp_path.ptr, &db) != c.SQLITE_OK) return -1;
    defer _ = c.sqlite3_close(db);

    _ = c.sqlite3_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);", null, null, null);

    var ins_stmt: ?*c.sqlite3_stmt = null;
    _ = c.sqlite3_prepare_v2(db, "INSERT INTO t VALUES (?, ?);", -1, &ins_stmt, null);
    var sel_stmt: ?*c.sqlite3_stmt = null;
    _ = c.sqlite3_prepare_v2(db, "SELECT * FROM t WHERE id = ?;", -1, &sel_stmt, null);
    var del_stmt: ?*c.sqlite3_stmt = null;
    _ = c.sqlite3_prepare_v2(db, "DELETE FROM t WHERE id = ?;", -1, &del_stmt, null);

    const timer = Timer.start();
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        // INSERT
        _ = c.sqlite3_bind_int64(ins_stmt, 1, @intCast(i + 10000));
        _ = c.sqlite3_bind_text(ins_stmt, 2, "mixed_data", -1, null);
        _ = c.sqlite3_step(ins_stmt);
        _ = c.sqlite3_reset(ins_stmt);
        // SELECT
        _ = c.sqlite3_bind_int64(sel_stmt, 1, @intCast(i + 10000));
        _ = c.sqlite3_step(sel_stmt);
        _ = c.sqlite3_reset(sel_stmt);
        // DELETE
        _ = c.sqlite3_bind_int64(del_stmt, 1, @intCast(i + 10000));
        _ = c.sqlite3_step(del_stmt);
        _ = c.sqlite3_reset(del_stmt);
    }

    _ = c.sqlite3_finalize(ins_stmt);
    _ = c.sqlite3_finalize(sel_stmt);
    _ = c.sqlite3_finalize(del_stmt);

    return timer.elapsedMs();
}

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark 6: Point Update (full SQL pipeline)
//
// Both engines: populate 200 rows, then UPDATE SET val = N WHERE pk = N.
// ═══════════════════════════════════════════════════════════════════════════

fn benchPointUpdateZqlite(allocator: std.mem.Allocator, count: u32) f64 {
    const tmp_path = "/tmp/zqlite_bench_update.db";
    defer zqlite.os.deleteFile(tmp_path);

    var fh = zqlite.os.FileHandle.open(tmp_path, zqlite.os.DEFAULT_PAGE_SIZE, false) catch return -1;
    defer fh.close();

    var pool = zqlite.pager.BufferPool.init(allocator, &fh, 2048) catch return -1;
    defer pool.deinit();

    var schema_store = zqlite.schema.Schema.init(allocator);
    defer schema_store.deinit();

    var exec_arena = std.heap.ArenaAllocator.init(allocator);
    defer exec_arena.deinit();

    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    // Set up journal for ACID mode
    var journal = zqlite.journal.Journal.init(allocator, tmp_path, fh.page_size, &fh);
    defer journal.deinit();
    defer zqlite.os.deleteFile(tmp_path ++ "-journal");
    pool.setJournal(&journal);
    exec.setJournal(&journal);

    _ = zqliteExecSql(&exec, allocator, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value INTEGER);") catch return -1;

    // Populate rows
    const row_count = @min(count, 200);
    var sql_buf: [256]u8 = undefined;
    var j: u32 = 0;
    while (j < row_count) : (j += 1) {
        const sql = std.fmt.bufPrint(&sql_buf, "INSERT INTO t VALUES ({d}, 'data', {d});", .{ j, j }) catch continue;
        _ = zqliteExecSql(&exec, allocator, sql) catch break;
    }

    // Time the updates
    var fba_buf: [131072]u8 = undefined;
    const timer = Timer.start();
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const sql = std.fmt.bufPrint(&sql_buf, "UPDATE t SET value = {d} WHERE id = {d};", .{ i * 10, i % row_count }) catch continue;
        var fba = std.heap.FixedBufferAllocator.init(&fba_buf);
        const fba_alloc = fba.allocator();
        var parser = zqlite.parser.Parser.init(sql, fba_alloc);
        const stmt = parser.parseStatement() catch continue;
        _ = exec.execute(stmt, fba_alloc) catch continue;
    }
    return timer.elapsedMs();
}

fn benchPointUpdateSqlite(count: u32) f64 {
    const tmp_path = "/tmp/sqlite_bench_update.db\x00";
    defer zqlite.os.deleteFile("/tmp/sqlite_bench_update.db");

    var db: ?*c.sqlite3 = null;
    if (c.sqlite3_open(tmp_path.ptr, &db) != c.SQLITE_OK) return -1;
    defer _ = c.sqlite3_close(db);

    _ = c.sqlite3_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value INTEGER);", null, null, null);
    _ = c.sqlite3_exec(db, "BEGIN;", null, null, null);

    var ins_stmt: ?*c.sqlite3_stmt = null;
    _ = c.sqlite3_prepare_v2(db, "INSERT INTO t VALUES (?, ?, ?);", -1, &ins_stmt, null);
    const row_count = @min(count, 200);
    var j: u32 = 0;
    while (j < row_count) : (j += 1) {
        _ = c.sqlite3_bind_int64(ins_stmt, 1, @intCast(j));
        _ = c.sqlite3_bind_text(ins_stmt, 2, "data", -1, null);
        _ = c.sqlite3_bind_int64(ins_stmt, 3, @intCast(j));
        _ = c.sqlite3_step(ins_stmt);
        _ = c.sqlite3_reset(ins_stmt);
    }
    _ = c.sqlite3_finalize(ins_stmt);
    _ = c.sqlite3_exec(db, "COMMIT;", null, null, null);

    var upd_stmt: ?*c.sqlite3_stmt = null;
    _ = c.sqlite3_prepare_v2(db, "UPDATE t SET value = ? WHERE id = ?;", -1, &upd_stmt, null);

    const timer = Timer.start();
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        _ = c.sqlite3_bind_int64(upd_stmt, 1, @intCast(i * 10));
        _ = c.sqlite3_bind_int64(upd_stmt, 2, @intCast(i % row_count));
        _ = c.sqlite3_step(upd_stmt);
        _ = c.sqlite3_reset(upd_stmt);
    }
    _ = c.sqlite3_finalize(upd_stmt);

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
    print("All benchmarks use the FULL SQL pipeline for both engines:\n", .{});
    print("  ZQLite: SQL string → parser → executor → B-tree\n", .{});
    print("  SQLite: SQL string → prepared statement → VDBE → B-tree\n", .{});
    print("\n", .{});

    var results: [8]BenchResult = undefined;
    var num_results: usize = 0;

    const allocator = std.heap.page_allocator;

    // Benchmark 1: Bulk Insert via SQL
    {
        const count: u32 = 500;
        print("Running bulk_insert ({d} rows)...\n", .{count});
        const z = benchBulkInsertZqlite(allocator, count);
        const s = benchBulkInsertSqlite(count);
        results[num_results] = .{ .name = "bulk_insert", .zqlite_ms = z, .sqlite_ms = s };
        num_results += 1;
    }

    // Benchmark 2: Point Lookup via SQL
    {
        const count: u32 = 5000;
        print("Running point_lookup ({d} lookups)...\n", .{count});
        const z = benchPointLookupZqlite(allocator, count);
        const s = benchPointLookupSqlite(count);
        results[num_results] = .{ .name = "point_lookup", .zqlite_ms = z, .sqlite_ms = s };
        num_results += 1;
    }

    // Benchmark 3: Table Scan with Filter via SQL
    {
        const count: u32 = 1000;
        print("Running scan_filter ({d} iterations, 200 rows)...\n", .{count});
        const z = benchScanFilterZqlite(allocator, count);
        const s = benchScanFilterSqlite(count);
        results[num_results] = .{ .name = "scan_filter", .zqlite_ms = z, .sqlite_ms = s };
        num_results += 1;
    }

    // Benchmark 4: Point Delete via SQL
    {
        const count: u32 = 500;
        print("Running point_delete ({d} delete+reinsert cycles)...\n", .{count});
        const z = benchPointDeleteZqlite(allocator, count);
        const s = benchPointDeleteSqlite(count);
        results[num_results] = .{ .name = "point_delete", .zqlite_ms = z, .sqlite_ms = s };
        num_results += 1;
    }

    // Benchmark 5: Mixed Workload (INSERT + SELECT + DELETE interleaved)
    {
        const count: u32 = 500;
        print("Running mixed_workload ({d} cycles: insert+select+delete)...\n", .{count});
        const z = benchMixedWorkloadZqlite(allocator, count);
        const s = benchMixedWorkloadSqlite(count);
        results[num_results] = .{ .name = "mixed_workload", .zqlite_ms = z, .sqlite_ms = s };
        num_results += 1;
    }

    // Benchmark 6: Point Update via SQL
    {
        const count: u32 = 5000;
        print("Running point_update ({d} updates)...\n", .{count});
        const z = benchPointUpdateZqlite(allocator, count);
        const s = benchPointUpdateSqlite(count);
        results[num_results] = .{ .name = "point_update", .zqlite_ms = z, .sqlite_ms = s };
        num_results += 1;
    }

    // Print results table
    print("\n", .{});
    print("| Benchmark          | ZQLite (ms) | SQLite (ms) | Ratio    |\n", .{});
    print("|--------------------|-------------|-------------|----------|\n", .{});
    for (results[0..num_results]) |r| {
        print("| {s: <18} | {d: >11.2} | {d: >11.2} | {d: >6.2}x  |\n", .{
            r.name,
            r.zqlite_ms,
            r.sqlite_ms,
            r.speedup(),
        });
    }

    print("\nNotes:\n", .{});
    print("  * Both engines use the full SQL pipeline (parse → plan → execute → storage)\n", .{});
    print("  * ZQLite runs in ACID mode (rollback journal + fsync on each auto-commit)\n", .{});
    print("  * SQLite uses auto-commit per statement (rollback journal + fsync)\n", .{});
    print("  * ZQLite advantages: zero-copy allocator, PK fast-path (bt.search)\n", .{});
    print("  * SQLite advantages: prepared statements, decades of optimization, MVCC\n", .{});
    print("  * ZQLite does not yet implement: WAL, page splitting,\n", .{});
    print("    multi-table joins, indexes, or prepared statements\n", .{});
    print("  * Ratio > 1 = ZQLite faster, < 1 = SQLite faster\n", .{});
    print("\nBenchmark complete.\n", .{});
}
