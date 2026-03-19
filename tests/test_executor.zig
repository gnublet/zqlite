const std = @import("std");
const zqlite = @import("zqlite");

// ═══════════════════════════════════════════════════════════════════════════
// Helpers — each test uses standalone vars to avoid dangling pointers
// ═══════════════════════════════════════════════════════════════════════════

fn execSql(
    exec: *zqlite.executor.Executor,
    sql: []const u8,
) !zqlite.executor.ExecResult {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var parser = zqlite.parser.Parser.init(sql, arena.allocator());
    const stmt = try parser.parseStatement();
    return exec.execute(stmt, arena.allocator());
}

const QueryResult = struct {
    arena: std.heap.ArenaAllocator,
    result: zqlite.executor.ExecResult,
    fn deinit(self: *QueryResult) void {
        self.arena.deinit();
    }
};

fn querySql(
    exec: *zqlite.executor.Executor,
    sql: []const u8,
) !QueryResult {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    errdefer arena.deinit();
    var parser = zqlite.parser.Parser.init(sql, arena.allocator());
    const stmt = try parser.parseStatement();
    const result = try exec.execute(stmt, arena.allocator());
    return .{ .arena = arena, .result = result };
}

// ═══════════════════════════════════════════════════════════════════════════
// CREATE TABLE Tests
// ═══════════════════════════════════════════════════════════════════════════

test "executor: CREATE TABLE basic" {
    const path = "/tmp/zqlite_test_exec_create.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    const r = try execSql(&exec, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);");
    try std.testing.expect(r.rows_affected == 0);

    // Creating same table should fail
    const r2 = execSql(&exec, "CREATE TABLE users (id INTEGER PRIMARY KEY);");
    try std.testing.expectError(error.TableAlreadyExists, r2);
}

test "executor: CREATE TABLE IF NOT EXISTS" {
    const path = "/tmp/zqlite_test_exec_cine.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY);");
    _ = try execSql(&exec, "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY);");
}

// ═══════════════════════════════════════════════════════════════════════════
// INSERT Tests
// ═══════════════════════════════════════════════════════════════════════════

test "executor: INSERT single row" {
    const path = "/tmp/zqlite_test_exec_ins1.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);");
    const r = try execSql(&exec, "INSERT INTO t VALUES (1, 'Alice');");
    try std.testing.expectEqual(@as(usize, 1), r.rows_affected);
}

test "executor: INSERT multiple rows and count" {
    const path = "/tmp/zqlite_test_exec_ins2.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (1, 10);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (2, 20);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (3, 30);");

    var qr = try querySql(&exec, "SELECT * FROM t;");
    defer qr.deinit();
    try std.testing.expectEqual(@as(usize, 3), qr.result.rows.len);
}

test "executor: INSERT wrong column count errors" {
    const path = "/tmp/zqlite_test_exec_ins3.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);");
    const r = execSql(&exec, "INSERT INTO t VALUES (1, 'Alice');");
    try std.testing.expectError(error.ColumnCountMismatch, r);
}

test "executor: INSERT duplicate PK errors" {
    const path = "/tmp/zqlite_test_exec_ins4.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (1, 'Alice');");
    const r = execSql(&exec, "INSERT INTO t VALUES (1, 'Bob');");
    try std.testing.expectError(error.StorageError, r);
}

// ═══════════════════════════════════════════════════════════════════════════
// SELECT Tests
// ═══════════════════════════════════════════════════════════════════════════

test "executor: SELECT all rows" {
    const path = "/tmp/zqlite_test_exec_sel1.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (1, 'Alice');");
    _ = try execSql(&exec, "INSERT INTO t VALUES (2, 'Bob');");

    var qr = try querySql(&exec, "SELECT * FROM t;");
    defer qr.deinit();
    try std.testing.expectEqual(@as(usize, 2), qr.result.rows.len);
    try std.testing.expectEqual(@as(usize, 2), qr.result.column_names.len);
}

test "executor: SELECT specific columns" {
    const path = "/tmp/zqlite_test_exec_sel2.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (1, 'Alice', 30);");

    var qr = try querySql(&exec, "SELECT name FROM t;");
    defer qr.deinit();
    try std.testing.expectEqual(@as(usize, 1), qr.result.column_names.len);
}

test "executor: SELECT with PK WHERE (fast path)" {
    const path = "/tmp/zqlite_test_exec_sel3.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (1, 'Alice');");
    _ = try execSql(&exec, "INSERT INTO t VALUES (2, 'Bob');");
    _ = try execSql(&exec, "INSERT INTO t VALUES (3, 'Charlie');");

    var qr = try querySql(&exec, "SELECT * FROM t WHERE id = 2;");
    defer qr.deinit();
    try std.testing.expectEqual(@as(usize, 1), qr.result.rows.len);
}

test "executor: SELECT with non-PK filter" {
    const path = "/tmp/zqlite_test_exec_sel4.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (1, 'Alice', 30);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (2, 'Bob', 25);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (3, 'Charlie', 35);");

    var qr = try querySql(&exec, "SELECT * FROM t WHERE age > 28;");
    defer qr.deinit();
    try std.testing.expectEqual(@as(usize, 2), qr.result.rows.len);
}

test "executor: SELECT from empty table" {
    const path = "/tmp/zqlite_test_exec_sel5.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);");
    var qr = try querySql(&exec, "SELECT * FROM t;");
    defer qr.deinit();
    try std.testing.expectEqual(@as(usize, 0), qr.result.rows.len);
}

test "executor: SELECT nonexistent PK returns empty" {
    const path = "/tmp/zqlite_test_exec_sel6.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (1, 'Alice');");

    var qr = try querySql(&exec, "SELECT * FROM t WHERE id = 999;");
    defer qr.deinit();
    try std.testing.expectEqual(@as(usize, 0), qr.result.rows.len);
}

// ═══════════════════════════════════════════════════════════════════════════
// UPDATE Tests
// ═══════════════════════════════════════════════════════════════════════════

test "executor: UPDATE single row by PK" {
    const path = "/tmp/zqlite_test_exec_upd1.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (1, 'Alice', 30);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (2, 'Bob', 25);");

    const r = try execSql(&exec, "UPDATE t SET name = 'Bobby' WHERE id = 2;");
    try std.testing.expectEqual(@as(usize, 1), r.rows_affected);

    var qr = try querySql(&exec, "SELECT * FROM t WHERE id = 2;");
    defer qr.deinit();
    try std.testing.expectEqual(@as(usize, 1), qr.result.rows.len);
}

test "executor: UPDATE multiple rows with filter" {
    const path = "/tmp/zqlite_test_exec_upd2.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (1, 'Alice', 30);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (2, 'Bob', 25);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (3, 'Charlie', 35);");

    const r = try execSql(&exec, "UPDATE t SET age = 99 WHERE age > 28;");
    try std.testing.expectEqual(@as(usize, 2), r.rows_affected);
}

test "executor: UPDATE all rows (no WHERE)" {
    const path = "/tmp/zqlite_test_exec_upd3.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (1, 10);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (2, 20);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (3, 30);");

    const r = try execSql(&exec, "UPDATE t SET val = 0;");
    try std.testing.expectEqual(@as(usize, 3), r.rows_affected);
}

test "executor: UPDATE nonexistent row" {
    const path = "/tmp/zqlite_test_exec_upd4.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (1, 'Alice');");

    const r = try execSql(&exec, "UPDATE t SET name = 'Ghost' WHERE id = 999;");
    try std.testing.expectEqual(@as(usize, 0), r.rows_affected);
}

test "executor: UPDATE multiple columns" {
    const path = "/tmp/zqlite_test_exec_upd5.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (1, 'Alice', 30);");

    const r = try execSql(&exec, "UPDATE t SET name = 'Alicia', age = 31 WHERE id = 1;");
    try std.testing.expectEqual(@as(usize, 1), r.rows_affected);
}

// ═══════════════════════════════════════════════════════════════════════════
// DELETE Tests
// ═══════════════════════════════════════════════════════════════════════════

test "executor: DELETE by PK" {
    const path = "/tmp/zqlite_test_exec_del1.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (1, 'Alice');");
    _ = try execSql(&exec, "INSERT INTO t VALUES (2, 'Bob');");

    const r = try execSql(&exec, "DELETE FROM t WHERE id = 1;");
    try std.testing.expectEqual(@as(usize, 1), r.rows_affected);

    var qr = try querySql(&exec, "SELECT * FROM t;");
    defer qr.deinit();
    try std.testing.expectEqual(@as(usize, 1), qr.result.rows.len);
}

test "executor: DELETE with filter" {
    const path = "/tmp/zqlite_test_exec_del2.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (1, 10);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (2, 20);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (3, 30);");

    const r = try execSql(&exec, "DELETE FROM t WHERE val > 15;");
    try std.testing.expectEqual(@as(usize, 2), r.rows_affected);
}

test "executor: DELETE all rows" {
    const path = "/tmp/zqlite_test_exec_del3.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (1, 'Alice');");
    _ = try execSql(&exec, "INSERT INTO t VALUES (2, 'Bob');");

    const r = try execSql(&exec, "DELETE FROM t;");
    try std.testing.expectEqual(@as(usize, 2), r.rows_affected);

    var qr = try querySql(&exec, "SELECT * FROM t;");
    defer qr.deinit();
    try std.testing.expectEqual(@as(usize, 0), qr.result.rows.len);
}

test "executor: DELETE nonexistent row" {
    const path = "/tmp/zqlite_test_exec_del4.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);");
    _ = try execSql(&exec, "INSERT INTO t VALUES (1, 'Alice');");

    const r = try execSql(&exec, "DELETE FROM t WHERE id = 999;");
    try std.testing.expectEqual(@as(usize, 0), r.rows_affected);
}

// ═══════════════════════════════════════════════════════════════════════════
// DROP TABLE Tests
// ═══════════════════════════════════════════════════════════════════════════

test "executor: DROP TABLE" {
    const path = "/tmp/zqlite_test_exec_drop1.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    _ = try execSql(&exec, "CREATE TABLE t (id INTEGER PRIMARY KEY);");
    _ = try execSql(&exec, "DROP TABLE t;");

    const r = querySql(&exec, "SELECT * FROM t;");
    try std.testing.expectError(error.TableNotFound, r);
}

// ═══════════════════════════════════════════════════════════════════════════
// Full CRUD Lifecycle
// ═══════════════════════════════════════════════════════════════════════════

test "executor: full CRUD lifecycle" {
    const path = "/tmp/zqlite_test_exec_crud.db";
    defer zqlite.os.deleteFile(path);

    var fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
    defer fh.close();
    var pool = try zqlite.pager.BufferPool.init(std.testing.allocator, &fh, 64);
    defer pool.deinit();
    var schema_store = zqlite.schema.Schema.init(std.testing.allocator);
    defer schema_store.deinit();
    var exec_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer exec_arena.deinit();
    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    // Create
    _ = try execSql(&exec, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER);");

    // Insert
    _ = try execSql(&exec, "INSERT INTO items VALUES (1, 'Widget', 100);");
    _ = try execSql(&exec, "INSERT INTO items VALUES (2, 'Gadget', 50);");
    _ = try execSql(&exec, "INSERT INTO items VALUES (3, 'Doohickey', 25);");

    // Read all
    {
        var qr = try querySql(&exec, "SELECT * FROM items;");
        defer qr.deinit();
        try std.testing.expectEqual(@as(usize, 3), qr.result.rows.len);
    }

    // Update one
    const upd = try execSql(&exec, "UPDATE items SET qty = 200 WHERE id = 1;");
    try std.testing.expectEqual(@as(usize, 1), upd.rows_affected);

    // Verify update via PK lookup
    {
        var qr = try querySql(&exec, "SELECT * FROM items WHERE id = 1;");
        defer qr.deinit();
        try std.testing.expectEqual(@as(usize, 1), qr.result.rows.len);
    }

    // Delete one
    const del = try execSql(&exec, "DELETE FROM items WHERE id = 2;");
    try std.testing.expectEqual(@as(usize, 1), del.rows_affected);

    // Verify 2 remaining
    {
        var qr = try querySql(&exec, "SELECT * FROM items;");
        defer qr.deinit();
        try std.testing.expectEqual(@as(usize, 2), qr.result.rows.len);
    }

    // Drop
    _ = try execSql(&exec, "DROP TABLE items;");
    const r = querySql(&exec, "SELECT * FROM items;");
    try std.testing.expectError(error.TableNotFound, r);
}
