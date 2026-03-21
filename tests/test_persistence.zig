const std = @import("std");
const zqlite = @import("zqlite");

const TestDb = struct {
    allocator: std.mem.Allocator,
    path: []const u8,
    fh: zqlite.os.FileHandle,
    pool: zqlite.pager.BufferPool,
    journal: zqlite.journal.Journal,
    exec_arena: std.heap.ArenaAllocator,
    schema_store: zqlite.schema.Schema,
    exec: zqlite.executor.Executor,

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !*TestDb {
        var db = try allocator.create(TestDb);
        db.allocator = allocator;
        db.path = path;
        db.fh = try zqlite.os.FileHandle.open(path, zqlite.os.DEFAULT_PAGE_SIZE, false);
        db.pool = try zqlite.pager.BufferPool.init(allocator, &db.fh, 64);
        db.journal = zqlite.journal.Journal.init(allocator, path, db.fh.page_size, &db.fh);
        db.pool.setJournal(&db.journal);
        db.exec_arena = std.heap.ArenaAllocator.init(allocator);
        db.schema_store = zqlite.schema.Schema.init(db.exec_arena.allocator());
        db.exec = zqlite.executor.Executor.init(db.exec_arena.allocator(), &db.pool, &db.schema_store);
        db.exec.setFile(&db.fh);
        db.exec.setJournal(&db.journal);
        db.exec.loadSchemaFromDisk();
        return db;
    }

    pub fn deinit(self: *TestDb) void {
        self.exec.deinit();
        self.schema_store.deinit();
        self.exec_arena.deinit();
        self.journal.deinit();
        self.pool.deinit();
        self.fh.close();
        self.allocator.destroy(self);
    }
    
    pub fn execSql(self: *TestDb, sql: []const u8) !void {
        var parse_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer parse_arena.deinit();
        var parser = zqlite.parser.Parser.init(sql, parse_arena.allocator());
        const stmt = parser.parseStatement() catch |e| {
            std.debug.print("Parse failed for: {s}\n", .{sql});
            return e;
        };
        _ = self.exec.execute(stmt, parse_arena.allocator()) catch |e| {
            std.debug.print("Exec failed for: {s}\n", .{sql});
            return e;
        };
    }

    pub fn execQuery(self: *TestDb, sql: []const u8) !usize {
        var parse_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer parse_arena.deinit();
        var parser = zqlite.parser.Parser.init(sql, parse_arena.allocator());
        const stmt = try parser.parseStatement();
        const result = try self.exec.execute(stmt, parse_arena.allocator());
        return result.rows.len;
    }
};

test "persistence: create, insert, reopen, select with joins, update, delete" {
    const tmp_path = "/tmp/zqlite_persistence_e2e.db";
    _ = zqlite.os.deleteFile(tmp_path);
    defer _ = zqlite.os.deleteFile(tmp_path);
    defer _ = zqlite.os.deleteFile(tmp_path ++ "-journal");

    // Phase 1: Create Database, Tables, and Insert Data
    {
        var db = try TestDb.init(std.testing.allocator, tmp_path);
        defer db.deinit();

        try db.execSql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);");
        try db.execSql("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER);");

        try db.execSql("INSERT INTO users VALUES (1, 'Alice');");
        try db.execSql("INSERT INTO users VALUES (2, 'Bob');");
        try db.execSql("INSERT INTO users VALUES (3, 'Charlie');");

        try db.execSql("INSERT INTO orders VALUES (101, 1, 50);");
        try db.execSql("INSERT INTO orders VALUES (102, 1, 150);");
        try db.execSql("INSERT INTO orders VALUES (103, 2, 200);");
        try db.execSql("INSERT INTO orders VALUES (104, 3, 300);");
    }

    // Phase 2: Reopen Database, Select with Joins
    {
        var db = try TestDb.init(std.testing.allocator, tmp_path);
        defer db.deinit();

        const rowes = try db.execQuery("SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id;");
        try std.testing.expectEqual(@as(usize, 4), rowes);

        const alice_orders = try db.execQuery("SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.name = 'Alice';");
        try std.testing.expectEqual(@as(usize, 2), alice_orders);
    }

    // Phase 3: Unconnected Update
    {
        var db = try TestDb.init(std.testing.allocator, tmp_path);
        defer db.deinit();

        try db.execSql("UPDATE orders SET amount = 999 WHERE id = 101;");
    }

    // Phase 4: Reopen Database, Verify Update
    {
        var db = try TestDb.init(std.testing.allocator, tmp_path);
        defer db.deinit();

        var parse_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer parse_arena.deinit();
        
        var parser = zqlite.parser.Parser.init("SELECT amount FROM orders WHERE id = 101;", parse_arena.allocator());
        const stmt = try parser.parseStatement();
        var prepared = try db.exec.prepare(stmt, null);
        defer prepared.deinit();
        
        const row = (try prepared.step()).?;
        try std.testing.expectEqual(@as(i64, 999), row[0].integer);
    }

    // Phase 5: Reopen -> Delete
    {
        var db = try TestDb.init(std.testing.allocator, tmp_path);
        defer db.deinit();

        try db.execSql("DELETE FROM orders WHERE id = 101;");
    }

    // Phase 6: Verify Deletion
    {
        var db = try TestDb.init(std.testing.allocator, tmp_path);
        defer db.deinit();

        const rowes = try db.execQuery("SELECT * FROM orders WHERE id = 101;");
        try std.testing.expectEqual(@as(usize, 0), rowes);

        // Verify remaining
        const total_remaining = try db.execQuery("SELECT * FROM orders;");
        try std.testing.expectEqual(@as(usize, 3), total_remaining);
    }
}
