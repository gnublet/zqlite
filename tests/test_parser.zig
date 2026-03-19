const std = @import("std");
const zqlite = @import("zqlite");

test "parser: SELECT with multiple columns and aliases" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = zqlite.parser.Parser.init("SELECT id, name AS n, age FROM users WHERE age > 18 ORDER BY name;", arena.allocator());
    const stmt = try p.parseStatement();
    try std.testing.expect(stmt == .select);
    try std.testing.expectEqual(@as(usize, 3), stmt.select.columns.len);
    try std.testing.expect(stmt.select.where != null);
    try std.testing.expectEqual(@as(usize, 1), stmt.select.order_by.len);
}

test "parser: INSERT with multiple rows" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = zqlite.parser.Parser.init("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');", arena.allocator());
    const stmt = try p.parseStatement();
    try std.testing.expect(stmt == .insert);
    try std.testing.expectEqual(@as(usize, 2), stmt.insert.values.len);
}

test "parser: CREATE TABLE IF NOT EXISTS" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = zqlite.parser.Parser.init("CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY, msg TEXT);", arena.allocator());
    const stmt = try p.parseStatement();
    try std.testing.expect(stmt == .create_table);
    try std.testing.expect(stmt.create_table.if_not_exists);
    try std.testing.expectEqual(@as(usize, 2), stmt.create_table.columns.len);
}

test "parser: CREATE UNIQUE INDEX" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = zqlite.parser.Parser.init("CREATE UNIQUE INDEX idx_email ON users (email);", arena.allocator());
    const stmt = try p.parseStatement();
    try std.testing.expect(stmt == .create_index);
    try std.testing.expect(stmt.create_index.unique);
    try std.testing.expectEqualStrings("idx_email", stmt.create_index.name);
}

test "parser: SELECT with JOIN" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = zqlite.parser.Parser.init("SELECT u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id;", arena.allocator());
    const stmt = try p.parseStatement();
    try std.testing.expect(stmt == .select);
    try std.testing.expectEqual(@as(usize, 1), stmt.select.joins.len);
    try std.testing.expectEqual(zqlite.ast.JoinType.inner, stmt.select.joins[0].join_type);
}

test "parser: UPDATE multiple columns" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = zqlite.parser.Parser.init("UPDATE users SET name = 'Charlie', age = 25 WHERE id = 3;", arena.allocator());
    const stmt = try p.parseStatement();
    try std.testing.expect(stmt == .update);
    try std.testing.expectEqual(@as(usize, 2), stmt.update.assignments.len);
}

test "parser: SELECT with GROUP BY and HAVING" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = zqlite.parser.Parser.init("SELECT dept, COUNT(id) FROM employees GROUP BY dept HAVING COUNT(id) > 5;", arena.allocator());
    const stmt = try p.parseStatement();
    try std.testing.expect(stmt == .select);
    try std.testing.expectEqual(@as(usize, 1), stmt.select.group_by.len);
    try std.testing.expect(stmt.select.having != null);
}

test "parser: DROP TABLE IF EXISTS" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = zqlite.parser.Parser.init("DROP TABLE IF EXISTS temp;", arena.allocator());
    const stmt = try p.parseStatement();
    try std.testing.expect(stmt == .drop_table);
    try std.testing.expect(stmt.drop_table.if_exists);
}

test "parser: BEGIN IMMEDIATE TRANSACTION" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = zqlite.parser.Parser.init("BEGIN IMMEDIATE TRANSACTION;", arena.allocator());
    const stmt = try p.parseStatement();
    try std.testing.expect(stmt == .begin);
    try std.testing.expectEqual(zqlite.ast.Statement.TransactionMode.immediate, stmt.begin);
}
