const std = @import("std");
const tokenizer = @import("tokenizer.zig");
const ast = @import("ast.zig");

/// Recursive-descent SQL parser for ZQLite.
///
/// Parses tokenized SQL into typed AST nodes, all arena-allocated.

// ═══════════════════════════════════════════════════════════════════════════
// Errors
// ═══════════════════════════════════════════════════════════════════════════

pub const ParseError = error{
    UnexpectedToken,
    ExpectedExpression,
    ExpectedIdentifier,
    ExpectedLeftParen,
    ExpectedRightParen,
    ExpectedComma,
    ExpectedSemicolon,
    ExpectedKeyword,
    TooManyColumns,
    AllocationFailed,
};

// ═══════════════════════════════════════════════════════════════════════════
// Parser
// ═══════════════════════════════════════════════════════════════════════════

pub const Parser = struct {
    tok: tokenizer.Tokenizer,
    current: tokenizer.Token,
    arena: std.mem.Allocator,

    const Self = @This();

    pub fn init(source: []const u8, arena: std.mem.Allocator) Self {
        var t = tokenizer.Tokenizer.init(source);
        const first = t.next();
        return Self{
            .tok = t,
            .current = first,
            .arena = arena,
        };
    }

    /// Parse a single statement.
    pub fn parseStatement(self: *Self) ParseError!ast.Statement {
        return switch (self.current.type) {
            .kw_select => self.parseSelect(),
            .kw_insert => self.parseInsert(),
            .kw_update => self.parseUpdate(),
            .kw_delete => self.parseDelete(),
            .kw_create => self.parseCreate(),
            .kw_drop => self.parseDrop(),
            .kw_begin => self.parseBegin(),
            .kw_commit => {
                self.advance();
                self.skipSemicolon();
                return .{ .commit = {} };
            },
            .kw_rollback => {
                self.advance();
                self.skipSemicolon();
                return .{ .rollback = {} };
            },
            .kw_explain => {
                self.advance();
                const inner = try self.parseStatement();
                const ptr = self.arena.create(ast.Statement) catch return ParseError.AllocationFailed;
                ptr.* = inner;
                return .{ .explain = ptr };
            },
            else => ParseError.UnexpectedToken,
        };
    }

    // ─── SELECT ─────────────────────────────────────────────────────

    fn parseSelect(self: *Self) ParseError!ast.Statement {
        try self.expect(.kw_select);

        // Columns
        var cols: std.ArrayList(ast.Statement.SelectColumn) = .{};

        if (self.current.type == .star) {
            self.advance();
            cols.append(self.arena, .{ .all_columns = {} }) catch return ParseError.AllocationFailed;
        } else {
            while (true) {
                const expr = try self.parseExpression();
                var alias: ?[]const u8 = null;
                if (self.current.type == .kw_as) {
                    self.advance();
                    alias = try self.expectIdentifier();
                } else if (self.current.type == .identifier) {
                    alias = self.current.lexeme;
                    self.advance();
                }
                cols.append(self.arena, .{ .expr = .{ .expr = expr, .alias = alias } }) catch return ParseError.AllocationFailed;

                if (self.current.type != .comma) break;
                self.advance();
            }
        }

        // FROM
        var from: ?ast.TableRef = null;
        var joins: std.ArrayList(ast.JoinClause) = .{};

        if (self.current.type == .kw_from) {
            self.advance();
            from = try self.parseTableRef();

            // JOINs
            while (self.current.type == .kw_join or
                self.current.type == .kw_inner or
                self.current.type == .kw_left or
                self.current.type == .kw_cross)
            {
                var join_type: ast.JoinType = .inner;
                if (self.current.type == .kw_left) {
                    join_type = .left;
                    self.advance();
                } else if (self.current.type == .kw_cross) {
                    join_type = .cross;
                    self.advance();
                } else if (self.current.type == .kw_inner) {
                    self.advance();
                }
                try self.expect(.kw_join);
                const join_table = try self.parseTableRef();
                var on_expr: ?*ast.Expr = null;
                if (self.current.type == .kw_on) {
                    self.advance();
                    on_expr = try self.parseExpression();
                }
                joins.append(self.arena, .{
                    .join_type = join_type,
                    .table = join_table,
                    .on = on_expr,
                }) catch return ParseError.AllocationFailed;
            }
        }

        // WHERE
        var where: ?*ast.Expr = null;
        if (self.current.type == .kw_where) {
            self.advance();
            where = try self.parseExpression();
        }

        // GROUP BY
        var group_by: std.ArrayList(*ast.Expr) = .{};
        if (self.current.type == .kw_group) {
            self.advance();
            try self.expect(.kw_by);
            while (true) {
                const gb_expr = try self.parseExpression();
                group_by.append(self.arena, gb_expr) catch return ParseError.AllocationFailed;
                if (self.current.type != .comma) break;
                self.advance();
            }
        }

        // HAVING
        var having: ?*ast.Expr = null;
        if (self.current.type == .kw_having) {
            self.advance();
            having = try self.parseExpression();
        }

        // ORDER BY
        var order_by: std.ArrayList(ast.OrderByClause) = .{};
        if (self.current.type == .kw_order) {
            self.advance();
            try self.expect(.kw_by);
            while (true) {
                const ob_expr = try self.parseExpression();
                var order: ast.SortOrder = .asc;
                if (self.current.type == .kw_desc) {
                    order = .desc;
                    self.advance();
                } else if (self.current.type == .kw_asc) {
                    self.advance();
                }
                order_by.append(self.arena, .{ .expr = ob_expr, .order = order }) catch return ParseError.AllocationFailed;
                if (self.current.type != .comma) break;
                self.advance();
            }
        }

        // LIMIT
        var limit: ?*ast.Expr = null;
        if (self.current.type == .kw_limit) {
            self.advance();
            limit = try self.parseExpression();
        }

        self.skipSemicolon();

        return .{ .select = .{
            .columns = cols.items,
            .from = from,
            .joins = joins.items,
            .where = where,
            .group_by = group_by.items,
            .having = having,
            .order_by = order_by.items,
            .limit = limit,
        } };
    }

    // ─── INSERT ─────────────────────────────────────────────────────

    fn parseInsert(self: *Self) ParseError!ast.Statement {
        try self.expect(.kw_insert);
        try self.expect(.kw_into);
        const table = try self.expectIdentifier();

        // Optional column list
        var columns: ?[]const []const u8 = null;
        if (self.current.type == .lparen) {
            self.advance();
            var col_list: std.ArrayList([]const u8) = .{};
            while (true) {
                const col = try self.expectIdentifier();
                col_list.append(self.arena, col) catch return ParseError.AllocationFailed;
                if (self.current.type != .comma) break;
                self.advance();
            }
            try self.expect(.rparen);
            columns = col_list.items;
        }

        try self.expect(.kw_values);

        // Values rows
        var rows: std.ArrayList([]const *ast.Expr) = .{};
        while (true) {
            try self.expect(.lparen);
            var vals: std.ArrayList(*ast.Expr) = .{};
            while (true) {
                const expr = try self.parseExpression();
                vals.append(self.arena, expr) catch return ParseError.AllocationFailed;
                if (self.current.type != .comma) break;
                self.advance();
            }
            try self.expect(.rparen);
            rows.append(self.arena, vals.items) catch return ParseError.AllocationFailed;
            if (self.current.type != .comma) break;
            self.advance();
        }

        self.skipSemicolon();

        return .{ .insert = .{
            .table = table,
            .columns = columns,
            .values = rows.items,
        } };
    }

    // ─── UPDATE ─────────────────────────────────────────────────────

    fn parseUpdate(self: *Self) ParseError!ast.Statement {
        try self.expect(.kw_update);
        const table = try self.expectIdentifier();
        try self.expect(.kw_set);

        var assignments: std.ArrayList(ast.Statement.Assignment) = .{};
        while (true) {
            const col = try self.expectIdentifier();
            try self.expect(.eq);
            const val = try self.parseExpression();
            assignments.append(self.arena, .{ .column = col, .value = val }) catch return ParseError.AllocationFailed;
            if (self.current.type != .comma) break;
            self.advance();
        }

        var where: ?*ast.Expr = null;
        if (self.current.type == .kw_where) {
            self.advance();
            where = try self.parseExpression();
        }

        self.skipSemicolon();

        return .{ .update = .{
            .table = table,
            .assignments = assignments.items,
            .where = where,
        } };
    }

    // ─── DELETE ─────────────────────────────────────────────────────

    fn parseDelete(self: *Self) ParseError!ast.Statement {
        try self.expect(.kw_delete);
        try self.expect(.kw_from);
        const table = try self.expectIdentifier();

        var where: ?*ast.Expr = null;
        if (self.current.type == .kw_where) {
            self.advance();
            where = try self.parseExpression();
        }

        self.skipSemicolon();

        return .{ .delete = .{
            .table = table,
            .where = where,
        } };
    }

    // ─── CREATE ─────────────────────────────────────────────────────

    fn parseCreate(self: *Self) ParseError!ast.Statement {
        try self.expect(.kw_create);

        if (self.current.type == .kw_unique) {
            self.advance();
            try self.expect(.kw_index);
            return self.parseCreateIndex(true);
        }

        if (self.current.type == .kw_index) {
            self.advance();
            return self.parseCreateIndex(false);
        }

        try self.expect(.kw_table);

        var if_not_exists = false;
        if (self.current.type == .kw_if) {
            self.advance();
            try self.expect(.kw_not);
            try self.expect(.kw_exists);
            if_not_exists = true;
        }

        const name = try self.expectIdentifier();
        try self.expect(.lparen);

        var columns: std.ArrayList(ast.Statement.ColumnDef) = .{};
        while (true) {
            const col_name = try self.expectIdentifier();
            var type_name: ?[]const u8 = null;
            var not_null = false;
            var primary_key = false;

            // Type name (optional)
            if (self.current.type == .kw_integer or self.current.type == .kw_text or
                self.current.type == .kw_real or self.current.type == .kw_blob or
                self.current.type == .identifier)
            {
                type_name = self.current.lexeme;
                self.advance();
            }

            // Constraints
            while (self.current.type == .kw_primary or self.current.type == .kw_not or
                self.current.type == .kw_unique or self.current.type == .kw_default)
            {
                if (self.current.type == .kw_primary) {
                    self.advance();
                    try self.expect(.kw_key);
                    primary_key = true;
                } else if (self.current.type == .kw_not) {
                    self.advance();
                    try self.expect(.kw_null);
                    not_null = true;
                } else {
                    break;
                }
            }

            columns.append(self.arena, .{
                .name = col_name,
                .type_name = type_name,
                .not_null = not_null,
                .primary_key = primary_key,
                .default_value = null,
            }) catch return ParseError.AllocationFailed;

            if (self.current.type != .comma) break;
            self.advance();
        }

        try self.expect(.rparen);
        self.skipSemicolon();

        return .{ .create_table = .{
            .name = name,
            .columns = columns.items,
            .if_not_exists = if_not_exists,
        } };
    }

    fn parseCreateIndex(self: *Self, unique: bool) ParseError!ast.Statement {
        var if_not_exists = false;
        if (self.current.type == .kw_if) {
            self.advance();
            try self.expect(.kw_not);
            try self.expect(.kw_exists);
            if_not_exists = true;
        }

        const name = try self.expectIdentifier();
        try self.expect(.kw_on);
        const table = try self.expectIdentifier();
        try self.expect(.lparen);

        var index_cols: std.ArrayList([]const u8) = .{};
        while (true) {
            const col = try self.expectIdentifier();
            index_cols.append(self.arena, col) catch return ParseError.AllocationFailed;
            if (self.current.type != .comma) break;
            self.advance();
        }

        try self.expect(.rparen);
        self.skipSemicolon();

        return .{ .create_index = .{
            .name = name,
            .table = table,
            .columns = index_cols.items,
            .unique = unique,
            .if_not_exists = if_not_exists,
        } };
    }

    // ─── DROP ───────────────────────────────────────────────────────

    fn parseDrop(self: *Self) ParseError!ast.Statement {
        try self.expect(.kw_drop);

        if (self.current.type == .kw_index) {
            self.advance();
            var if_exists = false;
            if (self.current.type == .kw_if) {
                self.advance();
                try self.expect(.kw_exists);
                if_exists = true;
            }
            const name = try self.expectIdentifier();
            self.skipSemicolon();
            return .{ .drop_index = .{ .name = name, .if_exists = if_exists } };
        }

        try self.expect(.kw_table);
        var if_exists = false;
        if (self.current.type == .kw_if) {
            self.advance();
            try self.expect(.kw_exists);
            if_exists = true;
        }
        const name = try self.expectIdentifier();
        self.skipSemicolon();
        return .{ .drop_table = .{ .name = name, .if_exists = if_exists } };
    }

    // ─── BEGIN ──────────────────────────────────────────────────────

    fn parseBegin(self: *Self) ParseError!ast.Statement {
        try self.expect(.kw_begin);
        var mode: ast.Statement.TransactionMode = .deferred;
        if (self.current.type == .kw_deferred) {
            self.advance();
        } else if (self.current.type == .kw_immediate) {
            mode = .immediate;
            self.advance();
        } else if (self.current.type == .kw_exclusive) {
            mode = .exclusive;
            self.advance();
        }
        // Optional TRANSACTION keyword
        if (self.current.type == .kw_transaction) {
            self.advance();
        }
        self.skipSemicolon();
        return .{ .begin = mode };
    }

    // ─── Expression parsing (precedence climbing) ───────────────────

    fn parseExpression(self: *Self) ParseError!*ast.Expr {
        return self.parseOr();
    }

    fn parseOr(self: *Self) ParseError!*ast.Expr {
        var left = try self.parseAnd();
        while (self.current.type == .kw_or) {
            self.advance();
            const right = try self.parseAnd();
            left = try self.makeBinaryExpr(.@"or", left, right);
        }
        return left;
    }

    fn parseAnd(self: *Self) ParseError!*ast.Expr {
        var left = try self.parseComparison();
        while (self.current.type == .kw_and) {
            self.advance();
            const right = try self.parseComparison();
            left = try self.makeBinaryExpr(.@"and", left, right);
        }
        return left;
    }

    fn parseComparison(self: *Self) ParseError!*ast.Expr {
        var left = try self.parseAddition();

        while (true) {
            const op: ?ast.BinOp = switch (self.current.type) {
                .eq => .eq,
                .ne, .ne2 => .ne,
                .lt => .lt,
                .le => .le,
                .gt => .gt,
                .ge => .ge,
                else => null,
            };
            if (op) |o| {
                self.advance();
                const right = try self.parseAddition();
                left = try self.makeBinaryExpr(o, left, right);
            } else break;
        }

        // IS NULL / IS NOT NULL
        if (self.current.type == .kw_is) {
            self.advance();
            var negated = false;
            if (self.current.type == .kw_not) {
                negated = true;
                self.advance();
            }
            try self.expect(.kw_null);
            const expr = self.arena.create(ast.Expr) catch return ParseError.AllocationFailed;
            expr.* = .{ .is_null = .{ .operand = left, .negated = negated } };
            return expr;
        }

        // LIKE
        if (self.current.type == .kw_like) {
            self.advance();
            const pattern = try self.parseAddition();
            const expr = self.arena.create(ast.Expr) catch return ParseError.AllocationFailed;
            expr.* = .{ .like = .{ .operand = left, .pattern = pattern, .negated = false } };
            return expr;
        }

        return left;
    }

    fn parseAddition(self: *Self) ParseError!*ast.Expr {
        var left = try self.parseMultiplication();
        while (true) {
            const op: ?ast.BinOp = switch (self.current.type) {
                .plus => .add,
                .minus => .sub,
                .concat => .concat,
                else => null,
            };
            if (op) |o| {
                self.advance();
                const right = try self.parseMultiplication();
                left = try self.makeBinaryExpr(o, left, right);
            } else break;
        }
        return left;
    }

    fn parseMultiplication(self: *Self) ParseError!*ast.Expr {
        var left = try self.parseUnary();
        while (true) {
            const op: ?ast.BinOp = switch (self.current.type) {
                .star => .mul,
                .slash => .div,
                .percent => .mod,
                else => null,
            };
            if (op) |o| {
                self.advance();
                const right = try self.parseUnary();
                left = try self.makeBinaryExpr(o, left, right);
            } else break;
        }
        return left;
    }

    fn parseUnary(self: *Self) ParseError!*ast.Expr {
        if (self.current.type == .minus) {
            self.advance();
            const operand = try self.parsePrimary();
            const expr = self.arena.create(ast.Expr) catch return ParseError.AllocationFailed;
            expr.* = .{ .unary_op = .{ .op = .negate, .operand = operand } };
            return expr;
        }
        if (self.current.type == .kw_not) {
            self.advance();
            const operand = try self.parsePrimary();
            const expr = self.arena.create(ast.Expr) catch return ParseError.AllocationFailed;
            expr.* = .{ .unary_op = .{ .op = .not, .operand = operand } };
            return expr;
        }
        return self.parsePrimary();
    }

    fn parsePrimary(self: *Self) ParseError!*ast.Expr {
        const expr = self.arena.create(ast.Expr) catch return ParseError.AllocationFailed;

        switch (self.current.type) {
            .integer_literal => {
                const val = std.fmt.parseInt(i64, self.current.lexeme, 10) catch 0;
                expr.* = .{ .integer_literal = val };
                self.advance();
                return expr;
            },
            .real_literal => {
                const val = std.fmt.parseFloat(f64, self.current.lexeme) catch 0.0;
                expr.* = .{ .real_literal = val };
                self.advance();
                return expr;
            },
            .string_literal => {
                expr.* = .{ .string_literal = self.current.lexeme };
                self.advance();
                return expr;
            },
            .kw_null => {
                expr.* = .{ .null_literal = {} };
                self.advance();
                return expr;
            },
            .star => {
                expr.* = .{ .star = {} };
                self.advance();
                return expr;
            },
            .lparen => {
                self.advance();
                const inner = try self.parseExpression();
                try self.expect(.rparen);
                expr.* = .{ .paren = inner };
                return expr;
            },
            .identifier => {
                const name = self.current.lexeme;
                self.advance();

                // Check for function call: name(...)
                if (self.current.type == .lparen) {
                    self.advance();
                    var args: std.ArrayList(*ast.Expr) = .{};
                    var distinct = false;
                    if (self.current.type == .kw_distinct) {
                        distinct = true;
                        self.advance();
                    }
                    if (self.current.type != .rparen) {
                        while (true) {
                            const arg = try self.parseExpression();
                            args.append(self.arena, arg) catch return ParseError.AllocationFailed;
                            if (self.current.type != .comma) break;
                            self.advance();
                        }
                    }
                    try self.expect(.rparen);
                    expr.* = .{ .function_call = .{
                        .name = name,
                        .args = args.items,
                        .distinct = distinct,
                    } };
                    return expr;
                }

                // Check for table.column
                if (self.current.type == .dot) {
                    self.advance();
                    const col = try self.expectIdentifier();
                    expr.* = .{ .column_ref = .{ .table = name, .column = col } };
                    return expr;
                }

                expr.* = .{ .column_ref = .{ .table = null, .column = name } };
                return expr;
            },
            // Aggregate function keywords used as identifiers
            .kw_count, .kw_sum, .kw_avg, .kw_min, .kw_max => {
                const name = self.current.lexeme;
                self.advance();
                if (self.current.type == .lparen) {
                    self.advance();
                    var args: std.ArrayList(*ast.Expr) = .{};
                    var distinct = false;
                    if (self.current.type == .kw_distinct) {
                        distinct = true;
                        self.advance();
                    }
                    if (self.current.type != .rparen) {
                        while (true) {
                            const arg = try self.parseExpression();
                            args.append(self.arena, arg) catch return ParseError.AllocationFailed;
                            if (self.current.type != .comma) break;
                            self.advance();
                        }
                    }
                    try self.expect(.rparen);
                    expr.* = .{ .function_call = .{
                        .name = name,
                        .args = args.items,
                        .distinct = distinct,
                    } };
                    return expr;
                }
                expr.* = .{ .column_ref = .{ .table = null, .column = name } };
                return expr;
            },
            else => return ParseError.ExpectedExpression,
        }
    }

    // ─── Table reference ────────────────────────────────────────────

    fn parseTableRef(self: *Self) ParseError!ast.TableRef {
        const name = try self.expectIdentifier();
        var alias: ?[]const u8 = null;
        if (self.current.type == .kw_as) {
            self.advance();
            alias = try self.expectIdentifier();
        } else if (self.current.type == .identifier) {
            // Implicit alias
            alias = self.current.lexeme;
            self.advance();
        }
        return .{ .name = name, .alias = alias };
    }

    // ─── Helpers ────────────────────────────────────────────────────

    fn advance(self: *Self) void {
        self.current = self.tok.next();
    }

    fn expect(self: *Self, expected: tokenizer.TokenType) ParseError!void {
        if (self.current.type != expected) {
            return switch (expected) {
                .lparen => ParseError.ExpectedLeftParen,
                .rparen => ParseError.ExpectedRightParen,
                .comma => ParseError.ExpectedComma,
                .semicolon => ParseError.ExpectedSemicolon,
                .identifier => ParseError.ExpectedIdentifier,
                else => ParseError.ExpectedKeyword,
            };
        }
        self.advance();
    }

    fn expectIdentifier(self: *Self) ParseError![]const u8 {
        if (self.current.type != .identifier) {
            return ParseError.ExpectedIdentifier;
        }
        const name = self.current.lexeme;
        self.advance();
        return name;
    }

    fn skipSemicolon(self: *Self) void {
        if (self.current.type == .semicolon) {
            self.advance();
        }
    }

    fn makeBinaryExpr(self: *Self, op: ast.BinOp, left: *ast.Expr, right: *ast.Expr) ParseError!*ast.Expr {
        const expr = self.arena.create(ast.Expr) catch return ParseError.AllocationFailed;
        expr.* = .{ .binary_op = .{ .op = op, .left = left, .right = right } };
        return expr;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

test "parse SELECT * FROM table" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = Parser.init("SELECT * FROM users;", arena.allocator());
    const stmt = try p.parseStatement();
    try std.testing.expect(stmt == .select);
    try std.testing.expect(stmt.select.from != null);
    try std.testing.expectEqualStrings("users", stmt.select.from.?.name);
}

test "parse INSERT INTO" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = Parser.init("INSERT INTO users (name, age) VALUES ('Alice', 30);", arena.allocator());
    const stmt = try p.parseStatement();
    try std.testing.expect(stmt == .insert);
    try std.testing.expectEqualStrings("users", stmt.insert.table);
    try std.testing.expectEqual(@as(usize, 2), stmt.insert.columns.?.len);
    try std.testing.expectEqual(@as(usize, 1), stmt.insert.values.len);
}

test "parse CREATE TABLE" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = Parser.init("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);", arena.allocator());
    const stmt = try p.parseStatement();
    try std.testing.expect(stmt == .create_table);
    try std.testing.expectEqualStrings("users", stmt.create_table.name);
    try std.testing.expectEqual(@as(usize, 2), stmt.create_table.columns.len);
    try std.testing.expect(stmt.create_table.columns[0].primary_key);
    try std.testing.expect(stmt.create_table.columns[1].not_null);
}

test "parse UPDATE with WHERE" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = Parser.init("UPDATE users SET name = 'Bob' WHERE id = 1;", arena.allocator());
    const stmt = try p.parseStatement();
    try std.testing.expect(stmt == .update);
    try std.testing.expect(stmt.update.where != null);
}

test "parse DELETE with WHERE" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = Parser.init("DELETE FROM users WHERE id = 1;", arena.allocator());
    const stmt = try p.parseStatement();
    try std.testing.expect(stmt == .delete);
    try std.testing.expectEqualStrings("users", stmt.delete.table);
    try std.testing.expect(stmt.delete.where != null);
}

test "parse EXPLAIN" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = Parser.init("EXPLAIN SELECT * FROM users;", arena.allocator());
    const stmt = try p.parseStatement();
    try std.testing.expect(stmt == .explain);
    try std.testing.expect(stmt.explain.* == .select);
}

test "parse SELECT with ORDER BY and LIMIT" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = Parser.init("SELECT name FROM users ORDER BY name DESC LIMIT 10;", arena.allocator());
    const stmt = try p.parseStatement();
    try std.testing.expect(stmt == .select);
    try std.testing.expectEqual(@as(usize, 1), stmt.select.order_by.len);
    try std.testing.expectEqual(ast.SortOrder.desc, stmt.select.order_by[0].order);
    try std.testing.expect(stmt.select.limit != null);
}

test "parse transaction statements" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p1 = Parser.init("BEGIN;", arena.allocator());
    try std.testing.expect((try p1.parseStatement()) == .begin);

    var p2 = Parser.init("COMMIT;", arena.allocator());
    try std.testing.expect((try p2.parseStatement()) == .commit);

    var p3 = Parser.init("ROLLBACK;", arena.allocator());
    try std.testing.expect((try p3.parseStatement()) == .rollback);
}
