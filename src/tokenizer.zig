const std = @import("std");

/// SQL Tokenizer for ZQLite.
///
/// Zero-allocation tokenizer operating directly on the input slice.
/// Keywords are recognized via a comptime-generated hash map.

// ═══════════════════════════════════════════════════════════════════════════
// Token types
// ═══════════════════════════════════════════════════════════════════════════

pub const TokenType = enum {
    // Keywords
    kw_select,
    kw_from,
    kw_where,
    kw_and,
    kw_or,
    kw_not,
    kw_insert,
    kw_into,
    kw_values,
    kw_update,
    kw_set,
    kw_delete,
    kw_create,
    kw_drop,
    kw_table,
    kw_index,
    kw_on,
    kw_if,
    kw_exists,
    kw_null,
    kw_integer,
    kw_real,
    kw_text,
    kw_blob,
    kw_primary,
    kw_key,
    kw_unique,
    kw_default,
    kw_order,
    kw_by,
    kw_asc,
    kw_desc,
    kw_limit,
    kw_group,
    kw_having,
    kw_as,
    kw_join,
    kw_inner,
    kw_left,
    kw_cross,
    kw_begin,
    kw_commit,
    kw_rollback,
    kw_transaction,
    kw_explain,
    kw_like,
    kw_between,
    kw_in,
    kw_is,
    kw_distinct,
    kw_count,
    kw_sum,
    kw_avg,
    kw_min,
    kw_max,
    kw_deferred,
    kw_immediate,
    kw_exclusive,

    // Literals
    integer_literal,
    real_literal,
    string_literal,
    blob_literal,

    // Identifiers
    identifier,

    // Operators
    plus,
    minus,
    star,
    slash,
    percent,
    eq,
    ne, // !=
    ne2, // <>
    lt,
    le,
    gt,
    ge,
    concat, // ||

    // Punctuation
    lparen,
    rparen,
    comma,
    semicolon,
    dot,

    // Special
    question_mark, // ? placeholder
    eof,
    invalid,
};

// ═══════════════════════════════════════════════════════════════════════════
// Token
// ═══════════════════════════════════════════════════════════════════════════

pub const Token = struct {
    type: TokenType,
    lexeme: []const u8,
    line: u32,
    col: u32,
};

// ═══════════════════════════════════════════════════════════════════════════
// Keyword map (comptime perfect hash)
// ═══════════════════════════════════════════════════════════════════════════

const keywords = std.StaticStringMap(TokenType).initComptime(.{
    .{ "SELECT", .kw_select },
    .{ "FROM", .kw_from },
    .{ "WHERE", .kw_where },
    .{ "AND", .kw_and },
    .{ "OR", .kw_or },
    .{ "NOT", .kw_not },
    .{ "INSERT", .kw_insert },
    .{ "INTO", .kw_into },
    .{ "VALUES", .kw_values },
    .{ "UPDATE", .kw_update },
    .{ "SET", .kw_set },
    .{ "DELETE", .kw_delete },
    .{ "CREATE", .kw_create },
    .{ "DROP", .kw_drop },
    .{ "TABLE", .kw_table },
    .{ "INDEX", .kw_index },
    .{ "ON", .kw_on },
    .{ "IF", .kw_if },
    .{ "EXISTS", .kw_exists },
    .{ "NULL", .kw_null },
    .{ "INTEGER", .kw_integer },
    .{ "REAL", .kw_real },
    .{ "TEXT", .kw_text },
    .{ "BLOB", .kw_blob },
    .{ "PRIMARY", .kw_primary },
    .{ "KEY", .kw_key },
    .{ "UNIQUE", .kw_unique },
    .{ "DEFAULT", .kw_default },
    .{ "ORDER", .kw_order },
    .{ "BY", .kw_by },
    .{ "ASC", .kw_asc },
    .{ "DESC", .kw_desc },
    .{ "LIMIT", .kw_limit },
    .{ "GROUP", .kw_group },
    .{ "HAVING", .kw_having },
    .{ "AS", .kw_as },
    .{ "JOIN", .kw_join },
    .{ "INNER", .kw_inner },
    .{ "LEFT", .kw_left },
    .{ "CROSS", .kw_cross },
    .{ "BEGIN", .kw_begin },
    .{ "COMMIT", .kw_commit },
    .{ "ROLLBACK", .kw_rollback },
    .{ "TRANSACTION", .kw_transaction },
    .{ "EXPLAIN", .kw_explain },
    .{ "LIKE", .kw_like },
    .{ "BETWEEN", .kw_between },
    .{ "IN", .kw_in },
    .{ "IS", .kw_is },
    .{ "DISTINCT", .kw_distinct },
    .{ "COUNT", .kw_count },
    .{ "SUM", .kw_sum },
    .{ "AVG", .kw_avg },
    .{ "MIN", .kw_min },
    .{ "MAX", .kw_max },
    .{ "DEFERRED", .kw_deferred },
    .{ "IMMEDIATE", .kw_immediate },
    .{ "EXCLUSIVE", .kw_exclusive },
});

// ═══════════════════════════════════════════════════════════════════════════
// Tokenizer
// ═══════════════════════════════════════════════════════════════════════════

pub const Tokenizer = struct {
    source: []const u8,
    pos: usize,
    line: u32,
    col: u32,

    const Self = @This();

    pub fn init(source: []const u8) Self {
        return Self{
            .source = source,
            .pos = 0,
            .line = 1,
            .col = 1,
        };
    }

    /// Get the next token.
    pub fn next(self: *Self) Token {
        self.skipWhitespace();

        if (self.pos >= self.source.len) {
            return self.makeToken(.eof, "");
        }

        const start = self.pos;
        const start_line = self.line;
        const start_col = self.col;
        const c = self.advance();

        // Single-character tokens
        switch (c) {
            '(' => return self.makeTokenAt(.lparen, self.source[start .. start + 1], start_line, start_col),
            ')' => return self.makeTokenAt(.rparen, self.source[start .. start + 1], start_line, start_col),
            ',' => return self.makeTokenAt(.comma, self.source[start .. start + 1], start_line, start_col),
            ';' => return self.makeTokenAt(.semicolon, self.source[start .. start + 1], start_line, start_col),
            '.' => return self.makeTokenAt(.dot, self.source[start .. start + 1], start_line, start_col),
            '+' => return self.makeTokenAt(.plus, self.source[start .. start + 1], start_line, start_col),
            '-' => return self.makeTokenAt(.minus, self.source[start .. start + 1], start_line, start_col),
            '*' => return self.makeTokenAt(.star, self.source[start .. start + 1], start_line, start_col),
            '/' => return self.makeTokenAt(.slash, self.source[start .. start + 1], start_line, start_col),
            '%' => return self.makeTokenAt(.percent, self.source[start .. start + 1], start_line, start_col),
            '?' => return self.makeTokenAt(.question_mark, self.source[start .. start + 1], start_line, start_col),
            '=' => return self.makeTokenAt(.eq, self.source[start .. start + 1], start_line, start_col),
            '<' => {
                if (self.peek() == '=') {
                    _ = self.advance();
                    return self.makeTokenAt(.le, self.source[start .. start + 2], start_line, start_col);
                } else if (self.peek() == '>') {
                    _ = self.advance();
                    return self.makeTokenAt(.ne2, self.source[start .. start + 2], start_line, start_col);
                }
                return self.makeTokenAt(.lt, self.source[start .. start + 1], start_line, start_col);
            },
            '>' => {
                if (self.peek() == '=') {
                    _ = self.advance();
                    return self.makeTokenAt(.ge, self.source[start .. start + 2], start_line, start_col);
                }
                return self.makeTokenAt(.gt, self.source[start .. start + 1], start_line, start_col);
            },
            '!' => {
                if (self.peek() == '=') {
                    _ = self.advance();
                    return self.makeTokenAt(.ne, self.source[start .. start + 2], start_line, start_col);
                }
                return self.makeTokenAt(.invalid, self.source[start .. start + 1], start_line, start_col);
            },
            '|' => {
                if (self.peek() == '|') {
                    _ = self.advance();
                    return self.makeTokenAt(.concat, self.source[start .. start + 2], start_line, start_col);
                }
                return self.makeTokenAt(.invalid, self.source[start .. start + 1], start_line, start_col);
            },

            // String literal
            '\'' => return self.readStringLiteral(start, start_line, start_col),

            else => {
                // Numbers
                if (isDigit(c)) {
                    return self.readNumber(start, start_line, start_col);
                }
                // Identifiers / keywords
                if (isAlpha(c) or c == '_') {
                    return self.readIdentifier(start, start_line, start_col);
                }

                return self.makeTokenAt(.invalid, self.source[start .. start + 1], start_line, start_col);
            },
        }
    }

    // ─── Helpers ────────────────────────────────────────────────────

    fn readStringLiteral(self: *Self, start: usize, start_line: u32, start_col: u32) Token {
        while (self.pos < self.source.len) {
            const ch = self.advance();
            if (ch == '\'') {
                // Check for escaped quote ('')
                if (self.peek() == '\'') {
                    _ = self.advance();
                    continue;
                }
                return self.makeTokenAt(.string_literal, self.source[start + 1 .. self.pos - 1], start_line, start_col);
            }
        }
        return self.makeTokenAt(.invalid, self.source[start..self.pos], start_line, start_col);
    }

    fn readNumber(self: *Self, start: usize, start_line: u32, start_col: u32) Token {
        var is_real = false;
        while (self.pos < self.source.len and isDigit(self.source[self.pos])) {
            _ = self.advance();
        }
        if (self.pos < self.source.len and self.source[self.pos] == '.') {
            is_real = true;
            _ = self.advance();
            while (self.pos < self.source.len and isDigit(self.source[self.pos])) {
                _ = self.advance();
            }
        }
        const tt: TokenType = if (is_real) .real_literal else .integer_literal;
        return self.makeTokenAt(tt, self.source[start..self.pos], start_line, start_col);
    }

    fn readIdentifier(self: *Self, start: usize, start_line: u32, start_col: u32) Token {
        while (self.pos < self.source.len and (isAlphaNum(self.source[self.pos]) or self.source[self.pos] == '_')) {
            _ = self.advance();
        }
        const lexeme = self.source[start..self.pos];

        // Check for keyword (case-insensitive via uppercase copy)
        var upper_buf: [64]u8 = undefined;
        if (lexeme.len <= upper_buf.len) {
            for (lexeme, 0..) |ch, i| {
                upper_buf[i] = std.ascii.toUpper(ch);
            }
            if (keywords.get(upper_buf[0..lexeme.len])) |kw_type| {
                return self.makeTokenAt(kw_type, lexeme, start_line, start_col);
            }
        }

        return self.makeTokenAt(.identifier, lexeme, start_line, start_col);
    }

    fn skipWhitespace(self: *Self) void {
        while (self.pos < self.source.len) {
            const c = self.source[self.pos];
            if (c == ' ' or c == '\t' or c == '\r') {
                self.pos += 1;
                self.col += 1;
            } else if (c == '\n') {
                self.pos += 1;
                self.line += 1;
                self.col = 1;
            } else if (c == '-' and self.pos + 1 < self.source.len and self.source[self.pos + 1] == '-') {
                // Comment until end of line
                while (self.pos < self.source.len and self.source[self.pos] != '\n') {
                    self.pos += 1;
                }
            } else {
                break;
            }
        }
    }

    fn advance(self: *Self) u8 {
        const c = self.source[self.pos];
        self.pos += 1;
        self.col += 1;
        return c;
    }

    fn peek(self: *const Self) u8 {
        if (self.pos >= self.source.len) return 0;
        return self.source[self.pos];
    }

    fn makeToken(self: *const Self, tt: TokenType, lexeme: []const u8) Token {
        return .{ .type = tt, .lexeme = lexeme, .line = self.line, .col = self.col };
    }

    fn makeTokenAt(_: *const Self, tt: TokenType, lexeme: []const u8, line: u32, col: u32) Token {
        return .{ .type = tt, .lexeme = lexeme, .line = line, .col = col };
    }

    fn isDigit(c: u8) bool {
        return c >= '0' and c <= '9';
    }

    fn isAlpha(c: u8) bool {
        return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z');
    }

    fn isAlphaNum(c: u8) bool {
        return isDigit(c) or isAlpha(c);
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

test "tokenize SELECT statement" {
    var t = Tokenizer.init("SELECT * FROM users WHERE id = 42;");

    try std.testing.expectEqual(TokenType.kw_select, t.next().type);
    try std.testing.expectEqual(TokenType.star, t.next().type);
    try std.testing.expectEqual(TokenType.kw_from, t.next().type);
    try std.testing.expectEqual(TokenType.identifier, t.next().type);
    try std.testing.expectEqual(TokenType.kw_where, t.next().type);
    try std.testing.expectEqual(TokenType.identifier, t.next().type);
    try std.testing.expectEqual(TokenType.eq, t.next().type);
    try std.testing.expectEqual(TokenType.integer_literal, t.next().type);
    try std.testing.expectEqual(TokenType.semicolon, t.next().type);
    try std.testing.expectEqual(TokenType.eof, t.next().type);
}

test "tokenize INSERT statement" {
    var t = Tokenizer.init("INSERT INTO users (name, age) VALUES ('Alice', 30);");

    try std.testing.expectEqual(TokenType.kw_insert, t.next().type);
    try std.testing.expectEqual(TokenType.kw_into, t.next().type);
    try std.testing.expectEqual(TokenType.identifier, t.next().type); // users
    try std.testing.expectEqual(TokenType.lparen, t.next().type);
    const name_tok = t.next();
    try std.testing.expectEqual(TokenType.identifier, name_tok.type);
    try std.testing.expectEqualStrings("name", name_tok.lexeme);
    try std.testing.expectEqual(TokenType.comma, t.next().type);
    try std.testing.expectEqual(TokenType.identifier, t.next().type); // age
    try std.testing.expectEqual(TokenType.rparen, t.next().type);
    try std.testing.expectEqual(TokenType.kw_values, t.next().type);
    try std.testing.expectEqual(TokenType.lparen, t.next().type);
    const str_tok = t.next();
    try std.testing.expectEqual(TokenType.string_literal, str_tok.type);
    try std.testing.expectEqualStrings("Alice", str_tok.lexeme);
}

test "tokenize comparison operators" {
    var t = Tokenizer.init("a <= b >= c != d <> e");

    _ = t.next(); // a
    try std.testing.expectEqual(TokenType.le, t.next().type);
    _ = t.next(); // b
    try std.testing.expectEqual(TokenType.ge, t.next().type);
    _ = t.next(); // c
    try std.testing.expectEqual(TokenType.ne, t.next().type);
    _ = t.next(); // d
    try std.testing.expectEqual(TokenType.ne2, t.next().type);
}

test "tokenize real number" {
    var t = Tokenizer.init("3.14");
    const tok = t.next();
    try std.testing.expectEqual(TokenType.real_literal, tok.type);
    try std.testing.expectEqualStrings("3.14", tok.lexeme);
}

test "skip line comments" {
    var t = Tokenizer.init("-- this is a comment\nSELECT 1");
    try std.testing.expectEqual(TokenType.kw_select, t.next().type);
}

test "case-insensitive keywords" {
    var t = Tokenizer.init("select FROM Where");
    try std.testing.expectEqual(TokenType.kw_select, t.next().type);
    try std.testing.expectEqual(TokenType.kw_from, t.next().type);
    try std.testing.expectEqual(TokenType.kw_where, t.next().type);
}
