const std = @import("std");
const zqlite = @import("zqlite");

test "tokenizer: all token types" {
    var t = zqlite.tokenizer.Tokenizer.init(
        "SELECT * FROM t WHERE a = 1 AND b != 'test' OR c >= 3.14 ORDER BY d DESC LIMIT 10;",
    );

    const expected = [_]zqlite.tokenizer.TokenType{
        .kw_select, .star,    .kw_from, .identifier, .kw_where, .identifier,
        .eq,        .integer_literal, .kw_and,  .identifier,  .ne,        .string_literal,
        .kw_or,     .identifier, .ge,      .real_literal,     .kw_order,  .kw_by,
        .identifier, .kw_desc, .kw_limit, .integer_literal,              .semicolon,
        .eof,
    };

    for (expected) |exp| {
        const tok = t.next();
        try std.testing.expectEqual(exp, tok.type);
    }
}

test "tokenizer: concatenation and modulo" {
    var t = zqlite.tokenizer.Tokenizer.init("a || b % c");
    _ = t.next(); // a
    try std.testing.expectEqual(zqlite.tokenizer.TokenType.concat, t.next().type);
    _ = t.next(); // b
    try std.testing.expectEqual(zqlite.tokenizer.TokenType.percent, t.next().type);
}

test "tokenizer: line tracking" {
    var t = zqlite.tokenizer.Tokenizer.init("SELECT\n  1\n  +\n  2");
    const sel = t.next();
    try std.testing.expectEqual(@as(u32, 1), sel.line);

    const one = t.next();
    try std.testing.expectEqual(@as(u32, 2), one.line);
}

test "tokenizer: empty input" {
    var t = zqlite.tokenizer.Tokenizer.init("");
    try std.testing.expectEqual(zqlite.tokenizer.TokenType.eof, t.next().type);
}

test "tokenizer: nested parentheses" {
    var t = zqlite.tokenizer.Tokenizer.init("((1 + 2))");
    try std.testing.expectEqual(zqlite.tokenizer.TokenType.lparen, t.next().type);
    try std.testing.expectEqual(zqlite.tokenizer.TokenType.lparen, t.next().type);
    try std.testing.expectEqual(zqlite.tokenizer.TokenType.integer_literal, t.next().type);
    try std.testing.expectEqual(zqlite.tokenizer.TokenType.plus, t.next().type);
    try std.testing.expectEqual(zqlite.tokenizer.TokenType.integer_literal, t.next().type);
    try std.testing.expectEqual(zqlite.tokenizer.TokenType.rparen, t.next().type);
    try std.testing.expectEqual(zqlite.tokenizer.TokenType.rparen, t.next().type);
}
