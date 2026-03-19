const std = @import("std");
const zqlite = @import("zqlite");

test "record: round-trip all types" {
    const values = [_]zqlite.record.Value{
        .{ .null_val = {} },
        .{ .integer = 0 },
        .{ .integer = 1 },
        .{ .integer = -128 },
        .{ .integer = 32000 },
        .{ .integer = 100000 },
        .{ .integer = -9999999999 },
        .{ .real = 3.14159265 },
        .{ .text = "zqlite" },
        .{ .blob = &[_]u8{ 0xFF, 0x00, 0xAB } },
    };

    var buf: [2048]u8 = undefined;
    const n = try zqlite.record.serializeRecord(&values, &buf);
    const decoded = try zqlite.record.deserializeRecord(buf[0..n], std.testing.allocator);
    defer std.testing.allocator.free(decoded);

    try std.testing.expectEqual(values.len, decoded.len);
    try std.testing.expect(decoded[0].isNull());
    try std.testing.expectEqual(@as(i64, 0), decoded[1].asInteger().?);
    try std.testing.expectEqual(@as(i64, 1), decoded[2].asInteger().?);
    try std.testing.expectEqual(@as(i64, -128), decoded[3].asInteger().?);
    try std.testing.expectEqual(@as(i64, 32000), decoded[4].asInteger().?);
    try std.testing.expectEqual(@as(f64, 3.14159265), decoded[7].asReal().?);
    try std.testing.expectEqualStrings("zqlite", decoded[8].asText().?);
}

test "record: lazy column access skips unneeded columns" {
    const values = [_]zqlite.record.Value{
        .{ .integer = 100 },
        .{ .text = "skip me" },
        .{ .integer = 200 },
        .{ .text = "target" },
    };

    var buf: [512]u8 = undefined;
    const n = try zqlite.record.serializeRecord(&values, &buf);

    // Access column 3 directly
    const col3 = try zqlite.record.readColumn(buf[0..n], 3);
    try std.testing.expectEqualStrings("target", col3.asText().?);

    // Access column 0
    const col0 = try zqlite.record.readColumn(buf[0..n], 0);
    try std.testing.expectEqual(@as(i64, 100), col0.asInteger().?);
}

test "record: varint edge cases" {
    const cases = [_]u64{ 0, 1, 127, 128, 255, 256, 16383, 16384, 0xFFFF, 0xFFFFFFFF };
    for (cases) |v| {
        var buf: [9]u8 = undefined;
        const n = try zqlite.record.putVarint(&buf, v);
        const result = try zqlite.record.getVarint(buf[0..n]);
        try std.testing.expectEqual(v, result.value);
    }
}

test "record: empty record" {
    const values = [_]zqlite.record.Value{};
    var buf: [64]u8 = undefined;
    const n = try zqlite.record.serializeRecord(&values, &buf);
    try std.testing.expect(n > 0);

    const decoded = try zqlite.record.deserializeRecord(buf[0..n], std.testing.allocator);
    defer std.testing.allocator.free(decoded);
    try std.testing.expectEqual(@as(usize, 0), decoded.len);
}
