const std = @import("std");

/// Record format for ZQLite.
///
/// Compatible with SQLite's record format:
///   - Varint-encoded header length, followed by type codes for each column
///   - Column data packed immediately after the header
///
/// Type codes (serial types):
///   0 → NULL
///   1 → 1-byte signed integer
///   2 → 2-byte big-endian signed integer
///   3 → 3-byte big-endian signed integer
///   4 → 4-byte big-endian signed integer
///   5 → 6-byte big-endian signed integer
///   6 → 8-byte big-endian signed integer
///   7 → 8-byte IEEE 754 float
///   8 → integer 0 (schema format >= 4)
///   9 → integer 1 (schema format >= 4)
///   N >= 12 even → blob of length (N-12)/2
///   N >= 13 odd  → text of length (N-13)/2

// ═══════════════════════════════════════════════════════════════════════════
// Errors
// ═══════════════════════════════════════════════════════════════════════════

pub const RecordError = error{
    BufferTooSmall,
    InvalidSerialType,
    InvalidVarint,
    CorruptRecord,
    TooManyColumns,
};

// ═══════════════════════════════════════════════════════════════════════════
// Value — runtime representation of a column value
// ═══════════════════════════════════════════════════════════════════════════

pub const Value = union(enum) {
    null_val: void,
    integer: i64,
    real: f64,
    text: []const u8,
    blob: []const u8,

    pub fn isNull(self: Value) bool {
        return self == .null_val;
    }

    pub fn asInteger(self: Value) ?i64 {
        return switch (self) {
            .integer => |v| v,
            else => null,
        };
    }

    pub fn asReal(self: Value) ?f64 {
        return switch (self) {
            .real => |v| v,
            .integer => |v| @floatFromInt(v),
            else => null,
        };
    }

    pub fn asText(self: Value) ?[]const u8 {
        return switch (self) {
            .text => |v| v,
            else => null,
        };
    }

    pub fn asBlob(self: Value) ?[]const u8 {
        return switch (self) {
            .blob => |v| v,
            else => null,
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Varint encoding/decoding (SQLite-style, up to 9 bytes)
// ═══════════════════════════════════════════════════════════════════════════

/// Encode a u64 as a SQLite-style varint (up to 9 bytes). Returns the number of bytes written.
///
/// Format: bytes 1-8 use 7-bit encoding with high-bit continuation.
/// The 9th byte (if needed) stores all 8 bits with no continuation flag.
pub fn putVarint(buf: []u8, value: u64) RecordError!usize {
    if (buf.len == 0) return RecordError.BufferTooSmall;

    var v = value;
    var n: usize = 0;

    // Bytes 1-8: 7 bits each + continuation bit
    while (v > 0x7F and n < 8) {
        if (n >= buf.len) return RecordError.BufferTooSmall;
        buf[n] = @intCast((v & 0x7F) | 0x80);
        v >>= 7;
        n += 1;
    }

    if (n >= buf.len) return RecordError.BufferTooSmall;

    if (n < 8) {
        // Final byte with no continuation bit (value fits in 7 bits or less remaining)
        buf[n] = @intCast(v);
    } else {
        // 9th byte: store all remaining 8 bits (no continuation bit)
        buf[n] = @intCast(v);
    }
    n += 1;

    return n;
}

/// Decode a SQLite-style varint. Returns the value and number of bytes consumed.
pub fn getVarint(buf: []const u8) RecordError!struct { value: u64, bytes: usize } {
    if (buf.len == 0) return RecordError.InvalidVarint;

    // Fast path: single-byte varint (values 0-127, covers ~95% of record type codes)
    if (buf[0] & 0x80 == 0) {
        return .{ .value = buf[0], .bytes = 1 };
    }

    var result: u64 = @as(u64, buf[0] & 0x7F);
    var i: usize = 1;

    // Read up to 8 bytes with 7-bit encoding + continuation bit
    while (i < 8 and i < buf.len) : (i += 1) {
        result |= @as(u64, buf[i] & 0x7F) << @intCast(i * 7);
        if (buf[i] & 0x80 == 0) {
            return .{ .value = result, .bytes = i + 1 };
        }
    }

    // 9th byte: all 8 bits, no continuation
    if (i < buf.len) {
        result |= @as(u64, buf[i]) << @intCast(i * 7);
        return .{ .value = result, .bytes = i + 1 };
    }

    return RecordError.InvalidVarint;
}

// ═══════════════════════════════════════════════════════════════════════════
// Serial type helpers
// ═══════════════════════════════════════════════════════════════════════════

/// Determine the serial type for a value (used for encoding).
pub fn serialType(value: Value) u64 {
    return switch (value) {
        .null_val => 0,
        .integer => |v| blk: {
            if (v == 0) break :blk 8;
            if (v == 1) break :blk 9;
            if (v >= -128 and v <= 127) break :blk 1;
            if (v >= -32768 and v <= 32767) break :blk 2;
            if (v >= -8388608 and v <= 8388607) break :blk 3;
            if (v >= -2147483648 and v <= 2147483647) break :blk 4;
            if (v >= -140737488355328 and v <= 140737488355327) break :blk 5;
            break :blk 6;
        },
        .real => 7,
        .text => |t| 13 + @as(u64, @intCast(t.len)) * 2,
        .blob => |b| 12 + @as(u64, @intCast(b.len)) * 2,
    };
}

/// Get the byte length of the content for a given serial type.
pub fn serialTypeLen(st: u64) usize {
    return switch (st) {
        0 => 0, // NULL
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 4,
        5 => 6,
        6 => 8,
        7 => 8, // float64
        8 => 0, // integer 0
        9 => 0, // integer 1
        else => blk: {
            if (st >= 12) {
                break :blk @intCast((st - 12) / 2);
            }
            break :blk 0;
        },
    };
}

// ═══════════════════════════════════════════════════════════════════════════
// Record serialization
// ═══════════════════════════════════════════════════════════════════════════

pub const MAX_COLUMNS = 256;

/// Serialize a row of values into the record format.
/// Returns the number of bytes written.
pub fn serializeRecord(values: []const Value, buf: []u8) RecordError!usize {
    if (values.len > MAX_COLUMNS) return RecordError.TooManyColumns;

    // First pass: compute header size and type codes
    var type_codes: [MAX_COLUMNS]u64 = undefined;
    var header_content_size: usize = 0;
    var body_size: usize = 0;

    for (values, 0..) |val, i| {
        type_codes[i] = serialType(val);
        // Measure varint size for this type code
        var temp: [9]u8 = undefined;
        const tc_len = putVarint(&temp, type_codes[i]) catch unreachable;
        header_content_size += tc_len;
        body_size += serialTypeLen(type_codes[i]);
    }

    // Header length varint (includes itself)
    var header_len_buf: [9]u8 = undefined;
    // We need to figure out the total header size, which includes the header-length varint itself
    var total_header_size = header_content_size + 1; // guess 1 byte for header_len varint
    var header_len_size = putVarint(&header_len_buf, @intCast(total_header_size)) catch unreachable;
    // Adjust if the header length varint itself is larger than 1 byte
    if (header_len_size > 1) {
        total_header_size = header_content_size + header_len_size;
        header_len_size = putVarint(&header_len_buf, @intCast(total_header_size)) catch unreachable;
    }

    const total_size = total_header_size + body_size;
    if (buf.len < total_size) return RecordError.BufferTooSmall;

    // Write header: length varint + type code varints
    var pos: usize = 0;
    @memcpy(buf[pos .. pos + header_len_size], header_len_buf[0..header_len_size]);
    pos += header_len_size;

    for (values, 0..) |_, i| {
        const n = putVarint(buf[pos..], type_codes[i]) catch unreachable;
        pos += n;
    }

    // Write body
    for (values) |val| {
        switch (val) {
            .null_val => {},
            .integer => |v| {
                const st = serialType(val);
                const len = serialTypeLen(st);
                if (len > 0) {
                    writeIntBigEndian(buf[pos .. pos + len], v, len);
                    pos += len;
                }
                // st 8 and 9 have len=0, no bytes written
            },
            .real => |v| {
                const bits = @as(u64, @bitCast(v));
                std.mem.writeInt(u64, buf[pos..][0..8], bits, .big);
                pos += 8;
            },
            .text => |t| {
                @memcpy(buf[pos .. pos + t.len], t);
                pos += t.len;
            },
            .blob => |b| {
                @memcpy(buf[pos .. pos + b.len], b);
                pos += b.len;
            },
        }
    }

    return total_size;
}
/// Deserialize a record into a caller-provided buffer (zero-allocation).
/// Returns the slice of values written.
pub fn deserializeRecordBuf(buf: []const u8, out: []Value) RecordError![]Value {
    const header_info = getVarint(buf) catch return RecordError.CorruptRecord;
    const header_len = header_info.value;
    if (header_len > buf.len) return RecordError.CorruptRecord;

    var num_cols: usize = 0;
    var hdr_pos: usize = header_info.bytes;
    var body_pos: usize = @intCast(header_len);

    while (hdr_pos < header_len) {
        if (num_cols >= out.len) return RecordError.TooManyColumns;
        const tc_info = getVarint(buf[hdr_pos..]) catch return RecordError.CorruptRecord;
        const st = tc_info.value;
        const len = serialTypeLen(st);

        if (body_pos + len > buf.len) return RecordError.CorruptRecord;

        out[num_cols] = switch (st) {
            0 => .{ .null_val = {} },
            1, 2, 3, 4, 5, 6 => .{ .integer = readIntBigEndian(buf[body_pos .. body_pos + len], len) },
            7 => blk: {
                const bits = std.mem.readInt(u64, buf[body_pos..][0..8], .big);
                break :blk .{ .real = @bitCast(bits) };
            },
            8 => .{ .integer = 0 },
            9 => .{ .integer = 1 },
            else => blk: {
                if (st >= 12) {
                    const content = buf[body_pos .. body_pos + len];
                    if (st % 2 == 0) {
                        break :blk .{ .blob = content };
                    } else {
                        break :blk .{ .text = content };
                    }
                }
                break :blk .{ .null_val = {} };
            },
        };

        body_pos += len;
        hdr_pos += tc_info.bytes;
        num_cols += 1;
    }

    return out[0..num_cols];
}

/// Deserialize a record, returning all column values.
pub fn deserializeRecord(buf: []const u8, allocator: std.mem.Allocator) RecordError![]Value {
    // Read header length
    const header_info = getVarint(buf) catch return RecordError.CorruptRecord;
    const header_len = header_info.value;

    if (header_len > buf.len) return RecordError.CorruptRecord;

    // Read type codes from header
    var type_codes: [MAX_COLUMNS]u64 = undefined;
    var num_cols: usize = 0;
    var hdr_pos: usize = header_info.bytes;

    while (hdr_pos < header_len) {
        if (num_cols >= MAX_COLUMNS) return RecordError.TooManyColumns;
        const tc_info = getVarint(buf[hdr_pos..]) catch return RecordError.CorruptRecord;
        type_codes[num_cols] = tc_info.value;
        hdr_pos += tc_info.bytes;
        num_cols += 1;
    }

    // Allocate values array
    const values = allocator.alloc(Value, num_cols) catch
        return RecordError.CorruptRecord;

    // Decode body
    var body_pos: usize = @intCast(header_len);

    for (0..num_cols) |i| {
        const st = type_codes[i];
        const len = serialTypeLen(st);

        if (body_pos + len > buf.len) {
            allocator.free(values);
            return RecordError.CorruptRecord;
        }

        values[i] = switch (st) {
            0 => .{ .null_val = {} },
            1, 2, 3, 4, 5, 6 => .{ .integer = readIntBigEndian(buf[body_pos .. body_pos + len], len) },
            7 => blk: {
                const bits = std.mem.readInt(u64, buf[body_pos..][0..8], .big);
                break :blk .{ .real = @bitCast(bits) };
            },
            8 => .{ .integer = 0 },
            9 => .{ .integer = 1 },
            else => blk: {
                if (st >= 12) {
                    const content = buf[body_pos .. body_pos + len];
                    if (st % 2 == 0) {
                        break :blk .{ .blob = content };
                    } else {
                        break :blk .{ .text = content };
                    }
                }
                break :blk .{ .null_val = {} };
            },
        };

        body_pos += len;
    }

    return values;
}

/// Read a column from a record by index WITHOUT deserializing the entire record.
/// This is the "lazy deserialization" optimization.
pub fn readColumn(buf: []const u8, col_index: usize) RecordError!Value {
    const header_info = getVarint(buf) catch return RecordError.CorruptRecord;
    const header_len = header_info.value;

    if (header_len > buf.len) return RecordError.CorruptRecord;

    // Skip type codes until we reach the target column, tracking body offsets
    var hdr_pos: usize = header_info.bytes;
    var body_offset: usize = @intCast(header_len);
    var col: usize = 0;

    while (hdr_pos < header_len and col <= col_index) {
        const tc_info = getVarint(buf[hdr_pos..]) catch return RecordError.CorruptRecord;
        const st = tc_info.value;
        const len = serialTypeLen(st);

        if (col == col_index) {
            // Decode this column
            if (body_offset + len > buf.len) return RecordError.CorruptRecord;
            return switch (st) {
                0 => .{ .null_val = {} },
                1, 2, 3, 4, 5, 6 => .{ .integer = readIntBigEndian(buf[body_offset .. body_offset + len], len) },
                7 => blk: {
                    const bits = std.mem.readInt(u64, buf[body_offset..][0..8], .big);
                    break :blk .{ .real = @bitCast(bits) };
                },
                8 => .{ .integer = 0 },
                9 => .{ .integer = 1 },
                else => blk: {
                    if (st >= 12) {
                        const content = buf[body_offset .. body_offset + len];
                        if (st % 2 == 0) {
                            break :blk .{ .blob = content };
                        } else {
                            break :blk .{ .text = content };
                        }
                    }
                    break :blk .{ .null_val = {} };
                },
            };
        }

        body_offset += len;
        hdr_pos += tc_info.bytes;
        col += 1;
    }

    return RecordError.CorruptRecord;
}

// ═══════════════════════════════════════════════════════════════════════════
// Integer helpers (big-endian variable width)
// ═══════════════════════════════════════════════════════════════════════════

fn writeIntBigEndian(buf: []u8, value: i64, len: usize) void {
    const u: u64 = @bitCast(value);
    var i: usize = len;
    while (i > 0) {
        i -= 1;
        buf[i] = @intCast(u >> @intCast(8 * (len - 1 - i)) & 0xFF);
    }
}

fn readIntBigEndian(buf: []const u8, len: usize) i64 {
    var result: u64 = 0;
    for (0..len) |i| {
        result = (result << 8) | @as(u64, buf[i]);
    }
    // Sign extend
    const shift: u6 = @intCast(64 - len * 8);
    const signed: i64 = @bitCast(result << shift);
    return signed >> shift;
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

test "varint round-trip" {
    const test_values = [_]u64{ 0, 1, 127, 128, 255, 16383, 16384, 0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF };
    for (test_values) |v| {
        var buf: [9]u8 = undefined;
        const n = try putVarint(&buf, v);
        const result = try getVarint(buf[0..n]);
        try std.testing.expectEqual(v, result.value);
        try std.testing.expectEqual(n, result.bytes);
    }
}

test "record round-trip with all types" {
    const values = [_]Value{
        .{ .null_val = {} },
        .{ .integer = 42 },
        .{ .integer = -1000 },
        .{ .integer = 0 },
        .{ .integer = 1 },
        .{ .real = 3.14159 },
        .{ .text = "hello" },
        .{ .blob = &[_]u8{ 0xDE, 0xAD, 0xBE, 0xEF } },
    };

    var buf: [1024]u8 = undefined;
    const n = try serializeRecord(&values, &buf);
    try std.testing.expect(n > 0);

    const decoded = try deserializeRecord(buf[0..n], std.testing.allocator);
    defer std.testing.allocator.free(decoded);

    try std.testing.expectEqual(values.len, decoded.len);

    // NULL
    try std.testing.expect(decoded[0].isNull());
    // Integers
    try std.testing.expectEqual(@as(i64, 42), decoded[1].asInteger().?);
    try std.testing.expectEqual(@as(i64, -1000), decoded[2].asInteger().?);
    try std.testing.expectEqual(@as(i64, 0), decoded[3].asInteger().?);
    try std.testing.expectEqual(@as(i64, 1), decoded[4].asInteger().?);
    // Real
    try std.testing.expectEqual(@as(f64, 3.14159), decoded[5].asReal().?);
    // Text
    try std.testing.expectEqualStrings("hello", decoded[6].asText().?);
    // Blob
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xDE, 0xAD, 0xBE, 0xEF }, decoded[7].asBlob().?);
}

test "lazy column access" {
    const values = [_]Value{
        .{ .integer = 100 },
        .{ .text = "world" },
        .{ .real = 2.718 },
    };

    var buf: [256]u8 = undefined;
    const n = try serializeRecord(&values, &buf);

    // Access column 1 without deserializing everything
    const col1 = try readColumn(buf[0..n], 1);
    try std.testing.expectEqualStrings("world", col1.asText().?);

    // Access column 0
    const col0 = try readColumn(buf[0..n], 0);
    try std.testing.expectEqual(@as(i64, 100), col0.asInteger().?);

    // Access column 2
    const col2 = try readColumn(buf[0..n], 2);
    try std.testing.expectEqual(@as(f64, 2.718), col2.asReal().?);
}

test "serial type sizing" {
    try std.testing.expectEqual(@as(u64, 8), serialType(.{ .integer = 0 }));
    try std.testing.expectEqual(@as(u64, 9), serialType(.{ .integer = 1 }));
    try std.testing.expectEqual(@as(u64, 1), serialType(.{ .integer = 42 }));
    try std.testing.expectEqual(@as(u64, 2), serialType(.{ .integer = 1000 }));
    try std.testing.expectEqual(@as(u64, 3), serialType(.{ .integer = 100000 }));
    try std.testing.expectEqual(@as(u64, 0), serialType(.{ .null_val = {} }));
    try std.testing.expectEqual(@as(u64, 7), serialType(.{ .real = 1.0 }));
    // text "hi" → 13 + 2*2 = 17
    try std.testing.expectEqual(@as(u64, 17), serialType(.{ .text = "hi" }));
}
