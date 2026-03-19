const std = @import("std");

/// Schema management for ZQLite.
///
/// Stores table and index metadata, mirroring SQLite's sqlite_schema table.

// ═══════════════════════════════════════════════════════════════════════════
// Column type affinity
// ═══════════════════════════════════════════════════════════════════════════

pub const TypeAffinity = enum {
    integer,
    text,
    blob,
    real,
    numeric,
};

// ═══════════════════════════════════════════════════════════════════════════
// Column definition
// ═══════════════════════════════════════════════════════════════════════════

pub const Column = struct {
    name: []const u8,
    affinity: TypeAffinity,
    not_null: bool,
    is_primary_key: bool,
    default_value: ?[]const u8,
};

// ═══════════════════════════════════════════════════════════════════════════
// Table definition
// ═══════════════════════════════════════════════════════════════════════════

pub const Table = struct {
    name: []const u8,
    columns: []const Column,
    root_page: u32,
    next_rowid: i64,
    has_rowid_alias: bool, // e.g. INTEGER PRIMARY KEY is the rowid
    rowid_alias_col: ?usize, // index of the column that aliases rowid
};

// ═══════════════════════════════════════════════════════════════════════════
// Index definition
// ═══════════════════════════════════════════════════════════════════════════

pub const Index = struct {
    name: []const u8,
    table_name: []const u8,
    columns: []const []const u8,
    root_page: u32,
    is_unique: bool,
};

// ═══════════════════════════════════════════════════════════════════════════
// Schema — in-memory schema cache
// ═══════════════════════════════════════════════════════════════════════════

pub const Schema = struct {
    tables: std.StringHashMap(Table),
    indexes: std.StringHashMap(Index),
    allocator: std.mem.Allocator,

    const Self = @This();

    // Schema page format constants
    const SCHEMA_MAGIC = "ZQDB";
    const SCHEMA_VERSION: u32 = 1;
    const SCHEMA_HEADER_SIZE: usize = 12; // magic(4) + version(4) + table_count(4)

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .tables = std.StringHashMap(Table).init(allocator),
            .indexes = std.StringHashMap(Index).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.tables.deinit();
        self.indexes.deinit();
    }

    pub fn addTable(self: *Self, table: Table) !void {
        try self.tables.put(table.name, table);
    }

    pub fn getTable(self: *const Self, name: []const u8) ?Table {
        return self.tables.get(name);
    }

    pub fn dropTable(self: *Self, name: []const u8) bool {
        return self.tables.remove(name);
    }

    pub fn addIndex(self: *Self, index: Index) !void {
        try self.indexes.put(index.name, index);
    }

    pub fn getIndex(self: *const Self, name: []const u8) ?Index {
        return self.indexes.get(name);
    }

    pub fn dropIndex(self: *Self, name: []const u8) bool {
        return self.indexes.remove(name);
    }

    /// Find indexes that cover a given table.
    pub fn indexesForTable(self: *const Self, table_name: []const u8, buf: []Index) usize {
        var count: usize = 0;
        var it = self.indexes.iterator();
        while (it.next()) |entry| {
            if (std.mem.eql(u8, entry.value_ptr.table_name, table_name)) {
                if (count < buf.len) {
                    buf[count] = entry.value_ptr.*;
                    count += 1;
                }
            }
        }
        return count;
    }

    // ─── Schema persistence (page 0) ────────────────────────────────

    /// Save schema to page 0 of the database file.
    ///
    /// Format:
    ///   [Header: 12 bytes]
    ///     magic "ZQDB" (4), version u32 (4), table_count u32 (4)
    ///   [Per table:]
    ///     name_len u16, name bytes, root_page u32, next_rowid i64,
    ///     col_count u16, [per col: name_len u16, name bytes, affinity u8, flags u8]
    pub fn saveSchema(self: *const Self, page_buf: []u8) void {
        var pos: usize = 0;

        // Header
        @memcpy(page_buf[pos..][0..4], SCHEMA_MAGIC);
        pos += 4;
        std.mem.writeInt(u32, page_buf[pos..][0..4], SCHEMA_VERSION, .little);
        pos += 4;

        // Count tables
        var table_count: u32 = 0;
        var it = self.tables.iterator();
        while (it.next()) |_| table_count += 1;
        std.mem.writeInt(u32, page_buf[pos..][0..4], table_count, .little);
        pos += 4;

        // Serialize each table
        var it2 = self.tables.iterator();
        while (it2.next()) |entry| {
            const t = entry.value_ptr.*;
            if (pos + 2 + t.name.len + 4 + 8 + 2 >= page_buf.len) break; // overflow guard

            // Table name
            std.mem.writeInt(u16, page_buf[pos..][0..2], @intCast(t.name.len), .little);
            pos += 2;
            @memcpy(page_buf[pos..][0..t.name.len], t.name);
            pos += t.name.len;

            // Root page + next_rowid
            std.mem.writeInt(u32, page_buf[pos..][0..4], t.root_page, .little);
            pos += 4;
            std.mem.writeInt(i64, page_buf[pos..][0..8], t.next_rowid, .little);
            pos += 8;

            // has_rowid_alias + rowid_alias_col
            page_buf[pos] = if (t.has_rowid_alias) 1 else 0;
            pos += 1;
            page_buf[pos] = if (t.rowid_alias_col) |c| @intCast(c) else 0xFF;
            pos += 1;

            // Columns
            std.mem.writeInt(u16, page_buf[pos..][0..2], @intCast(t.columns.len), .little);
            pos += 2;

            for (t.columns) |col| {
                if (pos + 2 + col.name.len + 2 >= page_buf.len) break;

                std.mem.writeInt(u16, page_buf[pos..][0..2], @intCast(col.name.len), .little);
                pos += 2;
                @memcpy(page_buf[pos..][0..col.name.len], col.name);
                pos += col.name.len;

                page_buf[pos] = @intFromEnum(col.affinity);
                pos += 1;

                var flags: u8 = 0;
                if (col.not_null) flags |= 0x01;
                if (col.is_primary_key) flags |= 0x02;
                page_buf[pos] = flags;
                pos += 1;
            }
        }

        // Zero-fill remainder
        @memset(page_buf[pos..], 0);
    }

    /// Load schema from a page 0 buffer. Returns false if page is not a valid schema.
    pub fn loadSchema(self: *Self, page_buf: []const u8) bool {
        if (page_buf.len < SCHEMA_HEADER_SIZE) return false;

        // Validate magic
        if (!std.mem.eql(u8, page_buf[0..4], SCHEMA_MAGIC)) return false;

        const version = std.mem.readInt(u32, page_buf[4..8], .little);
        if (version != SCHEMA_VERSION) return false;

        const table_count = std.mem.readInt(u32, page_buf[8..12], .little);

        var pos: usize = SCHEMA_HEADER_SIZE;

        var i: u32 = 0;
        while (i < table_count) : (i += 1) {
            if (pos + 2 > page_buf.len) return false;

            // Table name
            const name_len = std.mem.readInt(u16, page_buf[pos..][0..2], .little);
            pos += 2;
            if (pos + name_len > page_buf.len) return false;
            const name = self.allocator.dupe(u8, page_buf[pos..][0..name_len]) catch return false;
            pos += name_len;

            if (pos + 14 > page_buf.len) return false;

            // Root page + next_rowid
            const root_page = std.mem.readInt(u32, page_buf[pos..][0..4], .little);
            pos += 4;
            const next_rowid = std.mem.readInt(i64, page_buf[pos..][0..8], .little);
            pos += 8;

            // has_rowid_alias + rowid_alias_col
            const has_rowid_alias = page_buf[pos] == 1;
            pos += 1;
            const alias_byte = page_buf[pos];
            const rowid_alias_col: ?usize = if (alias_byte == 0xFF) null else @intCast(alias_byte);
            pos += 1;

            // Columns
            const col_count = std.mem.readInt(u16, page_buf[pos..][0..2], .little);
            pos += 2;

            const cols = self.allocator.alloc(Column, col_count) catch return false;

            var ci: u16 = 0;
            while (ci < col_count) : (ci += 1) {
                if (pos + 2 > page_buf.len) return false;
                const cname_len = std.mem.readInt(u16, page_buf[pos..][0..2], .little);
                pos += 2;
                if (pos + cname_len + 2 > page_buf.len) return false;
                const cname = self.allocator.dupe(u8, page_buf[pos..][0..cname_len]) catch return false;
                pos += cname_len;

                const affinity: TypeAffinity = @enumFromInt(page_buf[pos]);
                pos += 1;
                const flags = page_buf[pos];
                pos += 1;

                cols[ci] = .{
                    .name = cname,
                    .affinity = affinity,
                    .not_null = (flags & 0x01) != 0,
                    .is_primary_key = (flags & 0x02) != 0,
                    .default_value = null,
                };
            }

            self.addTable(.{
                .name = name,
                .columns = cols,
                .root_page = root_page,
                .next_rowid = next_rowid,
                .has_rowid_alias = has_rowid_alias,
                .rowid_alias_col = rowid_alias_col,
            }) catch return false;
        }

        return true;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

test "Schema add and get table" {
    var s = Schema.init(std.testing.allocator);
    defer s.deinit();

    const cols = [_]Column{
        .{ .name = "id", .affinity = .integer, .not_null = true, .is_primary_key = true, .default_value = null },
        .{ .name = "name", .affinity = .text, .not_null = false, .is_primary_key = false, .default_value = null },
    };

    try s.addTable(.{
        .name = "users",
        .columns = &cols,
        .root_page = 2,
        .next_rowid = 1,
        .has_rowid_alias = true,
        .rowid_alias_col = 0,
    });

    const t = s.getTable("users");
    try std.testing.expect(t != null);
    try std.testing.expectEqualStrings("users", t.?.name);
    try std.testing.expectEqual(@as(usize, 2), t.?.columns.len);
}

test "Schema drop table" {
    var s = Schema.init(std.testing.allocator);
    defer s.deinit();

    try s.addTable(.{
        .name = "temp",
        .columns = &.{},
        .root_page = 3,
        .next_rowid = 1,
        .has_rowid_alias = false,
        .rowid_alias_col = null,
    });

    try std.testing.expect(s.dropTable("temp"));
    try std.testing.expect(s.getTable("temp") == null);
}
