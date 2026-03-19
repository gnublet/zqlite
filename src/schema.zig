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
