const std = @import("std");
const parser = @import("parser.zig");
const ast = @import("ast.zig");

/// Statement cache — avoids re-parsing identical SQL strings.
///
/// Uses a simple hash-map cache with per-entry arena allocators.
/// Each cached entry owns its parsed AST via a dedicated arena so that
/// the AST nodes remain valid across calls.
///
/// Usage:
///   var cache = StmtCache.init(allocator, 64);
///   defer cache.deinit();
///   const stmt = cache.getOrParse(sql) orelse return error;

pub const StmtCache = struct {
    entries: std.StringHashMap(CacheEntry),
    allocator: std.mem.Allocator,
    max_entries: usize,
    hits: u64,
    misses: u64,

    const CacheEntry = struct {
        stmt: ast.Statement,
        arena: std.heap.ArenaAllocator,
    };

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, max_entries: usize) Self {
        return Self{
            .entries = std.StringHashMap(CacheEntry).init(allocator),
            .allocator = allocator,
            .max_entries = max_entries,
            .hits = 0,
            .misses = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        var it = self.entries.iterator();
        while (it.next()) |entry| {
            // Free the key (owned copy)
            self.allocator.free(entry.key_ptr.*);
            // Free the arena that holds the AST
            entry.value_ptr.arena.deinit();
        }
        self.entries.deinit();
    }

    /// Get a cached statement or parse and cache it.
    /// Returns null on parse error.
    pub fn getOrParse(self: *Self, sql: []const u8) ?ast.Statement {
        // Check cache
        if (self.entries.get(sql)) |entry| {
            self.hits += 1;
            return entry.stmt;
        }

        // Cache miss — parse
        self.misses += 1;

        // Create a dedicated arena for this cache entry's AST
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();

        var p = parser.Parser.init(sql, arena.allocator());
        const stmt = p.parseStatement() catch {
            arena.deinit();
            return null;
        };

        // Evict if at capacity (simple: clear all — good enough for benchmarks)
        if (self.entries.count() >= self.max_entries) {
            self.evictAll();
        }

        // Store owned copy of key
        const owned_key = self.allocator.dupe(u8, sql) catch {
            arena.deinit();
            return null;
        };

        self.entries.put(owned_key, .{
            .stmt = stmt,
            .arena = arena,
        }) catch {
            self.allocator.free(owned_key);
            arena.deinit();
            return null;
        };

        return stmt;
    }

    fn evictAll(self: *Self) void {
        var it = self.entries.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.arena.deinit();
        }
        self.entries.clearRetainingCapacity();
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

test "stmt cache hit" {
    var cache = StmtCache.init(std.testing.allocator, 16);
    defer cache.deinit();

    const stmt1 = cache.getOrParse("SELECT * FROM t;");
    try std.testing.expect(stmt1 != null);
    try std.testing.expectEqual(@as(u64, 0), cache.hits);
    try std.testing.expectEqual(@as(u64, 1), cache.misses);

    const stmt2 = cache.getOrParse("SELECT * FROM t;");
    try std.testing.expect(stmt2 != null);
    try std.testing.expectEqual(@as(u64, 1), cache.hits);
    try std.testing.expectEqual(@as(u64, 1), cache.misses);
}

test "stmt cache different queries" {
    var cache = StmtCache.init(std.testing.allocator, 16);
    defer cache.deinit();

    _ = cache.getOrParse("SELECT * FROM t;");
    _ = cache.getOrParse("INSERT INTO t VALUES (1, 'a');");
    try std.testing.expectEqual(@as(u64, 0), cache.hits);
    try std.testing.expectEqual(@as(u64, 2), cache.misses);
    try std.testing.expectEqual(@as(usize, 2), cache.entries.count());
}
