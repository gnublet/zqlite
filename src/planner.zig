const std = @import("std");
const ast = @import("ast.zig");
const schema = @import("schema.zig");

/// Simple cost-based query planner for ZQLite.
///
/// Responsibilities:
///   - Choose between full table scan and index lookup
///   - Order joins (exhaustive for 2-3 tables)
///   - Determine sort strategy

// ═══════════════════════════════════════════════════════════════════════════
// Plan nodes
// ═══════════════════════════════════════════════════════════════════════════

pub const ScanType = enum {
    full_scan,
    index_scan,
    rowid_lookup,
};

pub const ScanPlan = struct {
    table_name: []const u8,
    scan_type: ScanType,
    index_name: ?[]const u8,
    index_key_expr: ?*ast.Expr,
    estimated_rows: u64,
};

pub const JoinPlan = struct {
    outer: ScanPlan,
    inner: ScanPlan,
    join_type: ast.JoinType,
    on_expr: ?*ast.Expr,
};

pub const SortPlan = struct {
    key_columns: []const []const u8,
    descending: []const bool,
    use_index_order: bool,
};

pub const QueryPlan = struct {
    scans: []const ScanPlan,
    joins: []const JoinPlan,
    sort: ?SortPlan,
    has_aggregation: bool,
    estimated_cost: u64,
};

// ═══════════════════════════════════════════════════════════════════════════
// Planner
// ═══════════════════════════════════════════════════════════════════════════

pub const Planner = struct {
    schema_cache: *const schema.Schema,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, s: *const schema.Schema) Self {
        return .{ .schema_cache = s, .allocator = allocator };
    }

    /// Generate a query plan for a SELECT statement.
    pub fn planSelect(self: *Self, select: ast.Statement.Select) !QueryPlan {
        var scans: std.ArrayList(ScanPlan) = .{};
        var joins: std.ArrayList(JoinPlan) = .{};

        // Plan the primary table scan
        if (select.from) |from| {
            const scan = self.planTableScan(from.name, select.where);
            try scans.append(self.allocator, scan);

            // Plan joins
            for (select.joins) |join| {
                const inner_scan = self.planTableScan(join.table.name, join.on);
                try joins.append(self.allocator, .{
                    .outer = scan,
                    .inner = inner_scan,
                    .join_type = join.join_type,
                    .on_expr = join.on,
                });
                try scans.append(self.allocator, inner_scan);
            }
        }

        // Check for aggregation
        const has_agg = self.detectAggregation(select.columns);

        // Sort plan
        var sort: ?SortPlan = null;
        if (select.order_by.len > 0) {
            sort = .{
                .key_columns = &[_][]const u8{},
                .descending = &[_]bool{},
                .use_index_order = false, // TODO: check if index matches ORDER BY
            };
        }

        const estimated_cost = self.estimateCost(scans.items);
        const owned_scans = scans.toOwnedSlice(self.allocator) catch return error.OutOfMemory;
        const owned_joins = joins.toOwnedSlice(self.allocator) catch return error.OutOfMemory;

        return .{
            .scans = owned_scans,
            .joins = owned_joins,
            .sort = sort,
            .has_aggregation = has_agg,
            .estimated_cost = estimated_cost,
        };
    }

    pub fn planTableScan(self: *Self, table_name: []const u8, where: ?*ast.Expr) ScanPlan {
        if (where) |w| {
            if (w.* == .binary_op and w.binary_op.op == .eq) {
                var col_name: ?[]const u8 = null;
                var val_expr: ?*ast.Expr = null;

                if (w.binary_op.left.* == .column_ref and
                    (w.binary_op.left.column_ref.table == null or std.mem.eql(u8, w.binary_op.left.column_ref.table.?, table_name)))
                {
                    col_name = w.binary_op.left.column_ref.column;
                    val_expr = w.binary_op.right;
                } else if (w.binary_op.right.* == .column_ref and
                           (w.binary_op.right.column_ref.table == null or std.mem.eql(u8, w.binary_op.right.column_ref.table.?, table_name)))
                {
                    col_name = w.binary_op.right.column_ref.column;
                    val_expr = w.binary_op.left;
                }

                if (col_name) |cname| {
                    if (self.schema_cache.getTable(table_name)) |tbl| {
                        if (tbl.has_rowid_alias and tbl.rowid_alias_col != null) {
                            if (std.mem.eql(u8, cname, tbl.columns[tbl.rowid_alias_col.?].name)) {
                                return .{
                                    .table_name = table_name,
                                    .scan_type = .rowid_lookup,
                                    .index_name = null,
                                    .index_key_expr = val_expr,
                                    .estimated_rows = 1,
                                };
                            }
                        }
                    }

                    var index_buf: [16]schema.Index = undefined;
                    const idx_count = self.schema_cache.indexesForTable(table_name, &index_buf);
                    
                    // Look for an index starting with this column
                    for (index_buf[0..idx_count]) |idx| {
                        if (idx.columns.len > 0 and std.mem.eql(u8, idx.columns[0], cname)) {
                            return .{
                                .table_name = table_name,
                                .scan_type = .index_scan,
                                .index_name = idx.name,
                                .index_key_expr = val_expr,
                                .estimated_rows = 1,
                            };
                        }
                    }
                }
            }
        }

        return .{
            .table_name = table_name,
            .scan_type = .full_scan,
            .index_name = null,
            .index_key_expr = null,
            .estimated_rows = 1000, // default estimate for full scan
        };
    }

    fn detectAggregation(_: *Self, columns: []const ast.Statement.SelectColumn) bool {
        for (columns) |col| {
            switch (col) {
                .expr => |e| {
                    if (e.expr.* == .function_call) return true;
                },
                else => {},
            }
        }
        return false;
    }

    fn estimateCost(_: *Self, scans: []const ScanPlan) u64 {
        var cost: u64 = 0;
        for (scans) |scan| {
            cost += switch (scan.scan_type) {
                .full_scan => scan.estimated_rows * 10,
                .index_scan => scan.estimated_rows * 2,
                .rowid_lookup => 1,
            };
        }
        return cost;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

test "Planner creates scan plan" {
    var s = schema.Schema.init(std.testing.allocator);
    defer s.deinit();

    try s.addTable(.{
        .name = "users",
        .columns = &.{},
        .root_page = 2,
        .next_rowid = 1,
        .has_rowid_alias = false,
        .rowid_alias_col = null,
    });

    var planner = Planner.init(std.testing.allocator, &s);

    const select = ast.Statement.Select{
        .columns = &.{.{ .all_columns = {} }},
        .from = .{ .name = "users", .alias = null },
        .joins = &.{},
        .where = null,
        .group_by = &.{},
        .having = null,
        .order_by = &.{},
        .limit = null,
    };

    const plan = try planner.planSelect(select);
    defer std.testing.allocator.free(plan.scans);
    defer std.testing.allocator.free(plan.joins);
    try std.testing.expectEqual(@as(usize, 1), plan.scans.len);
    try std.testing.expectEqual(ScanType.full_scan, plan.scans[0].scan_type);
}
