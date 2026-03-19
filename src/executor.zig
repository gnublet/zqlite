const std = @import("std");
const ast = @import("ast.zig");
const btree = @import("btree.zig");
const cursor = @import("cursor.zig");
const pager = @import("pager.zig");
const record = @import("record.zig");
const schema = @import("schema.zig");

/// SQL Executor — interprets parsed AST directly against the storage engine.
///
/// Handles CREATE TABLE, INSERT, SELECT FROM, DELETE, DROP TABLE by driving
/// the B-tree, pager, and schema modules.

// ═══════════════════════════════════════════════════════════════════════════
// Errors
// ═══════════════════════════════════════════════════════════════════════════

pub const ExecError = error{
    TableNotFound,
    TableAlreadyExists,
    ColumnCountMismatch,
    SerializationFailed,
    StorageError,
    TypeError,
    ColumnNotFound,
    UnsupportedExpr,
    UnsupportedStatement,
    BufferFull,
};

// ═══════════════════════════════════════════════════════════════════════════
// Result row
// ═══════════════════════════════════════════════════════════════════════════

pub const ResultRow = struct {
    values: []Value,
};

pub const Value = union(enum) {
    null_val: void,
    integer: i64,
    real: f64,
    text: []const u8,
};

// ═══════════════════════════════════════════════════════════════════════════
// Executor output
// ═══════════════════════════════════════════════════════════════════════════

pub const ExecResult = struct {
    rows: []ResultRow,
    column_names: [][]const u8,
    rows_affected: usize,
    message: ?[]const u8,
};

// ═══════════════════════════════════════════════════════════════════════════
// Executor
// ═══════════════════════════════════════════════════════════════════════════

pub const Executor = struct {
    pool: *pager.BufferPool,
    schema_store: *schema.Schema,
    allocator: std.mem.Allocator,
    next_page: u32,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, pool: *pager.BufferPool, schema_store: *schema.Schema) Self {
        return Self{
            .pool = pool,
            .schema_store = schema_store,
            .allocator = allocator,
            .next_page = 1, // page 0 is reserved
        };
    }

    /// Execute a parsed statement.
    pub fn execute(self: *Self, stmt: ast.Statement, arena: std.mem.Allocator) ExecError!ExecResult {
        return switch (stmt) {
            .create_table => |ct| self.execCreateTable(ct, arena),
            .drop_table => |dt| self.execDropTable(dt, arena),
            .insert => |ins| self.execInsert(ins, arena),
            .select => |sel| self.execSelect(sel, arena),
            .delete => |del| self.execDelete(del, arena),
            .update => |upd| self.execUpdate(upd, arena),
            else => ExecError.UnsupportedStatement,
        };
    }

    // ─── CREATE TABLE ────────────────────────────────────────────────

    fn execCreateTable(self: *Self, ct: ast.Statement.CreateTable, _: std.mem.Allocator) ExecError!ExecResult {
        // Check if table already exists
        if (self.schema_store.getTable(ct.name) != null) {
            if (ct.if_not_exists) {
                return ExecResult{
                    .rows = &.{},
                    .column_names = &.{},
                    .rows_affected = 0,
                    .message = "table already exists, skipped",
                };
            }
            return ExecError.TableAlreadyExists;
        }

        // Create a B-tree for this table
        const bt = btree.Btree.create(self.pool, btree.PAGE_TYPE_TABLE_LEAF) catch
            return ExecError.StorageError;

        // Copy table name into persistent allocator (arena will be freed after this statement)
        const owned_name = self.allocator.dupe(u8, ct.name) catch return ExecError.StorageError;

        // Build column definitions for schema — also copy column names
        const cols = self.allocator.alloc(schema.Column, ct.columns.len) catch return ExecError.StorageError;

        var pk_col: ?usize = null;

        for (ct.columns, 0..) |col_def, i| {
            const affinity = inferAffinity(col_def.type_name);
            const owned_col_name = self.allocator.dupe(u8, col_def.name) catch return ExecError.StorageError;
            cols[i] = .{
                .name = owned_col_name,
                .affinity = affinity,
                .not_null = col_def.not_null,
                .is_primary_key = col_def.primary_key,
                .default_value = null,
            };
            if (col_def.primary_key and affinity == .integer) {
                pk_col = i;
            }
        }

        // Register in schema
        self.schema_store.addTable(.{
            .name = owned_name,
            .columns = cols,
            .root_page = bt.root_page_id,
            .next_rowid = 1,
            .has_rowid_alias = pk_col != null,
            .rowid_alias_col = pk_col,
        }) catch return ExecError.StorageError;

        return ExecResult{
            .rows = &.{},
            .column_names = &.{},
            .rows_affected = 0,
            .message = null,
        };
    }

    // ─── DROP TABLE ──────────────────────────────────────────────────

    fn execDropTable(self: *Self, dt: ast.Statement.DropTable, _: std.mem.Allocator) ExecError!ExecResult {
        if (!self.schema_store.dropTable(dt.name)) {
            if (dt.if_exists) {
                return ExecResult{
                    .rows = &.{},
                    .column_names = &.{},
                    .rows_affected = 0,
                    .message = "table does not exist, skipped",
                };
            }
            return ExecError.TableNotFound;
        }
        return ExecResult{
            .rows = &.{},
            .column_names = &.{},
            .rows_affected = 0,
            .message = null,
        };
    }

    // ─── INSERT ──────────────────────────────────────────────────────

    fn execInsert(self: *Self, ins: ast.Statement.Insert, _: std.mem.Allocator) ExecError!ExecResult {
        const table_entry = self.schema_store.getTable(ins.table) orelse
            return ExecError.TableNotFound;

        var bt = btree.Btree.open(self.pool, table_entry.root_page);

        var rows_inserted: usize = 0;

        for (ins.values) |value_row| {
            // Check column count
            if (value_row.len != table_entry.columns.len) {
                return ExecError.ColumnCountMismatch;
            }

            // Evaluate expressions to record values
            var rec_values: [64]record.Value = undefined;
            for (value_row, 0..) |expr, i| {
                rec_values[i] = evalToRecordValue(expr) catch return ExecError.TypeError;
            }

            // Serialize record
            var buf: [4096]u8 = undefined;
            const rec_size = record.serializeRecord(rec_values[0..value_row.len], &buf) catch
                return ExecError.SerializationFailed;

            // Determine rowid
            var rowid: i64 = undefined;
            if (table_entry.rowid_alias_col) |pk_idx| {
                // Use the PRIMARY KEY column value as rowid
                rowid = switch (rec_values[pk_idx]) {
                    .integer => |v| v,
                    else => return ExecError.TypeError,
                };
            } else {
                // Auto-increment
                rowid = table_entry.next_rowid;
            }

            // Insert into B-tree
            bt.insert(rowid, buf[0..rec_size]) catch return ExecError.StorageError;

            // Update next_rowid in schema
            var updated = self.schema_store.getTable(ins.table) orelse
                return ExecError.TableNotFound;
            updated.next_rowid = @max(updated.next_rowid, rowid + 1);
            self.schema_store.addTable(updated) catch return ExecError.StorageError;

            rows_inserted += 1;
        }

        return ExecResult{
            .rows = &.{},
            .column_names = &.{},
            .rows_affected = rows_inserted,
            .message = null,
        };
    }

    // ─── SELECT ──────────────────────────────────────────────────────

    fn execSelect(self: *Self, sel: ast.Statement.Select, arena: std.mem.Allocator) ExecError!ExecResult {
        const from = sel.from orelse return ExecError.UnsupportedExpr;
        const table_entry = self.schema_store.getTable(from.name) orelse
            return ExecError.TableNotFound;

        var bt = btree.Btree.open(self.pool, table_entry.root_page);

        // ── Fast path: PRIMARY KEY point lookup ──────────────────────
        // Detect WHERE pk_col = integer_literal and use bt.search() (O(log n))
        // instead of full cursor scan (O(n)).
        if (sel.where) |where_expr| {
            if (table_entry.rowid_alias_col) |pk_idx| {
                if (tryExtractPkLookup(where_expr, table_entry.columns, pk_idx)) |target_rowid| {
                    return self.execPkLookup(&bt, sel, table_entry, target_rowid, arena);
                }
            }
        }

        // ── Slow path: full cursor scan ──────────────────────────────
        var cur = cursor.Cursor.init(&bt);
        cur.first() catch return ExecError.StorageError;

        // Determine which columns to output
        var col_names_list: std.ArrayList([]const u8) = .{};
        var col_indices: std.ArrayList(?usize) = .{};
        var is_star = false;

        for (sel.columns) |col| {
            switch (col) {
                .all_columns => {
                    is_star = true;
                    for (table_entry.columns) |cc| {
                        col_names_list.append(arena, cc.name) catch return ExecError.StorageError;
                    }
                },
                .expr => |ec| {
                    const name = ec.alias orelse exprName(ec.expr);
                    col_names_list.append(arena, name) catch return ExecError.StorageError;
                    const idx = resolveColumnIndex(ec.expr, table_entry.columns);
                    col_indices.append(arena, idx) catch return ExecError.StorageError;
                },
                .table_all => {
                    is_star = true;
                    for (table_entry.columns) |cc| {
                        col_names_list.append(arena, cc.name) catch return ExecError.StorageError;
                    }
                },
            }
        }

        // Scan rows
        var rows_list: std.ArrayList(ResultRow) = .{};

        while (cur.valid) {
            const entry = cur.cell() catch return ExecError.StorageError;

            // Deserialize the record
            const decoded = record.deserializeRecord(entry.payload, arena) catch
                return ExecError.SerializationFailed;

            // Evaluate WHERE clause
            if (sel.where) |where_expr| {
                const matches = evalWhere(where_expr, table_entry.columns, decoded) catch false;
                if (!matches) {
                    _ = cur.next() catch return ExecError.StorageError;
                    continue;
                }
            }

            // Build result row
            if (is_star) {
                const vals = arena.alloc(Value, table_entry.columns.len) catch return ExecError.StorageError;
                for (0..table_entry.columns.len) |i| {
                    if (i < decoded.len) {
                        vals[i] = recordToValue(decoded[i]);
                    } else {
                        vals[i] = .{ .null_val = {} };
                    }
                }
                rows_list.append(arena, .{ .values = vals }) catch return ExecError.StorageError;
            } else {
                const vals = arena.alloc(Value, col_indices.items.len) catch return ExecError.StorageError;
                for (col_indices.items, 0..) |maybe_idx, i| {
                    if (maybe_idx) |idx| {
                        if (idx < decoded.len) {
                            vals[i] = recordToValue(decoded[idx]);
                        } else {
                            vals[i] = .{ .null_val = {} };
                        }
                    } else {
                        vals[i] = .{ .null_val = {} };
                    }
                }
                rows_list.append(arena, .{ .values = vals }) catch return ExecError.StorageError;
            }

            _ = cur.next() catch return ExecError.StorageError;
        }

        return ExecResult{
            .rows = rows_list.items,
            .column_names = col_names_list.items,
            .rows_affected = 0,
            .message = null,
        };
    }

    /// Fast path: direct B-tree search for a single rowid.
    fn execPkLookup(
        self: *Self,
        bt: *btree.Btree,
        sel: ast.Statement.Select,
        table_entry: schema.Table,
        target_rowid: i64,
        arena: std.mem.Allocator,
    ) ExecError!ExecResult {
        _ = self;
        const cell_result = bt.search(target_rowid) catch return ExecError.StorageError;

        // Build column names
        var col_names_list: std.ArrayList([]const u8) = .{};
        var is_star = false;
        var col_indices: std.ArrayList(?usize) = .{};

        for (sel.columns) |col| {
            switch (col) {
                .all_columns, .table_all => {
                    is_star = true;
                    for (table_entry.columns) |cc| {
                        col_names_list.append(arena, cc.name) catch return ExecError.StorageError;
                    }
                },
                .expr => |ec| {
                    col_names_list.append(arena, ec.alias orelse exprName(ec.expr)) catch return ExecError.StorageError;
                    col_indices.append(arena, resolveColumnIndex(ec.expr, table_entry.columns)) catch return ExecError.StorageError;
                },
            }
        }

        var rows_list: std.ArrayList(ResultRow) = .{};

        if (cell_result) |entry| {
            const decoded = record.deserializeRecord(entry.payload, arena) catch
                return ExecError.SerializationFailed;

            if (is_star) {
                const vals = arena.alloc(Value, table_entry.columns.len) catch return ExecError.StorageError;
                for (0..table_entry.columns.len) |i| {
                    vals[i] = if (i < decoded.len) recordToValue(decoded[i]) else .{ .null_val = {} };
                }
                rows_list.append(arena, .{ .values = vals }) catch return ExecError.StorageError;
            } else {
                const vals = arena.alloc(Value, col_indices.items.len) catch return ExecError.StorageError;
                for (col_indices.items, 0..) |maybe_idx, i| {
                    if (maybe_idx) |idx| {
                        vals[i] = if (idx < decoded.len) recordToValue(decoded[idx]) else .{ .null_val = {} };
                    } else {
                        vals[i] = .{ .null_val = {} };
                    }
                }
                rows_list.append(arena, .{ .values = vals }) catch return ExecError.StorageError;
            }
        }

        return ExecResult{
            .rows = rows_list.items,
            .column_names = col_names_list.items,
            .rows_affected = 0,
            .message = null,
        };
    }

    // ─── DELETE ──────────────────────────────────────────────────────

    fn execDelete(self: *Self, del: ast.Statement.Delete, arena: std.mem.Allocator) ExecError!ExecResult {
        const table_entry = self.schema_store.getTable(del.table) orelse
            return ExecError.TableNotFound;

        var bt = btree.Btree.open(self.pool, table_entry.root_page);

        // ── Fast path: DELETE WHERE pk = literal ──────────────────────
        if (del.where) |where_expr| {
            if (table_entry.rowid_alias_col) |pk_idx| {
                if (tryExtractPkLookup(where_expr, table_entry.columns, pk_idx)) |target_rowid| {
                    const ok = bt.delete(target_rowid) catch return ExecError.StorageError;
                    return ExecResult{
                        .rows = &.{},
                        .column_names = &.{},
                        .rows_affected = if (ok) 1 else 0,
                        .message = null,
                    };
                }
            }
        }

        // ── Slow path: cursor scan ───────────────────────────────────
        var cur = cursor.Cursor.init(&bt);
        cur.first() catch return ExecError.StorageError;

        // Collect rowids to delete (can't delete during iteration)
        var to_delete: std.ArrayList(i64) = .{};

        while (cur.valid) {
            const k = cur.key() catch return ExecError.StorageError;

            if (del.where) |where_expr| {
                const entry = cur.cell() catch return ExecError.StorageError;
                const decoded = record.deserializeRecord(entry.payload, arena) catch
                    return ExecError.SerializationFailed;

                const matches = evalWhere(where_expr, table_entry.columns, decoded) catch false;
                if (matches) {
                    to_delete.append(arena, k) catch return ExecError.StorageError;
                }
            } else {
                // No WHERE = delete all
                to_delete.append(arena, k) catch return ExecError.StorageError;
            }

            _ = cur.next() catch return ExecError.StorageError;
        }

        // Now delete collected rowids
        var deleted: usize = 0;
        for (to_delete.items) |rowid| {
            const ok = bt.delete(rowid) catch return ExecError.StorageError;
            if (ok) deleted += 1;
        }

        return ExecResult{
            .rows = &.{},
            .column_names = &.{},
            .rows_affected = deleted,
            .message = null,
        };
    }

    // ─── UPDATE ──────────────────────────────────────────────────────

    fn execUpdate(self: *Self, upd: ast.Statement.Update, arena: std.mem.Allocator) ExecError!ExecResult {
        const table_entry = self.schema_store.getTable(upd.table) orelse
            return ExecError.TableNotFound;

        var bt = btree.Btree.open(self.pool, table_entry.root_page);

        // ── Fast path: UPDATE WHERE pk = literal ─────────────────────
        if (upd.where) |where_expr| {
            if (table_entry.rowid_alias_col) |pk_idx| {
                if (tryExtractPkLookup(where_expr, table_entry.columns, pk_idx)) |target_rowid| {
                    const cell_result = bt.search(target_rowid) catch return ExecError.StorageError;
                    if (cell_result) |entry| {
                        const decoded = record.deserializeRecord(entry.payload, arena) catch
                            return ExecError.SerializationFailed;
                        const new_rec = applyAssignments(decoded, upd.assignments, table_entry.columns, arena) catch
                            return ExecError.TypeError;
                        var buf: [4096]u8 = undefined;
                        const rec_size = record.serializeRecord(new_rec, &buf) catch
                            return ExecError.SerializationFailed;
                        // Delete old and insert new
                        _ = bt.delete(target_rowid) catch return ExecError.StorageError;
                        bt.insert(target_rowid, buf[0..rec_size]) catch return ExecError.StorageError;
                        return ExecResult{ .rows = &.{}, .column_names = &.{}, .rows_affected = 1, .message = null };
                    }
                    return ExecResult{ .rows = &.{}, .column_names = &.{}, .rows_affected = 0, .message = null };
                }
            }
        }

        // ── Slow path: cursor scan ───────────────────────────────────
        var cur = cursor.Cursor.init(&bt);
        cur.first() catch return ExecError.StorageError;

        const UpdateEntry = struct { rowid: i64, new_payload: []const u8 };
        var updates: std.ArrayList(UpdateEntry) = .{};

        while (cur.valid) {
            const k = cur.key() catch return ExecError.StorageError;
            const entry = cur.cell() catch return ExecError.StorageError;
            const decoded = record.deserializeRecord(entry.payload, arena) catch
                return ExecError.SerializationFailed;

            var should_update = true;
            if (upd.where) |where_expr| {
                should_update = evalWhere(where_expr, table_entry.columns, decoded) catch false;
            }

            if (should_update) {
                const new_rec = applyAssignments(decoded, upd.assignments, table_entry.columns, arena) catch
                    return ExecError.TypeError;
                var buf: [4096]u8 = undefined;
                const rec_size = record.serializeRecord(new_rec, &buf) catch
                    return ExecError.SerializationFailed;
                const owned = arena.dupe(u8, buf[0..rec_size]) catch return ExecError.StorageError;
                updates.append(arena, .{ .rowid = k, .new_payload = owned }) catch return ExecError.StorageError;
            }

            _ = cur.next() catch return ExecError.StorageError;
        }

        // Apply updates (delete + reinsert)
        var updated: usize = 0;
        for (updates.items) |u| {
            _ = bt.delete(u.rowid) catch return ExecError.StorageError;
            bt.insert(u.rowid, u.new_payload) catch return ExecError.StorageError;
            updated += 1;
        }

        return ExecResult{ .rows = &.{}, .column_names = &.{}, .rows_affected = updated, .message = null };
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

fn inferAffinity(type_name: ?[]const u8) schema.TypeAffinity {
    const tn = type_name orelse return .blob;
    if (std.ascii.eqlIgnoreCase(tn, "INTEGER") or std.ascii.eqlIgnoreCase(tn, "INT"))
        return .integer;
    if (std.ascii.eqlIgnoreCase(tn, "TEXT") or std.ascii.eqlIgnoreCase(tn, "VARCHAR"))
        return .text;
    if (std.ascii.eqlIgnoreCase(tn, "REAL") or std.ascii.eqlIgnoreCase(tn, "FLOAT") or std.ascii.eqlIgnoreCase(tn, "DOUBLE"))
        return .real;
    if (std.ascii.eqlIgnoreCase(tn, "BLOB"))
        return .blob;
    return .numeric;
}
/// Apply SET assignments to an existing decoded record, producing new record values.
fn applyAssignments(
    decoded: []const record.Value,
    assignments: []const ast.Statement.Assignment,
    columns: []const schema.Column,
    arena: std.mem.Allocator,
) ![]const record.Value {
    const new_vals = try arena.alloc(record.Value, columns.len);

    // Start with existing values
    for (0..columns.len) |i| {
        new_vals[i] = if (i < decoded.len) decoded[i] else .{ .null_val = {} };
    }

    // Apply each assignment
    for (assignments) |assign| {
        var found = false;
        for (columns, 0..) |col, i| {
            if (std.ascii.eqlIgnoreCase(col.name, assign.column)) {
                new_vals[i] = try evalToRecordValue(assign.value);
                found = true;
                break;
            }
        }
        if (!found) return error.TypeError;
    }

    return new_vals;
}

fn evalToRecordValue(expr: *ast.Expr) !record.Value {
    return switch (expr.*) {
        .integer_literal => |v| .{ .integer = v },
        .real_literal => |v| .{ .real = v },
        .string_literal => |v| .{ .text = v },
        .null_literal => .{ .null_val = {} },
        .unary_op => |u| {
            if (u.op == .negate) {
                const inner = try evalToRecordValue(u.operand);
                return switch (inner) {
                    .integer => |v| record.Value{ .integer = -v },
                    .real => |v| record.Value{ .real = -v },
                    else => error.TypeError,
                };
            }
            return error.TypeError;
        },
        else => error.TypeError,
    };
}

/// Try to extract a primary key point lookup from a WHERE expression.
/// Returns the target rowid if the WHERE is `pk_col = integer_literal`
/// (or `integer_literal = pk_col`).
fn tryExtractPkLookup(
    expr: *ast.Expr,
    columns: []const schema.Column,
    pk_idx: usize,
) ?i64 {
    switch (expr.*) {
        .binary_op => |op| {
            if (op.op != .eq) return null;

            // Check: pk_col = literal
            if (isColumnAtIndex(op.left, columns, pk_idx)) {
                return extractIntegerLiteral(op.right);
            }
            // Check: literal = pk_col
            if (isColumnAtIndex(op.right, columns, pk_idx)) {
                return extractIntegerLiteral(op.left);
            }
            return null;
        },
        .paren => |inner| return tryExtractPkLookup(inner, columns, pk_idx),
        else => return null,
    }
}

fn isColumnAtIndex(expr: *ast.Expr, columns: []const schema.Column, target_idx: usize) bool {
    switch (expr.*) {
        .column_ref => |ref| {
            for (columns, 0..) |col, i| {
                if (i == target_idx and std.mem.eql(u8, col.name, ref.column)) return true;
            }
            return false;
        },
        else => return false,
    }
}

fn extractIntegerLiteral(expr: *ast.Expr) ?i64 {
    return switch (expr.*) {
        .integer_literal => |v| v,
        .unary_op => |u| {
            if (u.op == .negate) {
                const inner = extractIntegerLiteral(u.operand) orelse return null;
                return -inner;
            }
            return null;
        },
        .paren => |inner| extractIntegerLiteral(inner),
        else => null,
    };
}

fn recordToValue(rv: record.Value) Value {
    return switch (rv) {
        .integer => |v| .{ .integer = v },
        .real => |v| .{ .real = v },
        .text => |v| .{ .text = v },
        .null_val => .{ .null_val = {} },
        .blob => .{ .null_val = {} }, // simplify blobs as null for display
    };
}

fn resolveColumnIndex(expr: *ast.Expr, columns: []const schema.Column) ?usize {
    switch (expr.*) {
        .column_ref => |ref| {
            for (columns, 0..) |col, i| {
                if (std.mem.eql(u8, col.name, ref.column)) return i;
            }
            return null;
        },
        else => return null,
    }
}

fn exprName(expr: *ast.Expr) []const u8 {
    return switch (expr.*) {
        .column_ref => |ref| ref.column,
        else => "?column?",
    };
}

fn evalWhere(expr: *ast.Expr, columns: []const schema.Column, row: []const record.Value) !bool {
    switch (expr.*) {
        .binary_op => |op| {
            switch (op.op) {
                .eq => {
                    const left = try resolveValue(op.left, columns, row);
                    const right = try resolveValue(op.right, columns, row);
                    return valuesEqual(left, right);
                },
                .ne => {
                    const left = try resolveValue(op.left, columns, row);
                    const right = try resolveValue(op.right, columns, row);
                    return !valuesEqual(left, right);
                },
                .lt => {
                    const left = try resolveValue(op.left, columns, row);
                    const right = try resolveValue(op.right, columns, row);
                    return valueLess(left, right);
                },
                .gt => {
                    const left = try resolveValue(op.left, columns, row);
                    const right = try resolveValue(op.right, columns, row);
                    return valueLess(right, left);
                },
                .le => {
                    const left = try resolveValue(op.left, columns, row);
                    const right = try resolveValue(op.right, columns, row);
                    return valuesEqual(left, right) or valueLess(left, right);
                },
                .ge => {
                    const left = try resolveValue(op.left, columns, row);
                    const right = try resolveValue(op.right, columns, row);
                    return valuesEqual(left, right) or valueLess(right, left);
                },
                .@"and" => {
                    const left = try evalWhere(op.left, columns, row);
                    const right = try evalWhere(op.right, columns, row);
                    return left and right;
                },
                .@"or" => {
                    const left = try evalWhere(op.left, columns, row);
                    const right = try evalWhere(op.right, columns, row);
                    return left or right;
                },
                else => return error.TypeError,
            }
        },
        .unary_op => |op| {
            if (op.op == .not) {
                return !(try evalWhere(op.operand, columns, row));
            }
            return error.TypeError;
        },
        .paren => |inner| return evalWhere(inner, columns, row),
        else => return error.TypeError,
    }
}

fn resolveValue(expr: *ast.Expr, columns: []const schema.Column, row: []const record.Value) !record.Value {
    switch (expr.*) {
        .integer_literal => |v| return .{ .integer = v },
        .real_literal => |v| return .{ .real = v },
        .string_literal => |v| return .{ .text = v },
        .null_literal => return .{ .null_val = {} },
        .column_ref => |ref| {
            for (columns, 0..) |col, i| {
                if (std.mem.eql(u8, col.name, ref.column)) {
                    if (i < row.len) return row[i];
                    return .{ .null_val = {} };
                }
            }
            return error.TypeError;
        },
        .unary_op => |op| {
            if (op.op == .negate) {
                const inner = try resolveValue(op.operand, columns, row);
                return switch (inner) {
                    .integer => |v| record.Value{ .integer = -v },
                    .real => |v| record.Value{ .real = -v },
                    else => error.TypeError,
                };
            }
            return error.TypeError;
        },
        .paren => |inner| return resolveValue(inner, columns, row),
        else => return error.TypeError,
    }
}

fn valuesEqual(a: record.Value, b: record.Value) bool {
    return switch (a) {
        .integer => |av| switch (b) {
            .integer => |bv| av == bv,
            .real => |bv| @as(f64, @floatFromInt(av)) == bv,
            .text => |bv| blk: {
                const parsed = std.fmt.parseInt(i64, bv, 10) catch break :blk false;
                break :blk av == parsed;
            },
            else => false,
        },
        .text => |av| switch (b) {
            .text => |bv| std.mem.eql(u8, av, bv),
            else => false,
        },
        .real => |av| switch (b) {
            .real => |bv| av == bv,
            .integer => |bv| av == @as(f64, @floatFromInt(bv)),
            else => false,
        },
        .null_val => switch (b) {
            .null_val => true,
            else => false,
        },
        .blob => false,
    };
}

fn valueLess(a: record.Value, b: record.Value) bool {
    return switch (a) {
        .integer => |av| switch (b) {
            .integer => |bv| av < bv,
            .real => |bv| @as(f64, @floatFromInt(av)) < bv,
            else => false,
        },
        .real => |av| switch (b) {
            .real => |bv| av < bv,
            .integer => |bv| av < @as(f64, @floatFromInt(bv)),
            else => false,
        },
        .text => |av| switch (b) {
            .text => |bv| std.mem.order(u8, av, bv) == .lt,
            else => false,
        },
        else => false,
    };
}
