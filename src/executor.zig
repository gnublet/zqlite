const std = @import("std");
const ast = @import("ast.zig");
const btree = @import("btree.zig");
const cursor = @import("cursor.zig");
const journal_mod = @import("journal.zig");
const os = @import("os.zig");
const pager = @import("pager.zig");
const record = @import("record.zig");
const schema = @import("schema.zig");

/// Module-level bound params for prepared statement execution.
/// Set by executeWithParams, read by expression evaluation functions.
var current_bound_params: ?[]const record.Value = null;

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
// Lazy result iterator — zero-allocation per-row access
// ═══════════════════════════════════════════════════════════════════════════

/// A lazy iterator over SELECT results. Yields one row at a time from the
/// B-tree cursor without materializing all rows. This is the ZQLite equivalent
/// of SQLite's sqlite3_step() — each call to next() advances the cursor,
/// evaluates the WHERE clause, and decodes matching rows on the stack.
pub const ResultIterator = struct {
    cur: cursor.Cursor,
    where_expr: ?*ast.Expr,
    columns: []const schema.Column,
    num_output_cols: usize,
    done: bool,

    const Self = @This();

    /// Advance to the next matching row. Returns decoded values in `out_buf`
    /// (zero heap allocation), or null when exhausted.
    pub fn next(self: *Self, out_buf: []Value) ?[]Value {
        while (self.cur.valid) {
            const entry = self.cur.cell() catch {
                self.done = true;
                return null;
            };

            // Evaluate WHERE clause lazily
            if (self.where_expr) |w| {
                const matches = evalWhereLazy(w, self.columns, entry.payload) catch false;
                if (!matches) {
                    _ = self.cur.next() catch { self.done = true; return null; };
                    continue;
                }
            }

            // Decode matching row into caller's stack buffer
            var decode_buf: [64]record.Value = undefined;
            const decoded = record.deserializeRecordBuf(entry.payload, &decode_buf) catch {
                _ = self.cur.next() catch {};
                continue;
            };

            const n = @min(self.num_output_cols, out_buf.len);
            for (0..n) |i| {
                out_buf[i] = if (i < decoded.len) recordToValue(decoded[i]) else .{ .null_val = {} };
            }

            _ = self.cur.next() catch { self.done = true; };
            return out_buf[0..n];
        }
        self.done = true;
        return null;
    }

    /// Release cursor resources.
    pub fn deinit(self: *Self) void {
        self.cur.releaseCachedPage();
        self.done = true;
    }
};
// ═══════════════════════════════════════════════════════════════════════════
// Executor
// ═══════════════════════════════════════════════════════════════════════════

pub const Executor = struct {
    pool: *pager.BufferPool,
    schema_store: *schema.Schema,
    allocator: std.mem.Allocator,
    next_page: u32,
    journal: ?*journal_mod.Journal,
    in_transaction: bool,
    file: ?*os.FileHandle,
    bound_params: ?[]const record.Value,
    schema_version: u64,
    select_cache: std.AutoHashMap(usize, CachedSelectPlan),
    join_cache: std.AutoHashMap(usize, CachedJoinPlan),
    table_row_cache: std.AutoHashMap(u32, CachedTableRows),
    data_version: u64,

    // ── Compiled Plan Cache types ────────────────────────────

    const CachedSelectPlan = struct {
        table_name: []const u8,
        root_page: u32,
        columns: []const schema.Column,
        compiled_where: ?WhereCompiled,
        col_indices: []?usize,
        col_names: [][]const u8,
        is_star: bool,
        num_output_cols: usize,
        schema_version: u64,
    };

    const CachedJoinPlan = struct {
        left_table: schema.Table,
        right_table: schema.Table,
        left_on_idx: ?usize,
        right_on_idx: ?usize,
        total_cols: usize,
        is_star: bool,
        schema_version: u64,
    };

    const CachedTableRows = struct {
        rows: [][]record.Value, // each row is a []record.Value
        schema_version: u64,
        root_page: u32,
        data_version: u64,
    };

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, pool: *pager.BufferPool, schema_store: *schema.Schema) Self {
        return Self{
            .pool = pool,
            .schema_store = schema_store,
            .allocator = allocator,
            .next_page = 1,
            .journal = null,
            .in_transaction = false,
            .file = null,
            .bound_params = null,
            .schema_version = 0,
            .select_cache = std.AutoHashMap(usize, CachedSelectPlan).init(allocator),
            .join_cache = std.AutoHashMap(usize, CachedJoinPlan).init(allocator),
            .table_row_cache = std.AutoHashMap(u32, CachedTableRows).init(allocator),
            .data_version = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        self.select_cache.deinit();
        self.join_cache.deinit();
        self.table_row_cache.deinit();
    }

    /// Invalidate all cached plans (called on DDL)
    fn invalidatePlans(self: *Self) void {
        self.schema_version +%= 1;
        self.select_cache.clearRetainingCapacity();
        self.join_cache.clearRetainingCapacity();
        self.table_row_cache.clearRetainingCapacity();
    }

    /// Invalidate table row cache (called on INSERT/UPDATE/DELETE)
    /// Uses lazy invalidation: only bumps data_version counter.
    /// The table cache checks data_version at lookup time.
    fn invalidateTableData(self: *Self) void {
        self.data_version +%= 1;
    }

    /// Set the file handle for schema persistence.
    pub fn setFile(self: *Self, f: *os.FileHandle) void {
        self.file = f;
    }

    /// Load schema from page 0 of the database file.
    /// Call this on startup before any SQL execution.
    pub fn loadSchemaFromDisk(self: *Self) void {
        const fh = self.file orelse return;
        if (fh.file_size < fh.page_size) return; // No page 0 yet

        var page_buf: [os.MAX_PAGE_SIZE]u8 = undefined;
        fh.readPage(0, page_buf[0..fh.page_size]) catch return;
        _ = self.schema_store.loadSchema(page_buf[0..fh.page_size]);
    }

    /// Persist schema to page 0 of the database file (direct I/O, bypasses buffer pool).
    fn persistSchema(self: *Self) void {
        const fh = self.file orelse return;
        var page_buf: [os.MAX_PAGE_SIZE]u8 = undefined;
        const buf = page_buf[0..fh.page_size];
        self.schema_store.saveSchema(buf);
        fh.writePage(0, buf) catch {};
        fh.sync() catch {};
    }

    /// Attach a journal for ACID transactions.
    pub fn setJournal(self: *Self, j: ?*journal_mod.Journal) void {
        self.journal = j;
    }

    /// Execute a statement with bound parameter values.
    /// params is a slice of record.Value, indexed by placeholder position (1-based).
    pub fn executeWithParams(self: *Self, stmt: ast.Statement, params: []const record.Value, arena: std.mem.Allocator) ExecError!ExecResult {
        self.bound_params = params;
        current_bound_params = params;
        defer {
            self.bound_params = null;
            current_bound_params = null;
        }
        return self.execute(stmt, arena);
    }

    /// Execute a statement via the bytecode VM (VDBE-like).
    /// Compiles AST → bytecode → executes on register VM → returns ExecResult.
    /// Much faster than AST tree walking for repeated queries.
    pub fn executeViaVM(self: *Self, stmt: ast.Statement, params: ?[]const record.Value, arena: std.mem.Allocator) ExecError!ExecResult {
        const codegen = @import("codegen.zig");
        const vm_mod = @import("vm.zig");

        // Compile AST → bytecode
        var compiler = codegen.Compiler.initWithSchema(arena, self.schema_store);
        const program = compiler.compile(stmt) catch return ExecError.StorageError;

        // Create VM with pool access
        var vm_inst = vm_mod.VM.init(arena, program) catch return ExecError.StorageError;
        vm_inst.pool = self.pool;
        vm_inst.bound_params = params;

        // Execute
        vm_inst.execute() catch return ExecError.StorageError;

        // Convert VM results to ExecResult
        if (vm_inst.results.items.len > 0) {
            const rows = arena.alloc(ResultRow, vm_inst.results.items.len) catch
                return ExecError.StorageError;
            for (vm_inst.results.items, 0..) |vm_row, i| {
                const vals = arena.alloc(Value, vm_row.values.len) catch
                    return ExecError.StorageError;
                for (vm_row.values, 0..) |reg, j| {
                    vals[j] = switch (reg) {
                        .null_val => .{ .null_val = {} },
                        .integer => |v| .{ .integer = v },
                        .real => |v| .{ .real = v },
                        .text => |v| .{ .text = v },
                        .blob => |v| .{ .text = v },
                        .boolean => |v| .{ .integer = if (v) 1 else 0 },
                    };
                }
                rows[i] = .{ .values = vals };
            }
            return ExecResult{
                .rows = rows,
                .column_names = @constCast(program.column_names),
                .rows_affected = 0,
                .message = null,
            };
        }

        return ExecResult{
            .rows = &.{},
            .column_names = @constCast(program.column_names),
            .rows_affected = vm_inst.rows_affected,
            .message = null,
        };
    }

    /// Execute a parsed statement with auto-commit if not in explicit transaction.
    pub fn execute(self: *Self, stmt: ast.Statement, arena: std.mem.Allocator) ExecError!ExecResult {
        // Handle transaction control statements
        switch (stmt) {
            .begin => return self.execBegin(),
            .commit => return self.execCommit(),
            .rollback => return self.execRollback(),
            else => {},
        }

        // Check if this is a mutating statement that needs auto-commit
        const is_mutating = switch (stmt) {
            .create_table, .drop_table, .insert, .delete, .update => true,
            else => false,
        };

        // Auto-commit: wrap mutating statements in begin/commit if not in explicit transaction
        const needs_auto_commit = is_mutating and !self.in_transaction and self.journal != null;

        if (needs_auto_commit) {
            if (self.journal) |j| {
                j.begin() catch return ExecError.StorageError;
            }
        }

        const result = self.executeInner(stmt, arena);

        if (needs_auto_commit) {
            if (result) |_| {
                // Success: flush dirty pages and commit
                if (self.journal) |j| {
                    self.pool.flushAll() catch {
                        j.rollback() catch {};
                        return ExecError.StorageError;
                    };
                    j.commit() catch return ExecError.StorageError;
                }
            } else |_| {
                // Error: rollback
                if (self.journal) |j| {
                    j.rollback() catch {};
                }
            }
        }

        return result;
    }

    /// Execute a statement (inner, no auto-commit wrapping).
    fn executeInner(self: *Self, stmt: ast.Statement, arena: std.mem.Allocator) ExecError!ExecResult {
        return switch (stmt) {
            .create_table => |ct| self.execCreateTable(ct, arena),
            .drop_table => |dt| self.execDropTable(dt, arena),
            .create_index => |ci| self.execCreateIndex(ci, arena),
            .drop_index => |di| self.execDropIndex(di, arena),
            .insert => |ins| self.execInsert(ins, arena),
            .select => |sel| self.execSelect(sel, arena),
            .delete => |del| self.execDelete(del, arena),
            .update => |upd| self.execUpdate(upd, arena),
            else => ExecError.UnsupportedStatement,
        };
    }

    fn execBegin(self: *Self) ExecError!ExecResult {
        if (self.in_transaction) return ExecError.StorageError;
        if (self.journal) |j| {
            j.begin() catch return ExecError.StorageError;
        }
        self.in_transaction = true;
        return ExecResult{ .rows = &.{}, .column_names = &.{}, .rows_affected = 0, .message = "Transaction started" };
    }

    fn execCommit(self: *Self) ExecError!ExecResult {
        if (!self.in_transaction) return ExecError.StorageError;
        if (self.journal) |j| {
            self.pool.flushAll() catch {
                j.rollback() catch {};
                self.in_transaction = false;
                return ExecError.StorageError;
            };
            j.commit() catch return ExecError.StorageError;
        }
        self.in_transaction = false;
        // Persist schema at end of transaction (deferred from individual INSERTs)
        self.persistSchema();
        return ExecResult{ .rows = &.{}, .column_names = &.{}, .rows_affected = 0, .message = "Transaction committed" };
    }

    fn execRollback(self: *Self) ExecError!ExecResult {
        if (!self.in_transaction) return ExecError.StorageError;
        if (self.journal) |j| {
            j.rollback() catch {};
        }
        self.in_transaction = false;
        return ExecResult{ .rows = &.{}, .column_names = &.{}, .rows_affected = 0, .message = "Transaction rolled back" };
    }

    // ─── CREATE TABLE ────────────────────────────────────────────────

    fn execCreateTable(self: *Self, ct: ast.Statement.CreateTable, _: std.mem.Allocator) ExecError!ExecResult {
        self.invalidatePlans();
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

        // Reserve page 0 for schema on a fresh database.
        // allocatePage() uses file.pageCount() as the new page id, so we
        // write an empty schema page first to bump the count to 1.
        if (self.file) |fh| {
            if (fh.file_size == 0) {
                // Write initial empty schema header to page 0
                self.persistSchema();
            }
        }

        // Create a B-tree for this table (gets page 1+ since page 0 is reserved)
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

        // Persist schema to disk
        self.persistSchema();

        return ExecResult{
            .rows = &.{},
            .column_names = &.{},
            .rows_affected = 0,
            .message = null,
        };
    }

    // ─── DROP TABLE ──────────────────────────────────────────────────

    fn execDropTable(self: *Self, dt: ast.Statement.DropTable, _: std.mem.Allocator) ExecError!ExecResult {
        self.invalidatePlans();
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
        // Persist schema to disk
        self.persistSchema();
        return ExecResult{
            .rows = &.{},
            .column_names = &.{},
            .rows_affected = 0,
            .message = null,
        };
    }

    // ─── CREATE INDEX ────────────────────────────────────────────────

    fn execCreateIndex(self: *Self, ci: ast.Statement.CreateIndex, _: std.mem.Allocator) ExecError!ExecResult {
        self.invalidatePlans();
        if (self.schema_store.getIndex(ci.name) != null) {
            if (ci.if_not_exists) {
                return ExecResult{ .rows = &.{}, .column_names = &.{}, .rows_affected = 0, .message = "index already exists, skipped" };
            }
            return ExecError.TableAlreadyExists;
        }

        const table_entry = self.schema_store.getTable(ci.table) orelse
            return ExecError.TableNotFound;

        // Resolve indexed column indices
        var col_indices_buf: [16]usize = undefined;
        for (ci.columns, 0..) |col_name, idx| {
            var found = false;
            for (table_entry.columns, 0..) |c, j| {
                if (std.mem.eql(u8, c.name, col_name)) {
                    col_indices_buf[idx] = j;
                    found = true;
                    break;
                }
            }
            if (!found) return ExecError.ColumnNotFound;
        }
        const col_indices = col_indices_buf[0..ci.columns.len];

        // Allocate index B-tree page
        var idx_bt = btree.Btree.create(self.pool, btree.PAGE_TYPE_INDEX_LEAF) catch
            return ExecError.StorageError;

        // Backfill: scan existing table rows and insert into index
        var table_bt = btree.Btree.open(self.pool, table_entry.root_page);
        var cur = cursor.Cursor.init(&table_bt);
        cur.first() catch {};

        while (cur.valid) {
            const entry = cur.cell() catch break;
            var key_buf: [256]u8 = undefined;
            const key_len = serializeIndexKeyFromPayload(entry.payload, col_indices, &key_buf) catch break;
            idx_bt.indexInsert(key_buf[0..key_len], entry.key) catch break;
            _ = cur.next() catch break;
        }

        self.schema_store.addIndex(.{
            .name = ci.name,
            .table_name = ci.table,
            .columns = ci.columns,
            .root_page = idx_bt.root_page_id,
            .is_unique = ci.unique,
        }) catch return ExecError.StorageError;

        self.persistSchema();
        return ExecResult{ .rows = &.{}, .column_names = &.{}, .rows_affected = 0, .message = null };
    }

    // ─── DROP INDEX ──────────────────────────────────────────────────

    fn execDropIndex(self: *Self, di: ast.Statement.DropIndex, _: std.mem.Allocator) ExecError!ExecResult {
        self.invalidatePlans();
        if (self.schema_store.getIndex(di.name) == null) {
            if (di.if_exists) {
                return ExecResult{ .rows = &.{}, .column_names = &.{}, .rows_affected = 0, .message = "index does not exist, skipped" };
            }
            return ExecError.TableNotFound;
        }
        _ = self.schema_store.dropIndex(di.name);
        self.persistSchema();
        return ExecResult{ .rows = &.{}, .column_names = &.{}, .rows_affected = 0, .message = null };
    }

    // ─── Index maintenance helpers ───────────────────────────────────

    fn updateIndexesForInsert(self: *Self, table_name: []const u8, table_columns: []const schema.Column, payload: []const u8, rowid: i64) void {
        var idx_buf: [16]schema.Index = undefined;
        const idx_count = self.schema_store.indexesForTable(table_name, &idx_buf);
        for (idx_buf[0..idx_count]) |idx_entry| {
            var col_idx_buf: [16]usize = undefined;
            for (idx_entry.columns, 0..) |col_name, ci| {
                for (table_columns, 0..) |c, j| {
                    if (std.mem.eql(u8, c.name, col_name)) {
                        col_idx_buf[ci] = j;
                        break;
                    }
                }
            }
            var key_buf: [256]u8 = undefined;
            const key_len = serializeIndexKeyFromPayload(payload, col_idx_buf[0..idx_entry.columns.len], &key_buf) catch continue;
            var idx_bt = btree.Btree.open(self.pool, idx_entry.root_page);
            idx_bt.indexInsert(key_buf[0..key_len], rowid) catch {};
        }
    }

    fn updateIndexesForDelete(self: *Self, table_name: []const u8, table_columns: []const schema.Column, payload: []const u8, rowid: i64) void {
        var idx_buf: [16]schema.Index = undefined;
        const idx_count = self.schema_store.indexesForTable(table_name, &idx_buf);
        for (idx_buf[0..idx_count]) |idx_entry| {
            var col_idx_buf: [16]usize = undefined;
            for (idx_entry.columns, 0..) |col_name, ci| {
                for (table_columns, 0..) |c, j| {
                    if (std.mem.eql(u8, c.name, col_name)) {
                        col_idx_buf[ci] = j;
                        break;
                    }
                }
            }
            var key_buf: [256]u8 = undefined;
            const key_len = serializeIndexKeyFromPayload(payload, col_idx_buf[0..idx_entry.columns.len], &key_buf) catch continue;
            var idx_bt = btree.Btree.open(self.pool, idx_entry.root_page);
            idx_bt.indexDelete(key_buf[0..key_len], rowid) catch {};
        }
    }

    // ─── INSERT ──────────────────────────────────────────────────────

    fn execInsert(self: *Self, ins: ast.Statement.Insert, _: std.mem.Allocator) ExecError!ExecResult {
        self.invalidateTableData();
        const table_entry = self.schema_store.getTable(ins.table) orelse
            return ExecError.TableNotFound;

        var bt = btree.Btree.open(self.pool, table_entry.root_page);
        var current_next_rowid = table_entry.next_rowid;

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
                rowid = current_next_rowid;
            }

            // Insert into B-tree
            bt.insert(rowid, buf[0..rec_size]) catch return ExecError.StorageError;

            // Maintain indexes
            self.updateIndexesForInsert(ins.table, table_entry.columns, buf[0..rec_size], rowid);

            // Track next_rowid locally (avoid schema store lookup per row)
            current_next_rowid = @max(current_next_rowid, rowid + 1);

            rows_inserted += 1;
        }

        // Update schema once after all rows
        var updated = self.schema_store.getTable(ins.table) orelse
            return ExecError.TableNotFound;
        updated.next_rowid = current_next_rowid;
        self.schema_store.addTable(updated) catch return ExecError.StorageError;

        // Only persist schema if not in explicit transaction (deferred to COMMIT)
        if (!self.in_transaction) self.persistSchema();

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

        // ── Join path ────────────────────────────────────────────────
        if (sel.joins.len > 0) {
            return self.execJoinSelect(sel, from, arena);
        }

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

        // ── Index lookup fast-path ──────────────────────────────────
        // Detect WHERE indexed_col = literal and use index B-tree
        if (sel.where) |where_expr| {
            if (self.tryIndexLookup(&bt, sel, table_entry, where_expr, arena)) |result| {
                return result;
            }
        }

        // ── Slow path: full cursor scan ──────────────────────────────
        var cur = cursor.Cursor.init(&bt);
        cur.first() catch return ExecError.StorageError;

        // Check select plan cache — avoid re-resolving columns + WHERE per query
        const cache_key = @intFromPtr(sel.columns.ptr);
        var cached_plan: ?CachedSelectPlan = null;
        if (self.select_cache.get(cache_key)) |cp| {
            if (cp.schema_version == self.schema_version) {
                cached_plan = cp;
            }
        }

        // Use slices directly — zero allocation on cache hit
        var col_names_slice: [][]const u8 = &.{};
        var col_indices_slice: []const ?usize = &.{};
        var is_star: bool = false;
        var compiled_where: ?WhereCompiled = null;
        var num_output_cols: usize = 0;

        // Temporary ArrayLists only used on cache miss
        var col_names_list: std.ArrayList([]const u8) = .{};
        var col_indices_list: std.ArrayList(?usize) = .{};

        if (cached_plan) |cp| {
            // Cache hit — use pre-resolved data directly (zero allocation!)
            is_star = cp.is_star;
            compiled_where = cp.compiled_where;
            col_names_slice = cp.col_names;
            col_indices_slice = cp.col_indices;
            num_output_cols = cp.num_output_cols;
        } else {
            // Cache miss — resolve columns and WHERE
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
                        col_indices_list.append(arena, idx) catch return ExecError.StorageError;
                    },
                    .table_all => {
                        is_star = true;
                        for (table_entry.columns) |cc| {
                            col_names_list.append(arena, cc.name) catch return ExecError.StorageError;
                        }
                    },
                }
            }

            col_names_slice = col_names_list.items;
            col_indices_slice = col_indices_list.items;
            num_output_cols = if (is_star) table_entry.columns.len else col_indices_list.items.len;

            // Pre-compile WHERE
            if (sel.where) |where_expr| {
                const cw = compileSimpleWhere(where_expr, table_entry.columns, self.allocator);
                if (cw.kind != .not_compiled) compiled_where = cw;
            }

            // Store in cache (use executor's allocator, not per-query arena)
            const cached_names = self.allocator.dupe([]const u8, col_names_list.items) catch col_names_list.items;
            const cached_indices = self.allocator.dupe(?usize, col_indices_list.items) catch col_indices_list.items;

            self.select_cache.put(cache_key, .{
                .table_name = table_entry.name,
                .root_page = table_entry.root_page,
                .columns = table_entry.columns,
                .compiled_where = compiled_where,
                .col_indices = cached_indices,
                .col_names = cached_names,
                .is_star = is_star,
                .num_output_cols = num_output_cols,
                .schema_version = self.schema_version,
            }) catch {};
        }

        // Scan rows — pre-allocate flat values buffer (one alloc for ALL rows)
        const pre_alloc_rows: usize = 256;
        var vals_pool = arena.alloc(Value, pre_alloc_rows * num_output_cols) catch
            return ExecError.StorageError;
        var rows_buf: [pre_alloc_rows]ResultRow = undefined;
        var row_count: usize = 0;

        while (cur.valid) {
            const entry = cur.cell() catch return ExecError.StorageError;

            // Evaluate WHERE — use compiled fast path when available
            if (sel.where) |where_expr| {
                const matches = if (compiled_where) |*cw|
                    evalWhereCompiled(cw, table_entry.columns, entry.payload) catch false
                else
                    evalWhereLazy(where_expr, table_entry.columns, entry.payload) catch false;
                if (!matches) {
                    _ = cur.next() catch return ExecError.StorageError;
                    continue;
                }
            }

            // Get value slice from pre-allocated pool (or re-alloc if overflow)
            if (row_count >= pre_alloc_rows) {
                // Fallback: grow with arena alloc for overflow rows
                const extra = arena.alloc(Value, num_output_cols) catch return ExecError.StorageError;
                var rows_list: std.ArrayList(ResultRow) = .{};
                rows_list.appendSlice(arena, rows_buf[0..row_count]) catch return ExecError.StorageError;

                if (is_star) {
                    var decode_buf: [64]record.Value = undefined;
                    const decoded = record.deserializeRecordBuf(entry.payload, &decode_buf) catch
                        return ExecError.SerializationFailed;
                    for (0..num_output_cols) |ci| {
                        extra[ci] = if (ci < decoded.len) recordToValue(decoded[ci]) else .{ .null_val = {} };
                    }
                } else {
                    for (col_indices_slice, 0..) |maybe_idx, ci| {
                        if (maybe_idx) |idx| {
                            const rv = record.readColumn(entry.payload, idx) catch record.Value{ .null_val = {} };
                            extra[ci] = recordToValue(rv);
                        } else {
                            extra[ci] = .{ .null_val = {} };
                        }
                    }
                }
                rows_list.append(arena, .{ .values = extra }) catch return ExecError.StorageError;

                _ = cur.next() catch return ExecError.StorageError;

                // Drain remaining with ArrayList
                while (cur.valid) {
                    const e2 = cur.cell() catch return ExecError.StorageError;
                    if (sel.where) |where_expr| {
                        const m2 = evalWhereLazy(where_expr, table_entry.columns, e2.payload) catch false;
                        if (!m2) { _ = cur.next() catch return ExecError.StorageError; continue; }
                    }
                    const extra2 = arena.alloc(Value, num_output_cols) catch return ExecError.StorageError;
                    if (is_star) {
                        var decode_buf2: [64]record.Value = undefined;
                        const decoded2 = record.deserializeRecordBuf(e2.payload, &decode_buf2) catch return ExecError.SerializationFailed;
                        for (0..num_output_cols) |ci| { extra2[ci] = if (ci < decoded2.len) recordToValue(decoded2[ci]) else .{ .null_val = {} }; }
                    } else {
                        for (col_indices_slice, 0..) |maybe_idx, ci| {
                            if (maybe_idx) |idx| { extra2[ci] = recordToValue(record.readColumn(e2.payload, idx) catch record.Value{ .null_val = {} }); } else { extra2[ci] = .{ .null_val = {} }; }
                        }
                    }
                    rows_list.append(arena, .{ .values = extra2 }) catch return ExecError.StorageError;
                    _ = cur.next() catch return ExecError.StorageError;
                }

                return ExecResult{
                    .rows = rows_list.items,
                    .column_names = col_names_slice,
                    .rows_affected = 0,
                    .message = null,
                };
            }

            const vals = vals_pool[row_count * num_output_cols ..][0..num_output_cols];

            // Build result row
            if (is_star) {
                var decode_buf: [64]record.Value = undefined;
                const decoded = record.deserializeRecordBuf(entry.payload, &decode_buf) catch
                    return ExecError.SerializationFailed;
                for (0..num_output_cols) |i| {
                    vals[i] = if (i < decoded.len) recordToValue(decoded[i]) else .{ .null_val = {} };
                }
            } else {
                for (col_indices_slice, 0..) |maybe_idx, i| {
                    if (maybe_idx) |idx| {
                        const rv = record.readColumn(entry.payload, idx) catch record.Value{ .null_val = {} };
                        vals[i] = recordToValue(rv);
                    } else {
                        vals[i] = .{ .null_val = {} };
                    }
                }
            }
            rows_buf[row_count] = .{ .values = vals };
            row_count += 1;

            _ = cur.next() catch return ExecError.StorageError;
        }

        // Copy stack rows to arena-owned slice for stable return
        const result_rows = arena.dupe(ResultRow, rows_buf[0..row_count]) catch
            return ExecError.StorageError;

        return ExecResult{
            .rows = result_rows,
            .column_names = col_names_slice,
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

    /// Try to use an index for WHERE col = literal lookups.
    /// Returns null if no index can be used.
    fn tryIndexLookup(
        self: *Self,
        bt: *btree.Btree,
        sel: ast.Statement.Select,
        table_entry: schema.Table,
        where_expr: *ast.Expr,
        arena: std.mem.Allocator,
    ) ?ExecResult {
        // Only handle simple equality: col = literal
        const eq_op = switch (where_expr.*) {
            .binary_op => |bop| if (bop.op == .eq) bop else return null,
            else => return null,
        };

        // Extract column name and literal value
        const col_name = switch (eq_op.left.*) {
            .column_ref => |cr| cr.column,
            else => return null,
        };

        // Check if we have an index on this column
        var idx_buf: [16]schema.Index = undefined;
        const idx_count = self.schema_store.indexesForTable(table_entry.name, &idx_buf);
        var matching_index: ?schema.Index = null;
        for (idx_buf[0..idx_count]) |idx_entry| {
            if (idx_entry.columns.len == 1 and std.mem.eql(u8, idx_entry.columns[0], col_name)) {
                matching_index = idx_entry;
                break;
            }
        }
        const idx_entry = matching_index orelse return null;

        // Serialize the search value to key bytes
        const search_val = evalToRecordValue(eq_op.right) catch return null;
        var key_buf: [256]u8 = undefined;
        var key_len: usize = 0;
        switch (search_val) {
            .integer => |v| {
                const unsigned: u64 = @bitCast(v);
                const sortable = unsigned ^ (@as(u64, 1) << 63);
                std.mem.writeInt(u64, key_buf[0..8], sortable, .big);
                key_len = 8;
            },
            .text => |t| {
                if (t.len > key_buf.len) return null;
                @memcpy(key_buf[0..t.len], t);
                key_len = t.len;
            },
            else => return null,
        }

        // Look up rowids in index
        var idx_bt = btree.Btree.open(self.pool, idx_entry.root_page);
        var rowid_buf: [1024]i64 = undefined;
        const matching_rowids = idx_bt.indexSearch(key_buf[0..key_len], &rowid_buf) catch return null;

        // Build column names
        var col_names_list: std.ArrayList([]const u8) = .{};
        var is_star = false;
        var col_indices_list: std.ArrayList(?usize) = .{};

        for (sel.columns) |col| {
            switch (col) {
                .all_columns, .table_all => {
                    is_star = true;
                    for (table_entry.columns) |cc| {
                        col_names_list.append(arena, cc.name) catch return null;
                    }
                },
                .expr => |ec| {
                    col_names_list.append(arena, ec.alias orelse exprName(ec.expr)) catch return null;
                    col_indices_list.append(arena, resolveColumnIndex(ec.expr, table_entry.columns)) catch return null;
                },
            }
        }

        // Fetch rows by rowid using targeted bt.search() per matching rowid
        var rows_list: std.ArrayList(ResultRow) = .{};
        for (matching_rowids) |rid| {
            const cell_result = bt.search(rid) catch continue;
            const entry = cell_result orelse continue;

            if (is_star) {
                const num_cols = table_entry.columns.len;
                const vals = arena.alloc(Value, num_cols) catch return null;
                // Use readColumn per column to avoid full deserialization
                for (0..num_cols) |ci| {
                    const rv = record.readColumn(entry.payload, ci) catch record.Value{ .null_val = {} };
                    vals[ci] = recordToValue(rv);
                }
                rows_list.append(arena, .{ .values = vals }) catch return null;
            } else {
                const vals = arena.alloc(Value, col_indices_list.items.len) catch return null;
                for (col_indices_list.items, 0..) |maybe_idx, i| {
                    if (maybe_idx) |idx| {
                        const rv = record.readColumn(entry.payload, idx) catch record.Value{ .null_val = {} };
                        vals[i] = recordToValue(rv);
                    } else {
                        vals[i] = .{ .null_val = {} };
                    }
                }
                rows_list.append(arena, .{ .values = vals }) catch return null;
            }
        }

        return ExecResult{
            .rows = rows_list.items,
            .column_names = col_names_list.items,
            .rows_affected = 0,
            .message = null,
        };
    }

    // ─── JOIN SELECT ─────────────────────────────────────────────────

    /// Table context for multi-table join evaluation
    const JoinTableCtx = struct {
        name: []const u8, // table name or alias
        columns: []const schema.Column,
        values: []const record.Value, // current row decoded values
    };

    fn execJoinSelect(
        self: *Self,
        sel: ast.Statement.Select,
        from: ast.TableRef,
        arena: std.mem.Allocator,
    ) ExecError!ExecResult {
        // Resolve left (FROM) table
        const left_table = self.schema_store.getTable(from.name) orelse
            return ExecError.TableNotFound;
        const left_alias = from.alias orelse from.name;

        // Collect all join tables
        var join_tables: [8]JoinInfo = undefined;
        for (sel.joins, 0..) |jc, i| {
            const jt = self.schema_store.getTable(jc.table.name) orelse
                return ExecError.TableNotFound;
            join_tables[i] = .{
                .table = jt,
                .alias = jc.table.alias orelse jc.table.name,
                .join_type = jc.join_type,
                .on_expr = jc.on,
            };
        }
        const num_joins = sel.joins.len;

        // Build output column list
        var col_names_list: std.ArrayList([]const u8) = .{};
        var is_star = false;
        // For star: columns from left + all join tables
        for (sel.columns) |col| {
            switch (col) {
                .all_columns => {
                    is_star = true;
                    for (left_table.columns) |cc| {
                        col_names_list.append(arena, cc.name) catch return ExecError.StorageError;
                    }
                    for (0..num_joins) |ji| {
                        for (join_tables[ji].table.columns) |cc| {
                            col_names_list.append(arena, cc.name) catch return ExecError.StorageError;
                        }
                    }
                },
                .table_all => {
                    is_star = true;
                    for (left_table.columns) |cc| {
                        col_names_list.append(arena, cc.name) catch return ExecError.StorageError;
                    }
                    for (0..num_joins) |ji| {
                        for (join_tables[ji].table.columns) |cc| {
                            col_names_list.append(arena, cc.name) catch return ExecError.StorageError;
                        }
                    }
                },
                .expr => |ec| {
                    col_names_list.append(arena, ec.alias orelse exprName(ec.expr)) catch return ExecError.StorageError;
                },
            }
        }

        // Result rows
        var rows_list: std.ArrayList(ResultRow) = .{};

        // ── Pre-load both tables into memory ────────────────────
        // Eliminates B-tree page reads and deserializations from inner loop.
        if (num_joins == 1) {
            const ji = join_tables[0];

            const PreloadedRow = struct { values: []record.Value };

            // Load tables — check table row cache first
            var right_rows: std.ArrayList(PreloadedRow) = .{};
            if (self.table_row_cache.get(ji.table.root_page)) |cached| {
                if (cached.data_version == self.data_version) {
                    // Cache hit — wrap cached rows as PreloadedRow
                    for (cached.rows) |row| {
                        right_rows.append(arena, .{ .values = row }) catch break;
                    }
                }
            }
            if (right_rows.items.len == 0) {
                // Cache miss — load from B-tree
                var right_bt = btree.Btree.open(self.pool, ji.table.root_page);
                var right_cur = cursor.Cursor.init(&right_bt);
                right_cur.first() catch {};
                while (right_cur.valid) {
                    const right_entry = right_cur.cell() catch break;
                    const decoded = record.deserializeRecord(right_entry.payload, self.allocator) catch {
                        _ = right_cur.next() catch {};
                        continue;
                    };
                    right_rows.append(arena, .{ .values = decoded }) catch break;
                    _ = right_cur.next() catch break;
                }
                // Store in table cache
                const cached_rows = self.allocator.alloc([]record.Value, right_rows.items.len) catch null;
                if (cached_rows) |cr| {
                    for (right_rows.items, 0..) |row, i| cr[i] = row.values;
                    self.table_row_cache.put(ji.table.root_page, .{
                        .rows = cr,
                        .schema_version = self.schema_version,
                        .root_page = ji.table.root_page,
                        .data_version = self.data_version,
                    }) catch {};
                }
            }

            var left_rows: std.ArrayList(PreloadedRow) = .{};
            if (self.table_row_cache.get(left_table.root_page)) |cached| {
                if (cached.data_version == self.data_version) {
                    for (cached.rows) |row| {
                        left_rows.append(arena, .{ .values = row }) catch break;
                    }
                }
            }
            if (left_rows.items.len == 0) {
                var left_bt = btree.Btree.open(self.pool, left_table.root_page);
                var left_cur = cursor.Cursor.init(&left_bt);
                left_cur.first() catch {};
                while (left_cur.valid) {
                    const left_entry = left_cur.cell() catch break;
                    const decoded = record.deserializeRecord(left_entry.payload, self.allocator) catch {
                        _ = left_cur.next() catch {};
                        continue;
                    };
                    left_rows.append(arena, .{ .values = decoded }) catch break;
                    _ = left_cur.next() catch break;
                }
                const cached_rows = self.allocator.alloc([]record.Value, left_rows.items.len) catch null;
                if (cached_rows) |cr| {
                    for (left_rows.items, 0..) |row, i| cr[i] = row.values;
                    self.table_row_cache.put(left_table.root_page, .{
                        .rows = cr,
                        .schema_version = self.schema_version,
                        .root_page = left_table.root_page,
                        .data_version = self.data_version,
                    }) catch {};
                }
            }

            const total_cols = left_table.columns.len + ji.table.columns.len;

            // ── Pre-compute ON clause column indices ──────────────
            // For simple ON equality (a.col = b.col), resolve column names
            // to integer indices once, then use direct array indexing in the loop.
            var left_on_idx: ?usize = null;
            var right_on_idx: ?usize = null;
            var use_fast_eq = false;

            if (ji.on_expr) |on_expr| {
                if (on_expr.* == .binary_op) {
                    const bop = on_expr.binary_op;
                    if (bop.op == .eq) {
                        if (bop.left.* == .column_ref and bop.right.* == .column_ref) {
                            const left_ref = bop.left.column_ref;
                            const right_ref = bop.right.column_ref;

                            // Resolve left column index
                            if (left_ref.table) |tbl| {
                                if (std.mem.eql(u8, tbl, left_alias)) {
                                    for (left_table.columns, 0..) |col, ci| {
                                        if (std.mem.eql(u8, col.name, left_ref.column)) { left_on_idx = ci; break; }
                                    }
                                } else if (std.mem.eql(u8, tbl, ji.alias)) {
                                    for (ji.table.columns, 0..) |col, ci| {
                                        if (std.mem.eql(u8, col.name, left_ref.column)) { right_on_idx = ci; break; }
                                    }
                                }
                            }

                            // Resolve right column index
                            if (right_ref.table) |tbl| {
                                if (std.mem.eql(u8, tbl, left_alias)) {
                                    for (left_table.columns, 0..) |col, ci| {
                                        if (std.mem.eql(u8, col.name, right_ref.column)) { left_on_idx = ci; break; }
                                    }
                                } else if (std.mem.eql(u8, tbl, ji.alias)) {
                                    for (ji.table.columns, 0..) |col, ci| {
                                        if (std.mem.eql(u8, col.name, right_ref.column)) { right_on_idx = ci; break; }
                                    }
                                }
                            }

                            if (left_on_idx != null and right_on_idx != null) {
                                use_fast_eq = true;
                            }
                        }
                    }
                }
            }

            // ── Hash join for integer equi-joins ─────────────────
            if (use_fast_eq) {
                // Build hash map: right join column value → list of row indices
                const HashMapType = std.AutoHashMap(i64, std.ArrayList(usize));
                var hash_map = HashMapType.init(arena);

                for (right_rows.items, 0..) |right_row, ri| {
                    if (right_on_idx.? < right_row.values.len) {
                        const rv = right_row.values[right_on_idx.?];
                        switch (rv) {
                            .integer => |iv| {
                                const gop = hash_map.getOrPut(iv) catch continue;
                                if (!gop.found_existing) {
                                    gop.value_ptr.* = .{};
                                }
                                gop.value_ptr.append(arena, ri) catch continue;
                            },
                            else => {},
                        }
                    }
                }

                // Pre-allocate batch buffer for result values (one big alloc)
                const max_result_rows = left_rows.items.len * 2; // estimate: at most 2x left table
                var vals_batch = arena.alloc(Value, max_result_rows * total_cols) catch
                    return ExecError.StorageError;
                var batch_idx: usize = 0;
                var rows_buf: [256]ResultRow = undefined;
                var row_count: usize = 0;

                // Probe hash map for each left row
                for (left_rows.items) |left_row| {
                    var matched = false;
                    const lv = if (left_on_idx.? < left_row.values.len) left_row.values[left_on_idx.?] else record.Value{ .null_val = {} };

                    if (lv == .integer) {
                        if (hash_map.get(lv.integer)) |indices| {
                            matched = true;
                            for (indices.items) |ri| {
                                const right_row = right_rows.items[ri];
                                if (is_star) {
                                    // Slice from batch or fallback to arena
                                    const vals = if (batch_idx + total_cols <= vals_batch.len)
                                        blk: {
                                            const s = vals_batch[batch_idx .. batch_idx + total_cols];
                                            batch_idx += total_cols;
                                            break :blk s;
                                        }
                                    else
                                        arena.alloc(Value, total_cols) catch continue;

                                    var vi: usize = 0;
                                    for (0..left_table.columns.len) |ci| {
                                        vals[vi] = if (ci < left_row.values.len) recordToValue(left_row.values[ci]) else .{ .null_val = {} };
                                        vi += 1;
                                    }
                                    for (0..ji.table.columns.len) |ci| {
                                        vals[vi] = if (ci < right_row.values.len) recordToValue(right_row.values[ci]) else .{ .null_val = {} };
                                        vi += 1;
                                    }
                                    if (row_count < rows_buf.len) {
                                        rows_buf[row_count] = .{ .values = vals };
                                        row_count += 1;
                                    } else {
                                        rows_list.append(arena, .{ .values = vals }) catch continue;
                                    }
                                } else {
                                    self.buildJoinResultRow(
                                        sel, is_star, left_table, left_alias, left_row.values,
                                        &[_]JoinInfo{ji}, &[_]?[]const record.Value{right_row.values},
                                        arena, &rows_list,
                                    ) catch continue;
                                }
                            }
                        }
                    }

                    // LEFT JOIN: emit left row with NULLs if no match
                    if (!matched and ji.join_type == .left) {
                        if (is_star) {
                            const vals = if (batch_idx + total_cols <= vals_batch.len)
                                blk: {
                                    const s = vals_batch[batch_idx .. batch_idx + total_cols];
                                    batch_idx += total_cols;
                                    break :blk s;
                                }
                            else
                                arena.alloc(Value, total_cols) catch continue;
                            var vi: usize = 0;
                            for (0..left_table.columns.len) |ci| {
                                vals[vi] = if (ci < left_row.values.len) recordToValue(left_row.values[ci]) else .{ .null_val = {} };
                                vi += 1;
                            }
                            for (0..ji.table.columns.len) |_| {
                                vals[vi] = .{ .null_val = {} };
                                vi += 1;
                            }
                            if (row_count < rows_buf.len) {
                                rows_buf[row_count] = .{ .values = vals };
                                row_count += 1;
                            } else {
                                rows_list.append(arena, .{ .values = vals }) catch continue;
                            }
                        } else {
                            self.buildJoinResultRow(
                                sel, is_star, left_table, left_alias, left_row.values,
                                &[_]JoinInfo{ji}, &[_]?[]const record.Value{null},
                                arena, &rows_list,
                            ) catch continue;
                        }
                    }
                }

                // Flush rows_buf into rows_list
                if (row_count > 0) {
                    rows_list.appendSlice(arena, rows_buf[0..row_count]) catch {};
                }
            } else {
            // ── Nested loop join (fallback) ──────────────────────
            for (left_rows.items) |left_row| {
                var matched = false;

                for (right_rows.items) |right_row| {
                    var on_match: bool = true;
                    if (ji.on_expr) |on_expr| {
                        const tables = [_]JoinTableCtx{
                            .{ .name = left_alias, .columns = left_table.columns, .values = left_row.values },
                            .{ .name = ji.alias, .columns = ji.table.columns, .values = right_row.values },
                        };
                        on_match = evalExprMultiTable(on_expr, &tables) catch false;
                    }

                    if (on_match) {
                        matched = true;
                        if (is_star) {
                            const vals = arena.alloc(Value, total_cols) catch continue;
                            var vi: usize = 0;
                            for (0..left_table.columns.len) |ci| {
                                vals[vi] = if (ci < left_row.values.len) recordToValue(left_row.values[ci]) else .{ .null_val = {} };
                                vi += 1;
                            }
                            for (0..ji.table.columns.len) |ci| {
                                vals[vi] = if (ci < right_row.values.len) recordToValue(right_row.values[ci]) else .{ .null_val = {} };
                                vi += 1;
                            }
                            rows_list.append(arena, .{ .values = vals }) catch continue;
                        } else {
                            self.buildJoinResultRow(
                                sel, is_star, left_table, left_alias, left_row.values,
                                &[_]JoinInfo{ji}, &[_]?[]const record.Value{right_row.values},
                                arena, &rows_list,
                            ) catch continue;
                        }
                    }
                }

                // LEFT JOIN: emit left row with NULLs if no match
                if (!matched and ji.join_type == .left) {
                    if (is_star) {
                        const vals = arena.alloc(Value, total_cols) catch continue;
                        var vi: usize = 0;
                        for (0..left_table.columns.len) |ci| {
                            vals[vi] = if (ci < left_row.values.len) recordToValue(left_row.values[ci]) else .{ .null_val = {} };
                            vi += 1;
                        }
                        for (0..ji.table.columns.len) |_| {
                            vals[vi] = .{ .null_val = {} };
                            vi += 1;
                        }
                        rows_list.append(arena, .{ .values = vals }) catch continue;
                    } else {
                        self.buildJoinResultRow(
                            sel, is_star, left_table, left_alias, left_row.values,
                            &[_]JoinInfo{ji}, &[_]?[]const record.Value{null},
                            arena, &rows_list,
                        ) catch continue;
                    }
                }
            }
            } // end else (nested loop fallback)
        }

        return ExecResult{
            .rows = rows_list.items,
            .column_names = col_names_list.items,
            .rows_affected = 0,
            .message = null,
        };
    }

    const JoinInfo = struct {
        table: schema.Table,
        alias: []const u8,
        join_type: ast.JoinType,
        on_expr: ?*ast.Expr,
    };

    fn buildJoinResultRow(
        self: *Self,
        sel: ast.Statement.Select,
        is_star: bool,
        left_table: schema.Table,
        left_alias: []const u8,
        left_vals: []const record.Value,
        join_infos: []const JoinInfo,
        right_vals_list: []const ?[]const record.Value,
        arena: std.mem.Allocator,
        rows_list: *std.ArrayList(ResultRow),
    ) ExecError!void {
        _ = self;
        if (is_star) {
            // Count total columns
            var total_cols = left_table.columns.len;
            for (join_infos) |ji| total_cols += ji.table.columns.len;

            const vals = arena.alloc(Value, total_cols) catch return ExecError.StorageError;
            var vi: usize = 0;

            // Left table values
            for (0..left_table.columns.len) |i| {
                vals[vi] = if (i < left_vals.len) recordToValue(left_vals[i]) else .{ .null_val = {} };
                vi += 1;
            }

            // Right table values (or NULLs for LEFT JOIN)
            for (join_infos, 0..) |ji, ji_idx| {
                if (right_vals_list[ji_idx]) |rv| {
                    for (0..ji.table.columns.len) |i| {
                        vals[vi] = if (i < rv.len) recordToValue(rv[i]) else .{ .null_val = {} };
                        vi += 1;
                    }
                } else {
                    for (0..ji.table.columns.len) |_| {
                        vals[vi] = .{ .null_val = {} };
                        vi += 1;
                    }
                }
            }

            rows_list.append(arena, .{ .values = vals }) catch return ExecError.StorageError;
        } else {
            // Resolve specific columns
            var tables_buf: [8]JoinTableCtx = undefined;
            tables_buf[0] = .{ .name = left_alias, .columns = left_table.columns, .values = left_vals };
            for (join_infos, 0..) |ji, ji_idx| {
                if (right_vals_list[ji_idx]) |rv| {
                    tables_buf[1 + ji_idx] = .{ .name = ji.alias, .columns = ji.table.columns, .values = rv };
                } else {
                    // LEFT JOIN null row
                    tables_buf[1 + ji_idx] = .{ .name = ji.alias, .columns = ji.table.columns, .values = &.{} };
                }
            }
            const tables = tables_buf[0 .. 1 + join_infos.len];

            var expr_cols: std.ArrayList(Value) = .{};
            for (sel.columns) |col| {
                switch (col) {
                    .all_columns, .table_all => {}, // handled by is_star
                    .expr => |ec| {
                        const val = resolveValueMultiTable(ec.expr, tables) catch record.Value{ .null_val = {} };
                        expr_cols.append(arena, recordToValue(val)) catch return ExecError.StorageError;
                    },
                }
            }
            const vals = arena.dupe(Value, expr_cols.items) catch return ExecError.StorageError;
            rows_list.append(arena, .{ .values = vals }) catch return ExecError.StorageError;
        }
    }

    // ─── DELETE ──────────────────────────────────────────────────────

    fn execDelete(self: *Self, del: ast.Statement.Delete, arena: std.mem.Allocator) ExecError!ExecResult {
        self.invalidateTableData();
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

                const matches = evalWhereLazy(where_expr, table_entry.columns, entry.payload) catch false;
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
        self.invalidateTableData();
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

            var should_update = true;
            if (upd.where) |where_expr| {
                should_update = evalWhereLazy(where_expr, table_entry.columns, entry.payload) catch false;
            }

            if (should_update) {
                const decoded = record.deserializeRecord(entry.payload, arena) catch
                    return ExecError.SerializationFailed;
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
        .placeholder => |idx| {
            if (current_bound_params) |params| {
                if (idx >= 1 and idx <= params.len) return params[idx - 1];
            }
            return error.TypeError;
        },
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

// ═══════════════════════════════════════════════════════════════════════════
// Compiled WHERE — pre-resolve column indices once, avoid per-row string cmp
// ═══════════════════════════════════════════════════════════════════════════

const WhereCompiled = struct {
    kind: Kind,

    const Kind = union(enum) {
        simple: Simple,
        compound_and: Compound,
        compound_or: Compound,
        not_compiled: void, // fallback to evalWhereLazy
    };

    const Simple = struct {
        col_idx: usize,
        literal: record.Value,
        op: ast.BinOp,
    };

    const Compound = struct {
        parts: [4]?*const WhereCompiled,
        count: usize,
    };
};

fn compileSimpleWhere(expr: *ast.Expr, columns: []const schema.Column, arena: std.mem.Allocator) WhereCompiled {
    switch (expr.*) {
        .binary_op => |bop| {
            switch (bop.op) {
                .eq, .ne, .lt, .gt, .le, .ge => {
                    // Try: column op literal/placeholder
                    if (bop.left.* == .column_ref) {
                        const col_name = bop.left.column_ref.column;
                        for (columns, 0..) |col, i| {
                            if (std.mem.eql(u8, col.name, col_name)) {
                                const lit = resolveLiteralValue(bop.right) orelse
                                    return .{ .kind = .{ .not_compiled = {} } };
                                return .{ .kind = .{ .simple = .{
                                    .col_idx = i,
                                    .literal = lit,
                                    .op = bop.op,
                                } } };
                            }
                        }
                    }
                    // Try: literal op column (reversed)
                    if (bop.right.* == .column_ref) {
                        const col_name = bop.right.column_ref.column;
                        for (columns, 0..) |col, i| {
                            if (std.mem.eql(u8, col.name, col_name)) {
                                const lit = resolveLiteralValue(bop.left) orelse
                                    return .{ .kind = .{ .not_compiled = {} } };
                                // Reverse the operator
                                const rev_op: ast.BinOp = switch (bop.op) {
                                    .lt => .gt,
                                    .gt => .lt,
                                    .le => .ge,
                                    .ge => .le,
                                    else => bop.op, // eq, ne are symmetric
                                };
                                return .{ .kind = .{ .simple = .{
                                    .col_idx = i,
                                    .literal = lit,
                                    .op = rev_op,
                                } } };
                            }
                        }
                    }
                    return .{ .kind = .{ .not_compiled = {} } };
                },
                .@"and", .@"or" => {
                    const left_c = arena.create(WhereCompiled) catch return .{ .kind = .{ .not_compiled = {} } };
                    left_c.* = compileSimpleWhere(bop.left, columns, arena);
                    const right_c = arena.create(WhereCompiled) catch return .{ .kind = .{ .not_compiled = {} } };
                    right_c.* = compileSimpleWhere(bop.right, columns, arena);

                    var parts = [_]?*const WhereCompiled{null} ** 4;
                    parts[0] = left_c;
                    parts[1] = right_c;

                    if (bop.op == .@"and") {
                        return .{ .kind = .{ .compound_and = .{ .parts = parts, .count = 2 } } };
                    } else {
                        return .{ .kind = .{ .compound_or = .{ .parts = parts, .count = 2 } } };
                    }
                },
                else => return .{ .kind = .{ .not_compiled = {} } },
            }
        },
        .paren => |inner| return compileSimpleWhere(inner, columns, arena),
        else => return .{ .kind = .{ .not_compiled = {} } },
    }
}

/// Resolve a literal or placeholder value from an expression (no column refs).
fn resolveLiteralValue(expr: *ast.Expr) ?record.Value {
    return switch (expr.*) {
        .integer_literal => |v| .{ .integer = v },
        .real_literal => |v| .{ .real = v },
        .string_literal => |v| .{ .text = v },
        .null_literal => .{ .null_val = {} },
        .placeholder => |idx| {
            if (current_bound_params) |params| {
                if (idx >= 1 and idx <= params.len) return params[idx - 1];
            }
            return null;
        },
        .unary_op => |op| {
            if (op.op == .negate) {
                const inner = resolveLiteralValue(op.operand) orelse return null;
                return switch (inner) {
                    .integer => |v| record.Value{ .integer = -v },
                    .real => |v| record.Value{ .real = -v },
                    else => null,
                };
            }
            return null;
        },
        .paren => |inner| resolveLiteralValue(inner),
        else => null,
    };
}

/// Fast WHERE evaluation using pre-compiled column indices.
fn evalWhereCompiled(compiled: *const WhereCompiled, columns: []const schema.Column, payload: []const u8) !bool {
    switch (compiled.kind) {
        .simple => |s| {
            const col_val = record.readColumn(payload, s.col_idx) catch return false;
            return switch (s.op) {
                .eq => valuesEqual(col_val, s.literal),
                .ne => !valuesEqual(col_val, s.literal),
                .lt => valueLess(col_val, s.literal),
                .gt => valueLess(s.literal, col_val),
                .le => valuesEqual(col_val, s.literal) or valueLess(col_val, s.literal),
                .ge => valuesEqual(col_val, s.literal) or valueLess(s.literal, col_val),
                else => error.TypeError,
            };
        },
        .compound_and => |c| {
            for (c.parts[0..c.count]) |maybe_part| {
                const part = maybe_part orelse continue;
                if (part.kind == .not_compiled) return error.TypeError;
                if (!try evalWhereCompiled(part, columns, payload)) return false;
            }
            return true;
        },
        .compound_or => |c| {
            for (c.parts[0..c.count]) |maybe_part| {
                const part = maybe_part orelse continue;
                if (part.kind == .not_compiled) return error.TypeError;
                if (try evalWhereCompiled(part, columns, payload)) return true;
            }
            return false;
        },
        .not_compiled => return error.TypeError,
    }
}

/// Lazy WHERE evaluation: uses record.readColumn() to only decode referenced columns.
/// No allocation needed — zero-copy from the raw record buffer.
fn evalWhereLazy(expr: *ast.Expr, columns: []const schema.Column, payload: []const u8) !bool {
    switch (expr.*) {
        .binary_op => |op| {
            switch (op.op) {
                .eq => {
                    const left = try resolveValueLazy(op.left, columns, payload);
                    const right = try resolveValueLazy(op.right, columns, payload);
                    return valuesEqual(left, right);
                },
                .ne => {
                    const left = try resolveValueLazy(op.left, columns, payload);
                    const right = try resolveValueLazy(op.right, columns, payload);
                    return !valuesEqual(left, right);
                },
                .lt => {
                    const left = try resolveValueLazy(op.left, columns, payload);
                    const right = try resolveValueLazy(op.right, columns, payload);
                    return valueLess(left, right);
                },
                .gt => {
                    const left = try resolveValueLazy(op.left, columns, payload);
                    const right = try resolveValueLazy(op.right, columns, payload);
                    return valueLess(right, left);
                },
                .le => {
                    const left = try resolveValueLazy(op.left, columns, payload);
                    const right = try resolveValueLazy(op.right, columns, payload);
                    return valuesEqual(left, right) or valueLess(left, right);
                },
                .ge => {
                    const left = try resolveValueLazy(op.left, columns, payload);
                    const right = try resolveValueLazy(op.right, columns, payload);
                    return valuesEqual(left, right) or valueLess(right, left);
                },
                .@"and" => {
                    const left = try evalWhereLazy(op.left, columns, payload);
                    if (!left) return false; // short-circuit
                    return try evalWhereLazy(op.right, columns, payload);
                },
                .@"or" => {
                    const left = try evalWhereLazy(op.left, columns, payload);
                    if (left) return true; // short-circuit
                    return try evalWhereLazy(op.right, columns, payload);
                },
                else => return error.TypeError,
            }
        },
        .unary_op => |op| {
            if (op.op == .not) {
                return !(try evalWhereLazy(op.operand, columns, payload));
            }
            return error.TypeError;
        },
        .paren => |inner| return evalWhereLazy(inner, columns, payload),
        else => return error.TypeError,
    }
}

/// Resolve a value from an expression using lazy column access (readColumn).
fn resolveValueLazy(expr: *ast.Expr, columns: []const schema.Column, payload: []const u8) !record.Value {
    switch (expr.*) {
        .integer_literal => |v| return .{ .integer = v },
        .real_literal => |v| return .{ .real = v },
        .string_literal => |v| return .{ .text = v },
        .null_literal => return .{ .null_val = {} },
        .column_ref => |ref| {
            for (columns, 0..) |col, i| {
                if (std.mem.eql(u8, col.name, ref.column)) {
                    return record.readColumn(payload, i) catch return record.Value{ .null_val = {} };
                }
            }
            return error.TypeError;
        },
        .placeholder => |idx| {
            if (current_bound_params) |params| {
                if (idx >= 1 and idx <= params.len) return params[idx - 1];
            }
            return error.TypeError;
        },
        .unary_op => |op| {
            if (op.op == .negate) {
                const inner = try resolveValueLazy(op.operand, columns, payload);
                return switch (inner) {
                    .integer => |v| record.Value{ .integer = -v },
                    .real => |v| record.Value{ .real = -v },
                    else => error.TypeError,
                };
            }
            return error.TypeError;
        },
        .paren => |inner| return resolveValueLazy(inner, columns, payload),
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
        .placeholder => |idx| {
            if (current_bound_params) |params| {
                if (idx >= 1 and idx <= params.len) return params[idx - 1];
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

/// Serialize column values from a record payload into sortable index key bytes.
/// For integers: big-endian i64 with sign bit flipped (preserves sort order).
/// For text: raw bytes (lexicographic order).
fn serializeIndexKeyFromPayload(payload: []const u8, col_indices: []const usize, key_buf: []u8) !usize {
    var pos: usize = 0;
    for (col_indices) |col_idx| {
        const val = record.readColumn(payload, col_idx) catch return error.SerializationFailed;
        switch (val) {
            .integer => |v| {
                if (pos + 8 > key_buf.len) return error.SerializationFailed;
                // Flip sign bit for sort-correct unsigned comparison
                const unsigned: u64 = @bitCast(v);
                const sortable = unsigned ^ (@as(u64, 1) << 63);
                std.mem.writeInt(u64, key_buf[pos..][0..8], sortable, .big);
                pos += 8;
            },
            .text => |t| {
                if (pos + t.len > key_buf.len) return error.SerializationFailed;
                @memcpy(key_buf[pos..][0..t.len], t);
                pos += t.len;
            },
            .real => |r| {
                if (pos + 8 > key_buf.len) return error.SerializationFailed;
                // IEEE 754 double -> sortable bytes
                const bits: u64 = @bitCast(r);
                const sortable = if (bits & (@as(u64, 1) << 63) != 0)
                    ~bits // negative: flip all bits
                else
                    bits ^ (@as(u64, 1) << 63); // positive: flip sign bit
                std.mem.writeInt(u64, key_buf[pos..][0..8], sortable, .big);
                pos += 8;
            },
            .null_val => {
                if (pos + 1 > key_buf.len) return error.SerializationFailed;
                key_buf[pos] = 0;
                pos += 1;
            },
            .blob => |b| {
                if (pos + b.len > key_buf.len) return error.SerializationFailed;
                @memcpy(key_buf[pos..][0..b.len], b);
                pos += b.len;
            },
        }
    }
    return pos;
}

// ── Multi-table expression evaluation (for JOINs) ────────────────────

fn resolveValueMultiTable(expr: *ast.Expr, tables: []const Executor.JoinTableCtx) !record.Value {
    switch (expr.*) {
        .integer_literal => |v| return .{ .integer = v },
        .real_literal => |v| return .{ .real = v },
        .string_literal => |v| return .{ .text = v },
        .null_literal => return .{ .null_val = {} },
        .placeholder => |idx| {
            if (current_bound_params) |params| {
                if (idx >= 1 and idx <= params.len) return params[idx - 1];
            }
            return error.TypeError;
        },
        .column_ref => |ref| {
            if (ref.table) |tbl_name| {
                // Qualified: table.column
                for (tables) |tctx| {
                    if (std.mem.eql(u8, tctx.name, tbl_name)) {
                        for (tctx.columns, 0..) |col, i| {
                            if (std.mem.eql(u8, col.name, ref.column)) {
                                return if (i < tctx.values.len) tctx.values[i] else record.Value{ .null_val = {} };
                            }
                        }
                        return error.TypeError; // column not found in qualified table
                    }
                }
                return error.TypeError; // table not found
            } else {
                // Unqualified: search all tables
                for (tables) |tctx| {
                    for (tctx.columns, 0..) |col, i| {
                        if (std.mem.eql(u8, col.name, ref.column)) {
                            return if (i < tctx.values.len) tctx.values[i] else record.Value{ .null_val = {} };
                        }
                    }
                }
                return error.TypeError;
            }
        },
        .unary_op => |op| {
            if (op.op == .negate) {
                const inner = try resolveValueMultiTable(op.operand, tables);
                return switch (inner) {
                    .integer => |v| record.Value{ .integer = -v },
                    .real => |v| record.Value{ .real = -v },
                    else => error.TypeError,
                };
            }
            return error.TypeError;
        },
        .paren => |inner| return resolveValueMultiTable(inner, tables),
        else => return error.TypeError,
    }
}

fn evalExprMultiTable(expr: *ast.Expr, tables: []const Executor.JoinTableCtx) !bool {
    switch (expr.*) {
        .binary_op => |op| {
            switch (op.op) {
                .eq => {
                    const left = try resolveValueMultiTable(op.left, tables);
                    const right = try resolveValueMultiTable(op.right, tables);
                    return valuesEqual(left, right);
                },
                .ne => {
                    const left = try resolveValueMultiTable(op.left, tables);
                    const right = try resolveValueMultiTable(op.right, tables);
                    return !valuesEqual(left, right);
                },
                .lt => {
                    const left = try resolveValueMultiTable(op.left, tables);
                    const right = try resolveValueMultiTable(op.right, tables);
                    return valueLess(left, right);
                },
                .gt => {
                    const left = try resolveValueMultiTable(op.left, tables);
                    const right = try resolveValueMultiTable(op.right, tables);
                    return valueLess(right, left);
                },
                .le => {
                    const left = try resolveValueMultiTable(op.left, tables);
                    const right = try resolveValueMultiTable(op.right, tables);
                    return valuesEqual(left, right) or valueLess(left, right);
                },
                .ge => {
                    const left = try resolveValueMultiTable(op.left, tables);
                    const right = try resolveValueMultiTable(op.right, tables);
                    return valuesEqual(left, right) or valueLess(right, left);
                },
                .@"and" => {
                    const left = try evalExprMultiTable(op.left, tables);
                    if (!left) return false;
                    return try evalExprMultiTable(op.right, tables);
                },
                .@"or" => {
                    const left = try evalExprMultiTable(op.left, tables);
                    if (left) return true;
                    return try evalExprMultiTable(op.right, tables);
                },
                else => return error.TypeError,
            }
        },
        .unary_op => |op| {
            if (op.op == .not) {
                return !(try evalExprMultiTable(op.operand, tables));
            }
            return error.TypeError;
        },
        .paren => |inner| return evalExprMultiTable(inner, tables),
        else => return error.TypeError,
    }
}

