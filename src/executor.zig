const std = @import("std");
const ast = @import("ast.zig");
const btree = @import("btree.zig");
const cursor = @import("cursor.zig");
const journal_mod = @import("journal.zig");
const os = @import("os.zig");
const pager = @import("pager.zig");
const record = @import("record.zig");
const schema = @import("schema.zig");
const vm_mod = @import("vm.zig");
const planner = @import("planner.zig");
const codegen = @import("codegen.zig");

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
    data_version: u64,
    vm_program_cache: std.AutoHashMap(usize, vm_mod.Program),

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
            .data_version = 0,
            .vm_program_cache = std.AutoHashMap(usize, vm_mod.Program).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        var iter = self.vm_program_cache.iterator();
        while (iter.next()) |entry| {
            self.allocator.free(entry.value_ptr.instructions);
            self.allocator.free(entry.value_ptr.column_names);
        }
        self.vm_program_cache.deinit();
    }

    /// Invalidate all cached plans (called on DDL)
    fn invalidatePlans(self: *Self) void {
        self.schema_version +%= 1;

        var iter = self.vm_program_cache.iterator();
        while (iter.next()) |entry| {
            self.allocator.free(entry.value_ptr.instructions);
            self.allocator.free(entry.value_ptr.column_names);
        }
        self.vm_program_cache.clearRetainingCapacity();
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

    pub const Statement = struct {
        vm: *vm_mod.VM,
        arena: *std.heap.ArenaAllocator,
        parent_allocator: std.mem.Allocator,
        row_buf: []record.Value,
        column_names: [][]const u8,

        pub fn deinit(self: *Statement) void {
            self.vm.deinit();
            self.arena.deinit();
            self.parent_allocator.destroy(self.arena);
        }

        pub fn step(self: *Statement) !?[]const record.Value {
            switch (try self.vm.step()) {
                .done => return null,
                .row => {
                    const instr = self.vm.program.instructions[self.vm.pc - 1];
                    std.debug.assert(instr.opcode == .result_row);
                    const start: usize = @intCast(instr.p1);
                    const count: usize = @intCast(instr.p2);
                    std.debug.assert(count == self.row_buf.len);

                    for (0..count) |i| {
                        const reg = self.vm.registers[start + i];
                        self.row_buf[i] = switch (reg) {
                            .null_val => .{ .null_val = {} },
                            .integer => |v| .{ .integer = v },
                            .real => |v| .{ .real = v },
                            .text => |v| .{ .text = v },
                            .blob => |v| .{ .text = v },
                            .boolean => |v| .{ .integer = if (v) 1 else 0 },
                        };
                    }
                    return self.row_buf;
                },
            }
        }

        pub fn reset(self: *Statement) void {
            self.vm.pc = 0;
            self.vm.halted = false;
            // Also need to clear cursors
            for (&self.vm.cursors) |*cursor_opt| {
                cursor_opt.* = null;
            }
        }

        pub fn bindParams(self: *Statement, params: ?[]const record.Value) void {
            self.vm.bound_params = params;
        }
    };

    /// Prepare a statement into a bytecode string for streaming execution (VDBE-like).
    pub fn prepare(self: *Self, stmt: ast.Statement, params: ?[]const record.Value) ExecError!Statement {
        const cache_key: usize = switch (stmt) {
            .select => |s| @intFromPtr(s.columns.ptr),
            .insert => |i| @intFromPtr(i.table.ptr),
            .update => |u| @intFromPtr(u.table.ptr),
            .delete => |d| @intFromPtr(d.table.ptr),
            else => return ExecError.StorageError, // unsupported for vm via cache right now
        };

        var program: vm_mod.Program = undefined;
        if (self.vm_program_cache.get(cache_key)) |p| {
            program = p;
        } else {
            var compiler = codegen.Compiler.initWithSchema(self.allocator, self.schema_store);
            errdefer compiler.deinit();
            
            program = compiler.compile(stmt) catch |err| switch (err) {
                error.TableNotFound => return ExecError.TableNotFound,
                error.UnsupportedStatement => return ExecError.UnsupportedStatement,
                error.UnsupportedExpression => return ExecError.UnsupportedExpr,
                error.TooManyRegisters => return ExecError.StorageError,
                error.AllocationFailed => return ExecError.StorageError,
            };
            
            self.vm_program_cache.put(cache_key, program) catch {
                compiler.deinit();
                return ExecError.StorageError;
            };
        }

        const arena = self.allocator.create(std.heap.ArenaAllocator) catch return ExecError.StorageError;
        arena.* = std.heap.ArenaAllocator.init(self.allocator);
        errdefer {
            arena.deinit();
            self.allocator.destroy(arena);
        }

        var vm_inst = arena.allocator().create(vm_mod.VM) catch return ExecError.StorageError;
        vm_inst.* = vm_mod.VM.init(arena.allocator(), program) catch return ExecError.StorageError;
        vm_inst.pool = self.pool;
        vm_inst.bound_params = params;

        const row_buf = arena.allocator().alloc(record.Value, program.column_names.len) catch return ExecError.StorageError;

        return Statement{
            .vm = vm_inst,
            .arena = arena,
            .parent_allocator = self.allocator,
            .row_buf = row_buf,
            .column_names = @constCast(program.column_names),
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
            .select => |sel| {
                var stmt_iter = try self.prepare(ast.Statement{ .select = sel }, self.bound_params);
                defer stmt_iter.deinit();

                var rows_list: std.ArrayList(ResultRow) = .{};
                while (try stmt_iter.step()) |row_vals| {
                    const vals_dupe = arena.alloc(Value, row_vals.len) catch return ExecError.StorageError;
                    for (row_vals, 0..) |rv, i| {
                        vals_dupe[i] = recordToValue(rv);
                    }
                    rows_list.append(arena, .{ .values = vals_dupe }) catch return ExecError.StorageError;
                }

                const col_names = arena.alloc([]const u8, stmt_iter.vm.program.column_names.len) catch return ExecError.StorageError;
                for (stmt_iter.vm.program.column_names, 0..) |name, i| col_names[i] = name;

                return ExecResult{
                    .rows = rows_list.items,
                    .column_names = col_names,
                    .rows_affected = 0,
                    .message = null,
                };
            },
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

        const owned_name = self.allocator.dupe(u8, ci.name) catch return ExecError.StorageError;
        const owned_table = self.allocator.dupe(u8, ci.table) catch return ExecError.StorageError;
        const owned_columns = self.allocator.alloc([]const u8, ci.columns.len) catch return ExecError.StorageError;
        for (ci.columns, 0..) |col_name, idx| {
            owned_columns[idx] = self.allocator.dupe(u8, col_name) catch return ExecError.StorageError;
        }

        self.schema_store.addIndex(.{
            .name = owned_name,
            .table_name = owned_table,
            .columns = owned_columns,
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

