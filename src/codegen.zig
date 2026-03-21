const std = @import("std");
const ast = @import("ast.zig");
const vm = @import("vm.zig");
const schema = @import("schema.zig");
const planner = @import("planner.zig");

/// Bytecode compiler — translates AST → VM instructions.
///
/// Supports:
///   - SELECT with FROM, WHERE, column projection
///   - INSERT with values
///   - Constant expression SELECT (no FROM)

// ═══════════════════════════════════════════════════════════════════════════
// Errors
// ═══════════════════════════════════════════════════════════════════════════

pub const CodegenError = error{
    UnsupportedStatement,
    UnsupportedExpression,
    TooManyRegisters,
    AllocationFailed,
    TableNotFound,
};

// ═══════════════════════════════════════════════════════════════════════════
// Compiler
// ═══════════════════════════════════════════════════════════════════════════

pub const Compiler = struct {
    instructions: std.ArrayList(vm.Instruction),
    column_names: std.ArrayList([]const u8),
    next_reg: u32,
    num_cursors: u32,
    allocator: std.mem.Allocator,
    schema_cache: ?*const schema.Schema,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .instructions = .{},
            .column_names = .{},
            .next_reg = 0,
            .num_cursors = 0,
            .allocator = allocator,
            .schema_cache = null,
        };
    }

    pub fn initWithSchema(allocator: std.mem.Allocator, s: *const schema.Schema) Self {
        return .{
            .instructions = .{},
            .column_names = .{},
            .next_reg = 0,
            .num_cursors = 0,
            .allocator = allocator,
            .schema_cache = s,
        };
    }

    pub fn deinit(self: *Self) void {
        self.instructions.deinit(self.allocator);
        self.column_names.deinit(self.allocator);
    }

    /// Compile a statement into a VM program.
    pub fn compile(self: *Self, stmt: ast.Statement) CodegenError!vm.Program {
        switch (stmt) {
            .select => |s| try self.compileSelect(s),
            .insert => |i| try self.compileInsert(i),
            .update => try self.emitHalt(),
            .delete => try self.emitHalt(),
            .create_table => try self.emitHalt(),
            .drop_table => try self.emitHalt(),
            .create_index => try self.emitHalt(),
            .drop_index => try self.emitHalt(),
            .begin => try self.emitHalt(),
            .commit => try self.emitHalt(),
            .rollback => try self.emitHalt(),
            .explain => |inner| return self.compile(inner.*),
        }

        return .{
            .instructions = self.instructions.toOwnedSlice(self.allocator) catch return CodegenError.AllocationFailed,
            .num_registers = self.next_reg,
            .num_cursors = self.num_cursors,
            .column_names = self.column_names.toOwnedSlice(self.allocator) catch return CodegenError.AllocationFailed,
        };
    }

    fn compileSelect(self: *Self, select: ast.Statement.Select) CodegenError!void {
        // If we have a FROM clause and schema, compile a full table scan or index scan
        if (select.from) |from| {
            if (self.schema_cache) |sc| {
                if (sc.getTable(from.name)) |table_entry| {
                    var p = planner.Planner.init(self.allocator, sc);
                    const plan = p.planSelect(select) catch return CodegenError.AllocationFailed;
                    defer {
                        self.allocator.free(plan.scans);
                        self.allocator.free(plan.joins);
                    }

                    if (plan.joins.len > 0) {
                        try self.compileJoinScan(select, plan);
                        return;
                    }

                    if (plan.scans.len > 0) {
                        const scan = plan.scans[0];
                        if (scan.scan_type == .index_scan and scan.index_name != null and scan.index_key_expr != null) {
                            try self.compileIndexScan(select, table_entry, scan.index_name.?, scan.index_key_expr.?);
                            return;
                        } else if (scan.scan_type == .rowid_lookup and scan.index_key_expr != null) {
                            try self.compileRowidLookup(select, table_entry, scan.index_key_expr.?);
                            return;
                        }
                    }

                    try self.compileTableScan(select, table_entry);
                    return;
                } else {
                    return CodegenError.TableNotFound;
                }
            } else {
                return CodegenError.TableNotFound;
            }
        }

    // Fall back: constant-expression SELECT (no FROM)
        try self.compileConstantSelect(select);
    }

    fn compileJoinScan(self: *Self, select: ast.Statement.Select, plan: planner.QueryPlan) CodegenError!void {
        const join_plan = plan.joins[0]; // for now we only support 1 join (2 tables)
        const outer_scan = join_plan.outer;
        const inner_scan = join_plan.inner;
        
        const sc = self.schema_cache orelse return CodegenError.TableNotFound;
        const outer_table = sc.getTable(outer_scan.table_name) orelse return CodegenError.TableNotFound;
        const inner_table = sc.getTable(inner_scan.table_name) orelse return CodegenError.TableNotFound;
        
        const tables = [_]schema.Table{outer_table, inner_table};
        
        const outer_cursor: i32 = 0;
        const inner_cursor: i32 = 1;
        const inner_idx_cursor: i32 = 2;

        if (inner_scan.scan_type == .index_scan) {
            self.num_cursors = 3;
        } else {
            self.num_cursors = 2;
        }

        const cursors = [_]i32{outer_cursor, inner_cursor}; 

        // 1. Open outer table
        self.emit(.open_read, outer_cursor, @intCast(outer_table.root_page), 0) catch return CodegenError.AllocationFailed;

        // 2. Open inner table
        self.emit(.open_read, inner_cursor, @intCast(inner_table.root_page), 0) catch return CodegenError.AllocationFailed;
        
        // 3. Open inner index if applicable
        var inner_idx_opt: ?schema.Index = null;
        if (inner_scan.scan_type == .index_scan) {
            // Find index manually since we don't have getIndex by name direct wrapper
            var indices: [16]schema.Index = undefined;
            const total_indices = sc.indexesForTable(inner_table.name, &indices);
            for (indices[0..total_indices]) |idx| {
                if (std.mem.eql(u8, idx.name, inner_scan.index_name.?)) {
                    inner_idx_opt = idx;
                    break;
                }
            }
            const inner_idx = inner_idx_opt orelse return CodegenError.TableNotFound;
            self.emit(.open_read, inner_idx_cursor, @intCast(inner_idx.root_page), 0) catch return CodegenError.AllocationFailed;
        }

        // OUTER LOOP:
        const rewind_outer_pc = self.instructions.items.len;
        self.emit(.rewind, outer_cursor, 0, 0) catch return CodegenError.AllocationFailed;
        
        const loop_outer_pc: i32 = @intCast(self.instructions.items.len);

        var loop_inner_pc: i32 = 0;
        var rewind_inner_pc: usize = 0;
        var idx_search_pc: usize = 0;
        var seek_rowid_pc: usize = 0;
        var key_reg: u32 = 0;
        var on_jump_pc: ?usize = null;

        if (inner_scan.scan_type == .index_scan) {
            key_reg = try self.compileExprMulti(inner_scan.index_key_expr.?, &cursors, &tables);
            
            idx_search_pc = self.instructions.items.len;
            self.emit(.idx_search, inner_idx_cursor, @intCast(key_reg), 0) catch return CodegenError.AllocationFailed;
            
            loop_inner_pc = @intCast(self.instructions.items.len);
            
            const rowid_reg = self.allocReg();
            self.emit(.idx_rowid, inner_idx_cursor, @intCast(rowid_reg), 0) catch return CodegenError.AllocationFailed;
            
            self.emit(.seek_rowid, inner_cursor, @intCast(rowid_reg), 0) catch return CodegenError.AllocationFailed;
        } else if (inner_scan.scan_type == .rowid_lookup) {
            key_reg = try self.compileExprMulti(inner_scan.index_key_expr.?, &cursors, &tables);
            seek_rowid_pc = self.instructions.items.len;
            self.emit(.seek_rowid, inner_cursor, @intCast(key_reg), 0) catch return CodegenError.AllocationFailed;
        } else {
            rewind_inner_pc = self.instructions.items.len;
            self.emit(.rewind, inner_cursor, 0, 0) catch return CodegenError.AllocationFailed;
            
            loop_inner_pc = @intCast(self.instructions.items.len);
            
            if (join_plan.on_expr) |on_expr| {
                const cond_reg = try self.compileExprMulti(on_expr, &cursors, &tables);
                on_jump_pc = self.instructions.items.len;
                self.emit(.jump_if_not, @intCast(cond_reg), 0, 0) catch return CodegenError.AllocationFailed;
            }
        }
        
        // BOTH MATCH: collect columns and WHERE condition
        var where_jump_pc: ?usize = null;
        if (select.where) |where_expr| {
            const cond_reg = try self.compileExprMulti(where_expr, &cursors, &tables);
            where_jump_pc = self.instructions.items.len;
            self.emit(.jump_if_not, @intCast(cond_reg), 0, 0) catch return CodegenError.AllocationFailed;
        }

        // OUTPUT COLUMNS
        const start_reg = self.next_reg;
        var num_output_cols: u32 = 0;

        for (select.columns) |col| {
            switch (col) {
                .all_columns => {
                    for (outer_table.columns, 0..) |tc, ci| {
                        const reg = self.allocReg();
                        self.emit(.column, outer_cursor, @intCast(ci), @intCast(reg)) catch return CodegenError.AllocationFailed;
                        self.column_names.append(self.allocator, tc.name) catch return CodegenError.AllocationFailed;
                        num_output_cols += 1;
                    }
                    for (inner_table.columns, 0..) |tc, ci| {
                        const reg = self.allocReg();
                        self.emit(.column, inner_cursor, @intCast(ci), @intCast(reg)) catch return CodegenError.AllocationFailed;
                        self.column_names.append(self.allocator, tc.name) catch return CodegenError.AllocationFailed;
                        num_output_cols += 1;
                    }
                },
                .table_all => |tname| {
                    const ctx_table = if (std.mem.eql(u8, tname, outer_table.name)) outer_table else inner_table;
                    const ctx_cursor = if (std.mem.eql(u8, tname, outer_table.name)) outer_cursor else inner_cursor;
                    for (ctx_table.columns, 0..) |tc, ci| {
                        const reg = self.allocReg();
                        self.emit(.column, ctx_cursor, @intCast(ci), @intCast(reg)) catch return CodegenError.AllocationFailed;
                        self.column_names.append(self.allocator, tc.name) catch return CodegenError.AllocationFailed;
                        num_output_cols += 1;
                    }
                },
                .expr => |e| {
                    const reg = try self.compileExprMulti(e.expr, &cursors, &tables);
                    _ = reg;
                    const name = e.alias orelse exprName(e.expr);
                    self.column_names.append(self.allocator, name) catch return CodegenError.AllocationFailed;
                    num_output_cols += 1;
                },
            }
        }

        self.emit(.result_row, @intCast(start_reg), @intCast(num_output_cols), 0) catch return CodegenError.AllocationFailed;

        // NEXT_INNER:
        const next_inner_pc: i32 = @intCast(self.instructions.items.len);
        
        if (inner_scan.scan_type == .index_scan) {
            self.emit(.idx_next, inner_idx_cursor, @intCast(key_reg), loop_inner_pc) catch return CodegenError.AllocationFailed;
            self.instructions.items[@as(usize, @intCast(loop_inner_pc)) + 1].p3 = next_inner_pc; 
        } else if (inner_scan.scan_type == .rowid_lookup) {
            // Nothing to do for NEXT_INNER; fall through to NEXT_OUTER
        } else {
            self.emit(.next_row, inner_cursor, loop_inner_pc, 0) catch return CodegenError.AllocationFailed;
            self.instructions.items[rewind_inner_pc].p2 = next_inner_pc;
            if (on_jump_pc) |opc| {
                self.instructions.items[opc].p2 = next_inner_pc;
            }
        }
        
        if (where_jump_pc) |wpc| {
            self.instructions.items[wpc].p2 = next_inner_pc;
        }

        // NEXT_OUTER:
        const next_outer_pc: i32 = @intCast(self.instructions.items.len);
        if (inner_scan.scan_type == .index_scan) {
            self.instructions.items[idx_search_pc].p3 = next_outer_pc;
        } else if (inner_scan.scan_type == .rowid_lookup) {
            self.instructions.items[seek_rowid_pc].p3 = next_outer_pc;
        }
        
        self.emit(.next_row, outer_cursor, loop_outer_pc, 0) catch return CodegenError.AllocationFailed;

        // HALT:
        const halt_pc: i32 = @intCast(self.instructions.items.len);
        self.emit(.halt, 0, 0, 0) catch return CodegenError.AllocationFailed;
        
        self.instructions.items[rewind_outer_pc].p2 = halt_pc;
    }

    /// Compile a single-table rowid lookup
    fn compileRowidLookup(self: *Self, select: ast.Statement.Select, table: schema.Table, rowid_expr: *ast.Expr) CodegenError!void {
        const cursor_id: i32 = 0;
        self.num_cursors = 1;

        // 1. Evaluate rowid expression
        const key_reg = try self.compileExprForScan(rowid_expr, cursor_id, table);

        // 2. Open read cursor
        self.emit(.open_read, cursor_id, @intCast(table.root_page), 0) catch return CodegenError.AllocationFailed;

        // 3. seek_rowid, jump to HALT if not found
        const seek_pc: usize = self.instructions.items.len;
        self.emit(.seek_rowid, cursor_id, @intCast(key_reg), 0) catch return CodegenError.AllocationFailed;

        // 4. Output columns
        const start_reg = self.next_reg;
        var num_output_cols: u32 = 0;

        for (select.columns) |col| {
            switch (col) {
                .all_columns, .table_all => {
                    for (table.columns, 0..) |tc, ci| {
                        const reg = self.allocReg();
                        self.emit(.column, cursor_id, @intCast(ci), @intCast(reg)) catch return CodegenError.AllocationFailed;
                        self.column_names.append(self.allocator, tc.name) catch return CodegenError.AllocationFailed;
                        num_output_cols += 1;
                    }
                },
                .expr => |e| {
                    const reg = try self.compileExprForScan(e.expr, cursor_id, table);
                    _ = reg;
                    const name = e.alias orelse exprName(e.expr);
                    self.column_names.append(self.allocator, name) catch return CodegenError.AllocationFailed;
                    num_output_cols += 1;
                },
            }
        }

        // 5. result_row
        self.emit(.result_row, @intCast(start_reg), @intCast(num_output_cols), 0) catch return CodegenError.AllocationFailed;

        // HALT
        const halt_pc: i32 = @intCast(self.instructions.items.len);
        self.emit(.halt, 0, 0, 0) catch return CodegenError.AllocationFailed;

        // Patch seek_rowid jump target
        self.instructions.items[seek_pc].p3 = halt_pc;
    }

    /// Compile a full table scan SELECT:
    ///   open_read  0, root_page
    ///   rewind     0, HALT
    ///  LOOP:
    ///   column     0, col_idx, reg       (for each output column)
    ///   [WHERE: compare + jump_if_not → NEXT]
    ///   result_row start_reg, num_cols
    ///  NEXT:
    ///   next_row   0, LOOP
    ///  HALT:
    ///   halt
    fn compileTableScan(self: *Self, select: ast.Statement.Select, table: schema.Table) CodegenError!void {
        const cursor_id: i32 = 0;
        self.num_cursors = 1;

        // 1. open_read cursor 0 on root_page
        self.emit(.open_read, cursor_id, @intCast(table.root_page), 0) catch
            return CodegenError.AllocationFailed;

        // 2. rewind — jump to HALT if empty (we'll patch the target later)
        const rewind_pc = self.instructions.items.len;
        self.emit(.rewind, cursor_id, 0, 0) catch // p2 = placeholder, patched later
            return CodegenError.AllocationFailed;

        // Mark the start of the loop
        const loop_pc: i32 = @intCast(self.instructions.items.len);

        // 3. WHERE clause — compile to comparison + conditional jump
        //    If WHERE doesn't match, jump to NEXT (skip column extraction)
        var next_jump_pc: ?usize = null;
        if (select.where) |where_expr| {
            const cond_reg = try self.compileExprForScan(where_expr, cursor_id, table);
            // jump_if_not cond_reg → NEXT
            next_jump_pc = self.instructions.items.len;
            self.emit(.jump_if_not, @intCast(cond_reg), 0, 0) catch // p2 = placeholder
                return CodegenError.AllocationFailed;
        }

        // 4. Load output columns into registers
        const start_reg = self.next_reg;
        var num_output_cols: u32 = 0;

        for (select.columns) |col| {
            switch (col) {
                .all_columns, .table_all => {
                    // SELECT * — load all columns
                    for (table.columns, 0..) |tc, ci| {
                        const reg = self.allocReg();
                        self.emit(.column, cursor_id, @intCast(ci), @intCast(reg)) catch
                            return CodegenError.AllocationFailed;
                        self.column_names.append(self.allocator, tc.name) catch
                            return CodegenError.AllocationFailed;
                        num_output_cols += 1;
                    }
                },
                .expr => |e| {
                    const reg = try self.compileExprForScan(e.expr, cursor_id, table);
                    _ = reg;
                    const name = e.alias orelse exprName(e.expr);
                    self.column_names.append(self.allocator, name) catch
                        return CodegenError.AllocationFailed;
                    num_output_cols += 1;
                },
            }
        }

        // 5. result_row start_reg, num_output_cols
        self.emit(.result_row, @intCast(start_reg), @intCast(num_output_cols), 0) catch
            return CodegenError.AllocationFailed;

        // NEXT: next_row cursor → LOOP
        const next_pc: i32 = @intCast(self.instructions.items.len);
        self.emit(.next_row, cursor_id, loop_pc, 0) catch
            return CodegenError.AllocationFailed;

        // HALT
        const halt_pc: i32 = @intCast(self.instructions.items.len);
        self.emit(.halt, 0, 0, 0) catch
            return CodegenError.AllocationFailed;

        // Patch rewind jump target → HALT
        self.instructions.items[rewind_pc].p2 = halt_pc;

        // Patch WHERE jump target → NEXT (skip result_row)
        if (next_jump_pc) |jp| {
            self.instructions.items[jp].p2 = next_pc;
        }
    }

    /// Compile an index-driven SELECT scan:
    ///   open_read  0, table_root_page (table cursor)
    ///   open_read  1, index_root_page (index cursor)
    ///   evaluate key expr -> key_reg
    ///   idx_search 1, key_reg, HALT
    ///  LOOP:
    ///   idx_rowid  1, rowid_reg
    ///   seek_rowid 0, rowid_reg, NEXT
    ///   column     0, col_idx, reg       (for each output column)
    ///   [WHERE: conditional jump to NEXT]
    ///   result_row start_reg, num_cols
    ///  NEXT:
    ///   idx_next   1, key_reg, LOOP
    ///  HALT:
    ///   halt
    fn compileIndexScan(self: *Self, select: ast.Statement.Select, table: schema.Table, index_name: []const u8, key_expr: *ast.Expr) CodegenError!void {
        const sc = self.schema_cache orelse return CodegenError.TableNotFound;
        // Lookup index
        var target_idx: ?schema.Index = null;
        var indices: [16]schema.Index = undefined;
        const total_indices = sc.indexesForTable(table.name, &indices);
        for (indices[0..total_indices]) |idx| {
            if (std.mem.eql(u8, idx.name, index_name)) {
                target_idx = idx;
                break;
            }
        }
        
        const idx = target_idx orelse return CodegenError.TableNotFound;

        const table_cursor_id: i32 = 0;
        const idx_cursor_id: i32 = 1;
        self.num_cursors = 2;

        // 1. open_read table cursor 0 on table root_page
        self.emit(.open_read, table_cursor_id, @intCast(table.root_page), 0) catch return CodegenError.AllocationFailed;

        // 2. open_read index cursor 1 on index root_page
        self.emit(.open_read, idx_cursor_id, @intCast(idx.root_page), 0) catch return CodegenError.AllocationFailed;

        // 3. Evaluate key expr
        const key_reg = try self.compileExprForScan(key_expr, table_cursor_id, table);

        // 4. idx_search 1, key_reg, HALT
        const idx_search_pc = self.instructions.items.len;
        self.emit(.idx_search, idx_cursor_id, @intCast(key_reg), 0) catch return CodegenError.AllocationFailed; 
        
        // LOOP:
        const loop_pc: i32 = @intCast(self.instructions.items.len);

        // 5. idx_rowid 1 -> rowid_reg
        const rowid_reg = self.allocReg();
        self.emit(.idx_rowid, idx_cursor_id, @intCast(rowid_reg), 0) catch return CodegenError.AllocationFailed;

        // 6. seek_rowid 0, rowid_reg, NEXT
        const seek_rowid_pc = self.instructions.items.len;
        self.emit(.seek_rowid, table_cursor_id, @intCast(rowid_reg), 0) catch return CodegenError.AllocationFailed; 

        // 7. WHERE filter
        var next_jump_pc: ?usize = null;
        if (select.where) |where_expr| {
            const cond_reg = try self.compileExprForScan(where_expr, table_cursor_id, table);
            next_jump_pc = self.instructions.items.len;
            self.emit(.jump_if_not, @intCast(cond_reg), 0, 0) catch return CodegenError.AllocationFailed;
        }

        // 8. Extract columns
        const start_reg = self.next_reg;
        var num_output_cols: u32 = 0;

        for (select.columns) |col| {
            switch (col) {
                .all_columns, .table_all => {
                    for (table.columns, 0..) |tc, ci| {
                        const reg = self.allocReg();
                        self.emit(.column, table_cursor_id, @intCast(ci), @intCast(reg)) catch return CodegenError.AllocationFailed;
                        self.column_names.append(self.allocator, tc.name) catch return CodegenError.AllocationFailed;
                        num_output_cols += 1;
                    }
                },
                .expr => |e| {
                    const reg = try self.compileExprForScan(e.expr, table_cursor_id, table);
                    _ = reg;
                    const name = e.alias orelse exprName(e.expr);
                    self.column_names.append(self.allocator, name) catch return CodegenError.AllocationFailed;
                    num_output_cols += 1;
                },
            }
        }

        // 9. result_row
        self.emit(.result_row, @intCast(start_reg), @intCast(num_output_cols), 0) catch return CodegenError.AllocationFailed;

        // NEXT:
        const next_pc: i32 = @intCast(self.instructions.items.len);
        
        // idx_next 1, key_reg, LOOP
        self.emit(.idx_next, idx_cursor_id, @intCast(key_reg), @intCast(loop_pc)) catch return CodegenError.AllocationFailed;

        // HALT:
        const halt_pc: i32 = @intCast(self.instructions.items.len);
        self.emit(.halt, 0, 0, 0) catch return CodegenError.AllocationFailed;

        // Patch jumps
        self.instructions.items[idx_search_pc].p3 = halt_pc;
        self.instructions.items[seek_rowid_pc].p3 = next_pc;
        
        if (next_jump_pc) |jp| {
            self.instructions.items[jp].p2 = next_pc;
        }
    }

    /// Compile an expression that may reference table columns (for scan context)
    fn compileExprForScan(self: *Self, expr: *const ast.Expr, cursor_id: i32, table: schema.Table) CodegenError!u32 {
        const cursors = [_]i32{cursor_id};
        const tables = [_]schema.Table{table};
        return self.compileExprMulti(expr, &cursors, &tables);
    }

    /// Compile an expression that may reference columns from multiple tables (for joins)
    fn compileExprMulti(self: *Self, expr: *const ast.Expr, cursors: []const i32, tables: []const schema.Table) CodegenError!u32 {
        switch (expr.*) {
            .integer_literal => |v| {
                const reg = self.allocReg();
                self.emit(.integer, @intCast(reg), @intCast(v), 0) catch
                    return CodegenError.AllocationFailed;
                return reg;
            },
            .real_literal => |v| {
                const reg = self.allocReg();
                self.emitP4(.real, @intCast(reg), 0, 0, .{ .float = v }) catch
                    return CodegenError.AllocationFailed;
                return reg;
            },
            .string_literal => |s| {
                const reg = self.allocReg();
                self.emitP4(.string, @intCast(reg), 0, 0, .{ .str = s }) catch
                    return CodegenError.AllocationFailed;
                return reg;
            },
            .null_literal => {
                const reg = self.allocReg();
                self.emit(.null_val, @intCast(reg), 0, 0) catch
                    return CodegenError.AllocationFailed;
                return reg;
            },
            .column_ref => |ref| {
                if (ref.table) |tname| {
                    for (tables, 0..) |t, i| {
                        if (std.mem.eql(u8, t.name, tname)) {
                            if (resolveColumnByName(ref.column, t.columns)) |idx| {
                                const reg = self.allocReg();
                                self.emit(.column, cursors[i], @intCast(idx), @intCast(reg)) catch return CodegenError.AllocationFailed;
                                return reg;
                            }
                        }
                    }
                } else {
                    for (tables, 0..) |t, i| {
                        if (resolveColumnByName(ref.column, t.columns)) |idx| {
                            const reg = self.allocReg();
                            self.emit(.column, cursors[i], @intCast(idx), @intCast(reg)) catch return CodegenError.AllocationFailed;
                            return reg;
                        }
                    }
                }
                const reg = self.allocReg();
                self.emitP4(.string, @intCast(reg), 0, 0, .{ .str = ref.column }) catch return CodegenError.AllocationFailed;
                return reg;
            },
            .binary_op => |bin| {
                const left_reg = try self.compileExprMulti(bin.left, cursors, tables);
                const right_reg = try self.compileExprMulti(bin.right, cursors, tables);
                const result_reg = self.allocReg();
                const opcode: vm.Opcode = switch (bin.op) {
                    .add => .add,
                    .sub => .sub,
                    .mul => .mul,
                    .div => .div,
                    .mod => .mod,
                    .eq => .eq,
                    .ne => .ne,
                    .lt => .lt,
                    .le => .le,
                    .gt => .gt,
                    .ge => .ge,
                    .@"and" => .@"and",
                    .@"or" => .@"or",
                    .concat => .concat,
                };
                self.emit(opcode, @intCast(left_reg), @intCast(right_reg), @intCast(result_reg)) catch
                    return CodegenError.AllocationFailed;
                return result_reg;
            },
            .unary_op => |un| {
                const operand_reg = try self.compileExprMulti(un.operand, cursors, tables);
                const result_reg = self.allocReg();
                const opcode: vm.Opcode = switch (un.op) {
                    .negate => .negate,
                    .not => .not,
                };
                self.emit(opcode, @intCast(operand_reg), @intCast(result_reg), 0) catch
                    return CodegenError.AllocationFailed;
                return result_reg;
            },
            .paren => |inner| {
                return self.compileExprMulti(inner, cursors, tables);
            },
            .placeholder => |idx| {
                // Bind parameter: emit blob opcode to load from bound_params
                const reg = self.allocReg();
                self.emit(.blob, @intCast(reg), @intCast(idx), 0) catch
                    return CodegenError.AllocationFailed;
                return reg;
            },
            else => return CodegenError.UnsupportedExpression,
        }
    }

    /// Constant-expression SELECT (no FROM)
    fn compileConstantSelect(self: *Self, select: ast.Statement.Select) CodegenError!void {
        const start_reg = self.next_reg;
        var num_cols: u32 = 0;

        for (select.columns) |col| {
            switch (col) {
                .expr => |e| {
                    _ = try self.compileExpr(e.expr);
                    const name = e.alias orelse "?column?";
                    self.column_names.append(self.allocator, name) catch return CodegenError.AllocationFailed;
                    num_cols += 1;
                },
                .all_columns => {
                    self.column_names.append(self.allocator, "*") catch return CodegenError.AllocationFailed;
                    num_cols += 1;
                },
                .table_all => {
                    num_cols += 1;
                },
            }
        }

        if (num_cols > 0) {
            self.emit(.result_row, @intCast(start_reg), @intCast(num_cols), 0) catch
                return CodegenError.AllocationFailed;
        }

        try self.emitHalt();
    }

    /// Compile an expression (constant context, no table scan)
    fn compileExpr(self: *Self, expr: *const ast.Expr) CodegenError!u32 {
        const reg = self.allocReg();

        switch (expr.*) {
            .integer_literal => |v| {
                self.emit(.integer, @intCast(reg), @intCast(v), 0) catch
                    return CodegenError.AllocationFailed;
            },
            .real_literal => |v| {
                self.emitP4(.real, @intCast(reg), 0, 0, .{ .float = v }) catch
                    return CodegenError.AllocationFailed;
            },
            .string_literal => |s| {
                self.emitP4(.string, @intCast(reg), 0, 0, .{ .str = s }) catch
                    return CodegenError.AllocationFailed;
            },
            .null_literal => {
                self.emit(.null_val, @intCast(reg), 0, 0) catch
                    return CodegenError.AllocationFailed;
            },
            .placeholder => |idx| {
                self.emit(.blob, @intCast(reg), @intCast(idx), 0) catch
                    return CodegenError.AllocationFailed;
            },
            .binary_op => |bin| {
                const left_reg = try self.compileExpr(bin.left);
                const right_reg = try self.compileExpr(bin.right);
                const opcode: vm.Opcode = switch (bin.op) {
                    .add => .add,
                    .sub => .sub,
                    .mul => .mul,
                    .div => .div,
                    .mod => .mod,
                    .eq => .eq,
                    .ne => .ne,
                    .lt => .lt,
                    .le => .le,
                    .gt => .gt,
                    .ge => .ge,
                    .@"and" => .@"and",
                    .@"or" => .@"or",
                    .concat => .concat,
                };
                self.emit(opcode, @intCast(left_reg), @intCast(right_reg), @intCast(reg)) catch
                    return CodegenError.AllocationFailed;
            },
            .unary_op => |un| {
                const operand_reg = try self.compileExpr(un.operand);
                const opcode: vm.Opcode = switch (un.op) {
                    .negate => .negate,
                    .not => .not,
                };
                self.emit(opcode, @intCast(operand_reg), @intCast(reg), 0) catch
                    return CodegenError.AllocationFailed;
            },
            .column_ref => |ref| {
                self.emitP4(.string, @intCast(reg), 0, 0, .{ .str = ref.column }) catch
                    return CodegenError.AllocationFailed;
            },
            .paren => |inner| {
                const inner_reg = try self.compileExpr(inner);
                if (inner_reg != reg) {
                    self.emit(.copy, @intCast(inner_reg), @intCast(reg), 0) catch
                        return CodegenError.AllocationFailed;
                }
            },
            else => return CodegenError.UnsupportedExpression,
        }

        return reg;
    }

    // ─── INSERT Support ──────────────────────────────────────────────

    fn compileInsert(self: *Self, insert: ast.Statement.Insert) CodegenError!void {
        const sc = self.schema_cache orelse return CodegenError.UnsupportedStatement;
        const table = sc.getTable(insert.table) orelse return CodegenError.TableNotFound;

        // Currently we only support VM insert if rowid is aliased to a column (INTEGER PRIMARY KEY)
        // and we are inserting all columns implicitly (no explicit column list)
        const pk_idx = table.rowid_alias_col orelse return CodegenError.UnsupportedStatement;
        if (insert.columns != null) return CodegenError.UnsupportedStatement;

        const cursor_id: i32 = 0;
        self.num_cursors = 1;

        // open_write cursor 0 on root_page
        self.emit(.open_write, cursor_id, @intCast(table.root_page), 0) catch
            return CodegenError.AllocationFailed;

        // For each row being inserted
        for (insert.values) |value_row| {
            if (value_row.len != table.columns.len) {
                return CodegenError.UnsupportedStatement;
            }

            // Reserve contiguous registers for the record payload
            const start_reg = self.next_reg;
            self.next_reg += @intCast(table.columns.len);

            // Compile each expression, copying into the reserved block if necessary
            for (value_row, 0..) |expr, i| {
                const target_reg: i32 = @intCast(start_reg + i);
                const val_reg = try self.compileExpr(expr);
                if (val_reg != target_reg) {
                    self.emit(.copy, @intCast(val_reg), target_reg, 0) catch
                        return CodegenError.AllocationFailed;
                }
            }

            // make_record start_reg, num_cols, rec_reg
            const rec_reg = self.allocReg();
            self.emit(.make_record, @intCast(start_reg), @intCast(table.columns.len), @intCast(rec_reg)) catch
                return CodegenError.AllocationFailed;

            // insert cursor_id, rec_reg, rowid_reg
            const rowid_reg: i32 = @intCast(start_reg + pk_idx);
            self.emit(.insert, cursor_id, @intCast(rec_reg), rowid_reg) catch
                return CodegenError.AllocationFailed;
        }

        try self.emitHalt();
    }

    // ─── Helpers ─────────────────────────────────────────────────────

    fn resolveColumnByName(name: []const u8, columns: []const schema.Column) ?usize {
        for (columns, 0..) |col, i| {
            if (std.mem.eql(u8, col.name, name)) return i;
        }
        return null;
    }

    fn exprName(expr: *const ast.Expr) []const u8 {
        return switch (expr.*) {
            .column_ref => |r| r.column,
            .integer_literal => "?",
            .string_literal => |s| s,
            else => "?expr?",
        };
    }

    fn allocReg(self: *Self) u32 {
        const r = self.next_reg;
        self.next_reg += 1;
        return r;
    }

    fn emit(self: *Self, opcode: vm.Opcode, p1: i32, p2: i32, p3: i32) !void {
        try self.instructions.append(self.allocator, vm.Instruction.init(opcode, p1, p2, p3));
    }

    fn emitP4(self: *Self, opcode: vm.Opcode, p1: i32, p2: i32, p3: i32, p4: vm.Value) !void {
        try self.instructions.append(self.allocator, vm.Instruction.withP4(opcode, p1, p2, p3, p4));
    }

    fn emitHalt(self: *Self) CodegenError!void {
        self.emit(.halt, 0, 0, 0) catch return CodegenError.AllocationFailed;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

test "compile SELECT 1 + 2" {
    const parser = @import("parser.zig");

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = parser.Parser.init("SELECT 1 + 2;", arena.allocator());
    const stmt = try p.parseStatement();

    var compiler = Compiler.init(std.testing.allocator);
    defer compiler.deinit();

    const program = try compiler.compile(stmt);

    // Execute
    var vm_inst = try vm.VM.init(std.testing.allocator, program);
    defer vm_inst.deinit();
    try std.testing.expectEqual(vm.StepResult.row, try vm_inst.step());
    const start: usize = @intCast(vm_inst.program.instructions[vm_inst.pc - 1].p1);
    try std.testing.expectEqual(@as(i64, 3), vm_inst.registers[start].integer);
    try std.testing.expectEqual(vm.StepResult.done, try vm_inst.step());
}

test "compile SELECT with string" {
    const parser = @import("parser.zig");

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var p = parser.Parser.init("SELECT 'hello';", arena.allocator());
    const stmt = try p.parseStatement();

    var compiler = Compiler.init(std.testing.allocator);
    defer compiler.deinit();

    const program = try compiler.compile(stmt);

    var vm_inst = try vm.VM.init(std.testing.allocator, program);
    defer vm_inst.deinit();
    try std.testing.expectEqual(vm.StepResult.row, try vm_inst.step());
    const start: usize = @intCast(vm_inst.program.instructions[vm_inst.pc - 1].p1);
    try std.testing.expectEqualStrings("hello", vm_inst.registers[start].text);
    try std.testing.expectEqual(vm.StepResult.done, try vm_inst.step());
}
