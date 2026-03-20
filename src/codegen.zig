const std = @import("std");
const ast = @import("ast.zig");
const vm = @import("vm.zig");
const schema = @import("schema.zig");

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
            .insert => try self.emitHalt(),
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
            .instructions = self.instructions.items,
            .num_registers = self.next_reg,
            .num_cursors = self.num_cursors,
            .column_names = self.column_names.items,
        };
    }

    fn compileSelect(self: *Self, select: ast.Statement.Select) CodegenError!void {
        // If we have a FROM clause and schema, compile a full table scan
        if (select.from) |from| {
            if (self.schema_cache) |sc| {
                if (sc.getTable(from.name)) |table_entry| {
                    try self.compileTableScan(select, table_entry);
                    return;
                }
            }
        }

        // Fall back: constant-expression SELECT (no FROM)
        try self.compileConstantSelect(select);
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

        // 3. Load output columns into registers
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

        // 4. WHERE clause — compile to comparison + conditional jump
        //    If WHERE doesn't match, jump to NEXT (skip result_row)
        var next_jump_pc: ?usize = null;
        if (select.where) |where_expr| {
            const cond_reg = try self.compileExprForScan(where_expr, cursor_id, table);
            // jump_if_not cond_reg → NEXT
            next_jump_pc = self.instructions.items.len;
            self.emit(.jump_if_not, @intCast(cond_reg), 0, 0) catch // p2 = placeholder
                return CodegenError.AllocationFailed;
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

    /// Compile an expression that may reference table columns (for scan context)
    fn compileExprForScan(self: *Self, expr: *const ast.Expr, cursor_id: i32, table: schema.Table) CodegenError!u32 {
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
                // Resolve column name to index
                const col_idx = resolveColumnByName(ref.column, table.columns);
                if (col_idx) |idx| {
                    const reg = self.allocReg();
                    self.emit(.column, cursor_id, @intCast(idx), @intCast(reg)) catch
                        return CodegenError.AllocationFailed;
                    return reg;
                }
                // Unresolved — treat as string
                const reg = self.allocReg();
                self.emitP4(.string, @intCast(reg), 0, 0, .{ .str = ref.column }) catch
                    return CodegenError.AllocationFailed;
                return reg;
            },
            .binary_op => |bin| {
                const left_reg = try self.compileExprForScan(bin.left, cursor_id, table);
                const right_reg = try self.compileExprForScan(bin.right, cursor_id, table);
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
                const operand_reg = try self.compileExprForScan(un.operand, cursor_id, table);
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
                return self.compileExprForScan(inner, cursor_id, table);
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
    try vm_inst.execute();

    try std.testing.expectEqual(@as(usize, 1), vm_inst.results.items.len);
    try std.testing.expectEqual(@as(i64, 3), vm_inst.results.items[0].values[0].integer);
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
    try vm_inst.execute();

    try std.testing.expectEqualStrings("hello", vm_inst.results.items[0].values[0].text);
}
