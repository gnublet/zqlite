const std = @import("std");
const ast = @import("ast.zig");
const vm = @import("vm.zig");

/// Bytecode compiler — translates AST → VM instructions.
///
/// Phase 1 supports:
///   - SELECT constant expressions (e.g., SELECT 1 + 2)
///   - INSERT, UPDATE, DELETE stubs
///   - CREATE TABLE / DROP TABLE stubs

// ═══════════════════════════════════════════════════════════════════════════
// Errors
// ═══════════════════════════════════════════════════════════════════════════

pub const CodegenError = error{
    UnsupportedStatement,
    UnsupportedExpression,
    TooManyRegisters,
    AllocationFailed,
};

// ═══════════════════════════════════════════════════════════════════════════
// Compiler
// ═══════════════════════════════════════════════════════════════════════════

pub const Compiler = struct {
    instructions: std.ArrayList(vm.Instruction),
    column_names: std.ArrayList([]const u8),
    next_reg: u32,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .instructions = .{},
            .column_names = .{},
            .next_reg = 0,
            .allocator = allocator,
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
            .num_cursors = 0,
            .column_names = self.column_names.items,
        };
    }

    fn compileSelect(self: *Self, select: ast.Statement.Select) CodegenError!void {
        // For a constant-expression SELECT (no FROM), compile each expression
        // into registers and emit a result_row
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

    /// Compile an expression, placing its result in the next available register.
    /// Returns the register number.
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
                // For now, treat unresolved column refs as strings
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

    // ─── Register allocation ────────────────────────────────────────

    fn allocReg(self: *Self) u32 {
        const r = self.next_reg;
        self.next_reg += 1;
        return r;
    }

    // ─── Instruction emission ───────────────────────────────────────

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
