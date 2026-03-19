const std = @import("std");
const record = @import("record.zig");

/// Register-based Virtual Machine for ZQLite.
///
/// Executes bytecode programs compiled from SQL statements.
/// Uses a register file (not a stack) for operands, and a tight
/// switch-based dispatch loop.

// ═══════════════════════════════════════════════════════════════════════════
// Opcodes
// ═══════════════════════════════════════════════════════════════════════════

pub const Opcode = enum(u8) {
    // Control flow
    halt = 0,
    jump = 1,
    jump_if = 2,
    jump_if_not = 3,

    // Constants
    integer = 10,
    string = 11,
    real = 12,
    null_val = 13,
    blob = 14,

    // Comparison
    compare = 20,
    eq = 21,
    ne = 22,
    lt = 23,
    le = 24,
    gt = 25,
    ge = 26,

    // Arithmetic
    add = 30,
    sub = 31,
    mul = 32,
    div = 33,
    mod = 34,
    negate = 35,

    // Logical
    @"and" = 40,
    @"or" = 41,
    not = 42,

    // Table operations
    open_read = 50,
    open_write = 51,
    close_cursor = 52,
    rewind = 53,
    next_row = 54,
    prev_row = 55,
    column = 56,
    rowid = 57,

    // Result
    result_row = 60,

    // Record construction
    make_record = 70,
    insert = 71,
    delete = 72,

    // Index operations
    idx_insert = 80,
    idx_search = 81,

    // Aggregation
    agg_step = 90,
    agg_final = 91,

    // Sort
    sorter_open = 100,
    sorter_insert = 101,
    sorter_sort = 102,
    sorter_data = 103,
    sorter_next = 104,

    // Transaction control
    transaction = 110,
    commit = 111,
    rollback = 112,

    // Copy register
    copy = 120,
    move = 121,

    // String ops
    concat = 130,
    like = 131,
};

// ═══════════════════════════════════════════════════════════════════════════
// Instruction
// ═══════════════════════════════════════════════════════════════════════════

pub const Instruction = struct {
    opcode: Opcode,
    p1: i32, // first operand (register or immediate)
    p2: i32, // second operand (register, jump target, or immediate)
    p3: i32, // third operand
    p4: Value, // inline constant (string, etc.)

    pub fn init(opcode: Opcode, p1: i32, p2: i32, p3: i32) Instruction {
        return .{ .opcode = opcode, .p1 = p1, .p2 = p2, .p3 = p3, .p4 = .{ .none = {} } };
    }

    pub fn withP4(opcode: Opcode, p1: i32, p2: i32, p3: i32, p4: Value) Instruction {
        return .{ .opcode = opcode, .p1 = p1, .p2 = p2, .p3 = p3, .p4 = p4 };
    }
};

pub const Value = union(enum) {
    none: void,
    int: i64,
    float: f64,
    str: []const u8,
    bytes: []const u8,
};

// ═══════════════════════════════════════════════════════════════════════════
// Program
// ═══════════════════════════════════════════════════════════════════════════

pub const Program = struct {
    instructions: []const Instruction,
    num_registers: u32,
    num_cursors: u32,

    /// Column names for result rows.
    column_names: []const []const u8,
};

// ═══════════════════════════════════════════════════════════════════════════
// Register values
// ═══════════════════════════════════════════════════════════════════════════

pub const Register = union(enum) {
    null_val: void,
    integer: i64,
    real: f64,
    text: []const u8,
    blob: []const u8,
    boolean: bool,

    pub fn toRecordValue(self: Register) record.Value {
        return switch (self) {
            .null_val => .{ .null_val = {} },
            .integer => |v| .{ .integer = v },
            .real => |v| .{ .real = v },
            .text => |v| .{ .text = v },
            .blob => |v| .{ .blob = v },
            .boolean => |v| .{ .integer = if (v) 1 else 0 },
        };
    }

    pub fn isTrue(self: Register) bool {
        return switch (self) {
            .null_val => false,
            .integer => |v| v != 0,
            .real => |v| v != 0.0,
            .text => |v| v.len > 0,
            .blob => |v| v.len > 0,
            .boolean => |v| v,
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// VM — the execution engine
// ═══════════════════════════════════════════════════════════════════════════

pub const ResultRow = struct {
    values: []Register,
};

pub const VM = struct {
    registers: []Register,
    pc: u32,
    program: Program,
    results: std.ArrayList(ResultRow),
    allocator: std.mem.Allocator,
    halted: bool,

    const Self = @This();

    /// Create a new VM for the given program.
    pub fn init(allocator: std.mem.Allocator, program: Program) !Self {
        const regs = try allocator.alloc(Register, program.num_registers);
        @memset(regs, .{ .null_val = {} });

        return Self{
            .registers = regs,
            .pc = 0,
            .program = program,
            .results = .{},
            .allocator = allocator,
            .halted = false,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.results.items) |row| {
            self.allocator.free(row.values);
        }
        self.results.deinit(self.allocator);
        self.allocator.free(self.registers);
    }

    /// Execute the program to completion.
    pub fn execute(self: *Self) !void {
        while (!self.halted and self.pc < self.program.instructions.len) {
            try self.step();
        }
    }

    /// Execute a single instruction.
    pub fn step(self: *Self) !void {
        if (self.pc >= self.program.instructions.len) {
            self.halted = true;
            return;
        }

        const instr = self.program.instructions[self.pc];
        self.pc += 1;

        switch (instr.opcode) {
            .halt => {
                self.halted = true;
            },
            .integer => {
                const reg: usize = @intCast(instr.p1);
                self.registers[reg] = .{ .integer = @as(i64, instr.p2) };
            },
            .string => {
                const reg: usize = @intCast(instr.p1);
                switch (instr.p4) {
                    .str => |s| self.registers[reg] = .{ .text = s },
                    else => self.registers[reg] = .{ .text = "" },
                }
            },
            .real => {
                const reg: usize = @intCast(instr.p1);
                switch (instr.p4) {
                    .float => |f| self.registers[reg] = .{ .real = f },
                    else => self.registers[reg] = .{ .real = 0.0 },
                }
            },
            .null_val => {
                const reg: usize = @intCast(instr.p1);
                self.registers[reg] = .{ .null_val = {} };
            },
            .add, .sub, .mul, .div, .mod => {
                const dst: usize = @intCast(instr.p3);
                const a = self.registers[@intCast(instr.p1)];
                const b = self.registers[@intCast(instr.p2)];
                self.registers[dst] = try self.arithmetic(instr.opcode, a, b);
            },
            .negate => {
                const dst: usize = @intCast(instr.p2);
                const a = self.registers[@intCast(instr.p1)];
                self.registers[dst] = switch (a) {
                    .integer => |v| .{ .integer = -v },
                    .real => |v| .{ .real = -v },
                    else => .{ .null_val = {} },
                };
            },
            .eq, .ne, .lt, .le, .gt, .ge => {
                const a = self.registers[@intCast(instr.p1)];
                const b = self.registers[@intCast(instr.p2)];
                const dst: usize = @intCast(instr.p3);
                self.registers[dst] = .{ .boolean = self.compare(instr.opcode, a, b) };
            },
            .@"and" => {
                const a = self.registers[@intCast(instr.p1)];
                const b = self.registers[@intCast(instr.p2)];
                const dst: usize = @intCast(instr.p3);
                self.registers[dst] = .{ .boolean = a.isTrue() and b.isTrue() };
            },
            .@"or" => {
                const a = self.registers[@intCast(instr.p1)];
                const b = self.registers[@intCast(instr.p2)];
                const dst: usize = @intCast(instr.p3);
                self.registers[dst] = .{ .boolean = a.isTrue() or b.isTrue() };
            },
            .not => {
                const a = self.registers[@intCast(instr.p1)];
                const dst: usize = @intCast(instr.p2);
                self.registers[dst] = .{ .boolean = !a.isTrue() };
            },
            .jump => {
                self.pc = @intCast(instr.p1);
            },
            .jump_if => {
                const cond = self.registers[@intCast(instr.p1)];
                if (cond.isTrue()) {
                    self.pc = @intCast(instr.p2);
                }
            },
            .jump_if_not => {
                const cond = self.registers[@intCast(instr.p1)];
                if (!cond.isTrue()) {
                    self.pc = @intCast(instr.p2);
                }
            },
            .result_row => {
                const start: usize = @intCast(instr.p1);
                const count: usize = @intCast(instr.p2);
                const vals = try self.allocator.alloc(Register, count);
                @memcpy(vals, self.registers[start .. start + count]);
                try self.results.append(self.allocator, .{ .values = vals });
            },
            .copy => {
                const src: usize = @intCast(instr.p1);
                const dst: usize = @intCast(instr.p2);
                self.registers[dst] = self.registers[src];
            },

            // Stubs for operations that integrate with B-tree/pager
            .open_read, .open_write, .close_cursor, .rewind, .next_row, .prev_row, .column, .rowid, .make_record, .insert, .delete, .idx_insert, .idx_search, .agg_step, .agg_final, .sorter_open, .sorter_insert, .sorter_sort, .sorter_data, .sorter_next, .transaction, .commit, .rollback, .move, .concat, .like, .blob, .compare => {
                // Will be implemented as we integrate with the storage engine
            },
        }
    }

    // ─── Arithmetic helpers ─────────────────────────────────────────

    fn arithmetic(self: *Self, op: Opcode, a: Register, b: Register) !Register {
        _ = self;
        // Promote to real if either operand is real
        const a_val: f64 = switch (a) {
            .integer => |v| @floatFromInt(v),
            .real => |v| v,
            else => return .{ .null_val = {} },
        };
        const b_val: f64 = switch (b) {
            .integer => |v| @floatFromInt(v),
            .real => |v| v,
            else => return .{ .null_val = {} },
        };

        const both_int = (a == .integer) and (b == .integer);

        const result: f64 = switch (op) {
            .add => a_val + b_val,
            .sub => a_val - b_val,
            .mul => a_val * b_val,
            .div => if (b_val != 0) a_val / b_val else return .{ .null_val = {} },
            .mod => if (b_val != 0) @mod(a_val, b_val) else return .{ .null_val = {} },
            else => unreachable,
        };

        if (both_int and op != .div) {
            return .{ .integer = @intFromFloat(result) };
        }
        return .{ .real = result };
    }

    fn compare(_: *Self, op: Opcode, a: Register, b: Register) bool {
        // Compare integers
        if (a == .integer and b == .integer) {
            const av = a.integer;
            const bv = b.integer;
            return switch (op) {
                .eq => av == bv,
                .ne => av != bv,
                .lt => av < bv,
                .le => av <= bv,
                .gt => av > bv,
                .ge => av >= bv,
                else => false,
            };
        }

        // Compare as float
        const av: f64 = switch (a) {
            .integer => |v| @floatFromInt(v),
            .real => |v| v,
            else => return false,
        };
        const bv: f64 = switch (b) {
            .integer => |v| @floatFromInt(v),
            .real => |v| v,
            else => return false,
        };

        return switch (op) {
            .eq => av == bv,
            .ne => av != bv,
            .lt => av < bv,
            .le => av <= bv,
            .gt => av > bv,
            .ge => av >= bv,
            else => false,
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

test "VM basic arithmetic" {
    const program = Program{
        .instructions = &[_]Instruction{
            Instruction.init(.integer, 0, 10, 0), // r0 = 10
            Instruction.init(.integer, 1, 20, 0), // r1 = 20
            Instruction.init(.add, 0, 1, 2), // r2 = r0 + r1
            Instruction.init(.result_row, 2, 1, 0), // output r2
            Instruction.init(.halt, 0, 0, 0),
        },
        .num_registers = 4,
        .num_cursors = 0,
        .column_names = &[_][]const u8{"result"},
    };

    var vm_inst = try VM.init(std.testing.allocator, program);
    defer vm_inst.deinit();

    try vm_inst.execute();

    try std.testing.expectEqual(@as(usize, 1), vm_inst.results.items.len);
    try std.testing.expectEqual(@as(i64, 30), vm_inst.results.items[0].values[0].integer);
}

test "VM comparison and conditional jump" {
    const program = Program{
        .instructions = &[_]Instruction{
            Instruction.init(.integer, 0, 5, 0), // r0 = 5
            Instruction.init(.integer, 1, 10, 0), // r1 = 10
            Instruction.init(.lt, 0, 1, 2), // r2 = r0 < r1
            Instruction.init(.jump_if, 2, 5, 0), // if r2 goto 5
            Instruction.init(.integer, 3, 0, 0), // r3 = 0 (not taken)
            Instruction.init(.integer, 3, 1, 0), // r3 = 1 (taken)
            Instruction.init(.result_row, 3, 1, 0),
            Instruction.init(.halt, 0, 0, 0),
        },
        .num_registers = 4,
        .num_cursors = 0,
        .column_names = &[_][]const u8{"result"},
    };

    var vm_inst = try VM.init(std.testing.allocator, program);
    defer vm_inst.deinit();
    try vm_inst.execute();

    try std.testing.expectEqual(@as(i64, 1), vm_inst.results.items[0].values[0].integer);
}

test "VM string register" {
    const program = Program{
        .instructions = &[_]Instruction{
            Instruction.withP4(.string, 0, 0, 0, .{ .str = "hello" }),
            Instruction.init(.result_row, 0, 1, 0),
            Instruction.init(.halt, 0, 0, 0),
        },
        .num_registers = 2,
        .num_cursors = 0,
        .column_names = &[_][]const u8{"msg"},
    };

    var vm_inst = try VM.init(std.testing.allocator, program);
    defer vm_inst.deinit();
    try vm_inst.execute();

    try std.testing.expectEqualStrings("hello", vm_inst.results.items[0].values[0].text);
}
