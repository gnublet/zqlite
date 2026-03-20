const std = @import("std");
const record = @import("record.zig");
const btree = @import("btree.zig");
const cursor_mod = @import("cursor.zig");
const pager = @import("pager.zig");

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
    idx_next = 82,
    idx_rowid = 83,
    seek_rowid = 84,

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

/// Convert record.Value → Register
fn recordValueToRegister(rv: record.Value) Register {
    return switch (rv) {
        .null_val => .{ .null_val = {} },
        .integer => |v| .{ .integer = v },
        .real => |v| .{ .real = v },
        .text => |v| .{ .text = v },
        .blob => |v| .{ .blob = v },
    };
}

// ═══════════════════════════════════════════════════════════════════════════
// VM — the execution engine
// ═══════════════════════════════════════════════════════════════════════════

pub const StepResult = enum {
    row,
    done,
};

/// B-tree cursor state for open_read/open_write opcodes
const CursorState = struct {
    bt: btree.Btree,
    cur: cursor_mod.Cursor,
    valid: bool,
    is_write: bool,
    header_cache: record.HeaderCache = .{},
};

pub const VM = struct {
    registers: []Register,
    pc: u32,
    program: Program,
    allocator: std.mem.Allocator,
    halted: bool,
    pool: ?*pager.BufferPool,
    cursors: [8]?CursorState,
    bound_params: ?[]const record.Value,
    rows_affected: usize,

    const Self = @This();

    /// Create a new VM for the given program.
    pub fn init(allocator: std.mem.Allocator, program: Program) !Self {
        const regs = try allocator.alloc(Register, program.num_registers);
        @memset(regs, .{ .null_val = {} });

        return Self{
            .registers = regs,
            .pc = 0,
            .program = program,
            .allocator = allocator,
            .halted = false,
            .pool = null,
            .cursors = .{null} ** 8,
            .bound_params = null,
            .rows_affected = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.registers);
    }

    /// Execute the program until it yields a row or finishes.
    pub fn step(self: *Self) !StepResult {
        while (!self.halted and self.pc < self.program.instructions.len) {
            const instr = self.program.instructions[self.pc];
            self.pc += 1;

            switch (instr.opcode) {
                .halt => {
                    self.halted = true;
                    return .done;
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
                return .row;
            },
            .copy => {
                const src: usize = @intCast(instr.p1);
                const dst: usize = @intCast(instr.p2);
                self.registers[dst] = self.registers[src];
            },

            // ── B-tree cursor operations ──────────────────────────────

            .open_read => {
                // p1 = cursor_id, p2 = root_page
                const cid: usize = @intCast(instr.p1);
                if (self.pool) |pool| {
                    var bt = btree.Btree.open(pool, @intCast(instr.p2));
                    var cur = cursor_mod.Cursor.init(&bt);
                    cur.first() catch {};
                    self.cursors[cid] = .{
                        .bt = bt,
                        .cur = cur,
                        .valid = cur.valid,
                        .is_write = false,
                    };
                }
            },
            .open_write => {
                // p1 = cursor_id, p2 = root_page
                const cid: usize = @intCast(instr.p1);
                if (self.pool) |pool| {
                    const bt = btree.Btree.open(pool, @intCast(instr.p2));
                    self.cursors[cid] = .{
                        .bt = bt,
                        .cur = undefined,
                        .valid = true,
                        .is_write = true,
                        .header_cache = .{},
                    };
                }
            },
            .rewind => {
                // p1 = cursor_id, p2 = jump_target (if table is empty)
                const cid: usize = @intCast(instr.p1);
                if (self.cursors[cid]) |*cs| {
                    cs.header_cache.reset();
                    cs.cur = cursor_mod.Cursor.init(&cs.bt);
                    cs.cur.first() catch {
                        cs.valid = false;
                    };
                    cs.valid = cs.cur.valid;
                    if (!cs.valid) {
                        self.pc = @intCast(instr.p2); // jump to HALT
                    }
                } else {
                    self.pc = @intCast(instr.p2);
                }
            },
            .next_row => {
                // p1 = cursor_id, p2 = jump_target (loop back)
                const cid: usize = @intCast(instr.p1);
                if (self.cursors[cid]) |*cs| {
                    cs.header_cache.reset();
                    _ = cs.cur.next() catch {};
                    cs.valid = cs.cur.valid;
                    if (cs.valid) {
                        self.pc = @intCast(instr.p2); // loop back
                    }
                }
            },
            .column => {
                // p1 = cursor_id, p2 = col_idx, p3 = dest_reg
                const cid: usize = @intCast(instr.p1);
                const col_idx: usize = @intCast(instr.p2);
                const dst: usize = @intCast(instr.p3);
                if (self.cursors[cid]) |*cs| {
                    if (cs.valid) {
                        const entry = cs.cur.cell() catch {
                            self.registers[dst] = .{ .null_val = {} };
                            continue;
                        };
                        const rv = record.readColumnCached(entry.payload, col_idx, &cs.header_cache) catch record.Value{ .null_val = {} };
                        self.registers[dst] = recordValueToRegister(rv);
                    } else {
                        self.registers[dst] = .{ .null_val = {} };
                    }
                }
            },
            .rowid => {
                // p1 = cursor_id, p2 = dest_reg
                const cid: usize = @intCast(instr.p1);
                const dst: usize = @intCast(instr.p2);
                if (self.cursors[cid]) |*cs| {
                    if (cs.valid) {
                        const entry = cs.cur.cell() catch {
                            self.registers[dst] = .{ .null_val = {} };
                            continue;
                        };
                        self.registers[dst] = .{ .integer = entry.key };
                    }
                }
            },
            .close_cursor => {
                const cid: usize = @intCast(instr.p1);
                self.cursors[cid] = null;
            },
            .make_record => {
                // p1 = start_reg, p2 = num_fields, p3 = dest_reg
                // Serialize registers into a record buffer
                const start: usize = @intCast(instr.p1);
                const count: usize = @intCast(instr.p2);
                const dst: usize = @intCast(instr.p3);
                var rec_vals: [64]record.Value = undefined;
                for (0..count) |i| {
                    rec_vals[i] = self.registers[start + i].toRecordValue();
                }
                var buf: [4096]u8 = undefined;
                const size = record.serializeRecord(rec_vals[0..count], &buf) catch {
                    self.registers[dst] = .{ .null_val = {} };
                    continue;
                };
                // Store serialized bytes as blob
                const data = self.allocator.dupe(u8, buf[0..size]) catch {
                    self.registers[dst] = .{ .null_val = {} };
                    continue;
                };
                self.registers[dst] = .{ .blob = data };
            },
            .insert => {
                // p1 = cursor_id, p2 = record_reg (blob), p3 = rowid_reg
                const cid: usize = @intCast(instr.p1);
                const rec_reg: usize = @intCast(instr.p2);
                const rid_reg: usize = @intCast(instr.p3);
                if (self.cursors[cid]) |*cs| {
                    const rowid_val = self.registers[rid_reg];
                    const rid: i64 = switch (rowid_val) {
                        .integer => |v| v,
                        else => continue,
                    };
                    const payload = switch (self.registers[rec_reg]) {
                        .blob => |b| b,
                        else => continue,
                    };
                    cs.bt.insert(rid, payload) catch {};
                    self.rows_affected += 1;
                }
            },

            // Bind parameter: load bound param into register
            .blob => {
                // Used for bind params: p1 = dest_reg, p2 = param_index (1-based)
                const reg: usize = @intCast(instr.p1);
                const param_idx: usize = @intCast(instr.p2);
                if (self.bound_params) |params| {
                    if (param_idx > 0 and param_idx <= params.len) {
                        self.registers[reg] = recordValueToRegister(params[param_idx - 1]);
                    }
                }
            },

            .idx_search => {
                // p1 = cursor_id, p2 = key_reg, p3 = jump_not_found
                const cid: usize = @intCast(instr.p1);
                const key_reg: usize = @intCast(instr.p2);
                const jump_pc: u32 = @intCast(instr.p3);
                if (self.cursors[cid]) |*cs| {
                    cs.header_cache.reset();
                    const search_key_val = self.registers[key_reg].toRecordValue();
                    var buf: [4096]u8 = undefined;
                    const size = record.serializeRecord(&[_]record.Value{search_key_val}, &buf) catch {
                        self.pc = jump_pc;
                        continue;
                    };
                    const key_bytes = buf[0..size];

                    const found = cs.cur.seekIndex(key_bytes) catch false;
                    cs.valid = cs.cur.valid;
                    if (!found) {
                        self.pc = jump_pc;
                    }
                } else {
                    self.pc = jump_pc;
                }
            },
            .idx_next => {
                // p1 = cursor_id, p2 = key_reg, p3 = jump_loop
                const cid: usize = @intCast(instr.p1);
                const key_reg: usize = @intCast(instr.p2);
                const jump_pc: u32 = @intCast(instr.p3);
                if (self.cursors[cid]) |*cs| {
                    cs.header_cache.reset();
                    if (cs.cur.next() catch false) {
                        const search_key_val = self.registers[key_reg].toRecordValue();
                        var buf: [4096]u8 = undefined;
                        const size = record.serializeRecord(&[_]record.Value{search_key_val}, &buf) catch continue;
                        const key_bytes = buf[0..size];
                        
                        const matches = cs.cur.indexKeyEquals(key_bytes) catch false;
                        if (matches) {
                            self.pc = jump_pc;
                        } else {
                            cs.valid = false;
                        }
                    } else {
                        cs.valid = false;
                    }
                }
            },
            .idx_rowid => {
                // p1 = cursor_id, p2 = dest_reg
                const cid: usize = @intCast(instr.p1);
                const dst: usize = @intCast(instr.p2);
                if (self.cursors[cid]) |*cs| {
                    if (cs.valid) {
                        const rowid = cs.cur.indexRowid() catch {
                            self.registers[dst] = .{ .null_val = {} };
                            continue;
                        };
                        self.registers[dst] = .{ .integer = rowid };
                    }
                }
            },
            .seek_rowid => {
                // p1 = cursor_id, p2 = rowid_reg, p3 = jump_not_found
                const cid: usize = @intCast(instr.p1);
                const rid_reg: usize = @intCast(instr.p2);
                const jump_pc: u32 = @intCast(instr.p3);
                if (self.cursors[cid]) |*cs| {
                    cs.header_cache.reset();
                    const rowid = switch (self.registers[rid_reg]) {
                        .integer => |v| v,
                        else => {
                            self.pc = jump_pc;
                            continue;
                        },
                    };
                    const found = cs.cur.seek(rowid) catch false;
                    cs.valid = cs.cur.valid;
                    if (!found) {
                        self.pc = jump_pc;
                    }
                } else {
                    self.pc = jump_pc;
                }
            },

            // Stubs for unimplemented opcodes
            .delete, .idx_insert, .agg_step, .agg_final, .sorter_open, .sorter_insert, .sorter_sort, .sorter_data, .sorter_next, .transaction, .commit, .rollback, .move, .concat, .like, .compare, .prev_row => {},
        } // end switch
        } // end while
        
        self.halted = true;
        return .done;
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

    try std.testing.expectEqual(StepResult.row, try vm_inst.step());
    try std.testing.expectEqual(@as(i64, 30), vm_inst.registers[2].integer);
    try std.testing.expectEqual(StepResult.done, try vm_inst.step());
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
    try std.testing.expectEqual(StepResult.row, try vm_inst.step());
    try std.testing.expectEqual(@as(i64, 1), vm_inst.registers[3].integer);
    try std.testing.expectEqual(StepResult.done, try vm_inst.step());
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
    try std.testing.expectEqual(StepResult.row, try vm_inst.step());
    try std.testing.expectEqualStrings("hello", vm_inst.registers[0].text);
    try std.testing.expectEqual(StepResult.done, try vm_inst.step());
}
