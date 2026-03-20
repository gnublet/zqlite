const std = @import("std");
const zqlite = @import("zqlite");

test "VM: arithmetic operations" {
    const program = zqlite.vm.Program{
        .instructions = &[_]zqlite.vm.Instruction{
            zqlite.vm.Instruction.init(.integer, 0, 100, 0),
            zqlite.vm.Instruction.init(.integer, 1, 42, 0),
            zqlite.vm.Instruction.init(.add, 0, 1, 2), // r2 = 142
            zqlite.vm.Instruction.init(.sub, 0, 1, 3), // r3 = 58
            zqlite.vm.Instruction.init(.mul, 0, 1, 4), // r4 = 4200
            zqlite.vm.Instruction.init(.result_row, 2, 3, 0),
            zqlite.vm.Instruction.init(.halt, 0, 0, 0),
        },
        .num_registers = 8,
        .num_cursors = 0,
        .column_names = &[_][]const u8{ "add", "sub", "mul" },
    };

    var vm_inst = try zqlite.vm.VM.init(std.testing.allocator, program);
    defer vm_inst.deinit();
    try std.testing.expectEqual(zqlite.vm.StepResult.row, try vm_inst.step());
    try std.testing.expectEqual(@as(i64, 142), vm_inst.registers[2].integer);
    try std.testing.expectEqual(@as(i64, 58), vm_inst.registers[3].integer);
    try std.testing.expectEqual(@as(i64, 4200), vm_inst.registers[4].integer);
    try std.testing.expectEqual(zqlite.vm.StepResult.done, try vm_inst.step());
}

test "VM: conditional logic" {
    // if (5 == 5) result = 1 else result = 0
    const program = zqlite.vm.Program{
        .instructions = &[_]zqlite.vm.Instruction{
            zqlite.vm.Instruction.init(.integer, 0, 5, 0), // r0 = 5
            zqlite.vm.Instruction.init(.integer, 1, 5, 0), // r1 = 5
            zqlite.vm.Instruction.init(.eq, 0, 1, 2), // r2 = (r0 == r1)
            zqlite.vm.Instruction.init(.jump_if, 2, 6, 0), // if r2 goto 6
            zqlite.vm.Instruction.init(.integer, 3, 0, 0), // r3 = 0
            zqlite.vm.Instruction.init(.jump, 7, 0, 0), // goto 7
            zqlite.vm.Instruction.init(.integer, 3, 1, 0), // r3 = 1
            zqlite.vm.Instruction.init(.result_row, 3, 1, 0),
            zqlite.vm.Instruction.init(.halt, 0, 0, 0),
        },
        .num_registers = 4,
        .num_cursors = 0,
        .column_names = &[_][]const u8{"result"},
    };

    var vm_inst = try zqlite.vm.VM.init(std.testing.allocator, program);
    defer vm_inst.deinit();
    try std.testing.expectEqual(zqlite.vm.StepResult.row, try vm_inst.step());
    try std.testing.expectEqual(@as(i64, 1), vm_inst.registers[3].integer);
    try std.testing.expectEqual(zqlite.vm.StepResult.done, try vm_inst.step());
}

test "VM: loop producing multiple result rows" {
    // Loop 3 times, outputting counter each iteration
    const program = zqlite.vm.Program{
        .instructions = &[_]zqlite.vm.Instruction{
            zqlite.vm.Instruction.init(.integer, 0, 0, 0), // r0 = counter = 0
            zqlite.vm.Instruction.init(.integer, 1, 3, 0), // r1 = limit = 3
            zqlite.vm.Instruction.init(.integer, 2, 1, 0), // r2 = increment = 1
            // Loop start (pc = 3)
            zqlite.vm.Instruction.init(.ge, 0, 1, 3), // r3 = (r0 >= r1)
            zqlite.vm.Instruction.init(.jump_if, 3, 8, 0), // if r3 goto 8 (end)
            zqlite.vm.Instruction.init(.result_row, 0, 1, 0), // output r0
            zqlite.vm.Instruction.init(.add, 0, 2, 0), // r0 = r0 + 1
            zqlite.vm.Instruction.init(.jump, 3, 0, 0), // goto 3 (loop)
            zqlite.vm.Instruction.init(.halt, 0, 0, 0),
        },
        .num_registers = 4,
        .num_cursors = 0,
        .column_names = &[_][]const u8{"counter"},
    };

    var vm_inst = try zqlite.vm.VM.init(std.testing.allocator, program);
    defer vm_inst.deinit();
    var rows: i64 = 0;
    while (try vm_inst.step() == .row) {
        try std.testing.expectEqual(rows, vm_inst.registers[0].integer);
        rows += 1;
    }
    try std.testing.expectEqual(@as(i64, 3), rows);
}

test "VM: null handling" {
    const program = zqlite.vm.Program{
        .instructions = &[_]zqlite.vm.Instruction{
            zqlite.vm.Instruction.init(.null_val, 0, 0, 0), // r0 = null
            zqlite.vm.Instruction.init(.integer, 1, 42, 0), // r1 = 42
            zqlite.vm.Instruction.init(.add, 0, 1, 2), // r2 = null + 42 = null
            zqlite.vm.Instruction.init(.result_row, 2, 1, 0),
            zqlite.vm.Instruction.init(.halt, 0, 0, 0),
        },
        .num_registers = 4,
        .num_cursors = 0,
        .column_names = &[_][]const u8{"result"},
    };

    var vm_inst = try zqlite.vm.VM.init(std.testing.allocator, program);
    defer vm_inst.deinit();
    try std.testing.expectEqual(zqlite.vm.StepResult.row, try vm_inst.step());
    try std.testing.expect(vm_inst.registers[2] == .null_val);
    try std.testing.expectEqual(zqlite.vm.StepResult.done, try vm_inst.step());
}
