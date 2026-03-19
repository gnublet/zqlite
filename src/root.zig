/// ZQLite — A high-performance SQLite clone written in Zig
///
/// Root module exporting all public APIs.

pub const os = @import("os.zig");
pub const pager = @import("pager.zig");
pub const wal = @import("wal.zig");
pub const btree = @import("btree.zig");
pub const cursor = @import("cursor.zig");
pub const record = @import("record.zig");
pub const schema = @import("schema.zig");
pub const vm = @import("vm.zig");
pub const tokenizer = @import("tokenizer.zig");
pub const parser = @import("parser.zig");
pub const ast = @import("ast.zig");
pub const planner = @import("planner.zig");
pub const codegen = @import("codegen.zig");

test {
    // Pull in declarations from all sub-modules so `zig build test`
    // on root.zig will also run their embedded tests.
    @import("std").testing.refAllDeclsRecursive(@This());
}
