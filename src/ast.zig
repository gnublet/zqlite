const std = @import("std");
const record = @import("record.zig");

/// Abstract Syntax Tree node types for ZQLite's SQL parser.
///
/// Arena-allocated — after query execution, the entire arena is freed
/// at once (zero individual frees).

// ═══════════════════════════════════════════════════════════════════════════
// Expressions
// ═══════════════════════════════════════════════════════════════════════════

pub const Expr = union(enum) {
    integer_literal: i64,
    real_literal: f64,
    string_literal: []const u8,
    blob_literal: []const u8,
    null_literal: void,
    column_ref: ColumnRef,
    binary_op: BinaryOp,
    unary_op: UnaryOp,
    function_call: FunctionCall,
    between: Between,
    in_list: InList,
    is_null: IsNull,
    like: Like,
    star: void, // SELECT *
    paren: *Expr, // parenthesised expression
    placeholder: u32, // parameter placeholder (?), 1-based index

    pub const ColumnRef = struct {
        table: ?[]const u8,
        column: []const u8,
    };

    pub const BinaryOp = struct {
        op: BinOp,
        left: *Expr,
        right: *Expr,
    };

    pub const UnaryOp = struct {
        op: UOp,
        operand: *Expr,
    };

    pub const FunctionCall = struct {
        name: []const u8,
        args: []const *Expr,
        distinct: bool,
    };

    pub const Between = struct {
        operand: *Expr,
        low: *Expr,
        high: *Expr,
        negated: bool,
    };

    pub const InList = struct {
        operand: *Expr,
        list: []const *Expr,
        negated: bool,
    };

    pub const IsNull = struct {
        operand: *Expr,
        negated: bool, // IS NOT NULL
    };

    pub const Like = struct {
        operand: *Expr,
        pattern: *Expr,
        negated: bool,
    };
};

pub const BinOp = enum {
    add,
    sub,
    mul,
    div,
    mod,
    eq,
    ne,
    lt,
    le,
    gt,
    ge,
    @"and",
    @"or",
    concat, // ||
};

pub const UOp = enum {
    negate,
    not,
};

// ═══════════════════════════════════════════════════════════════════════════
// ORDER BY direction
// ═══════════════════════════════════════════════════════════════════════════

pub const SortOrder = enum {
    asc,
    desc,
};

pub const OrderByClause = struct {
    expr: *Expr,
    order: SortOrder,
};

// ═══════════════════════════════════════════════════════════════════════════
// JOIN
// ═══════════════════════════════════════════════════════════════════════════

pub const JoinType = enum {
    inner,
    left,
    cross,
};

pub const JoinClause = struct {
    join_type: JoinType,
    table: TableRef,
    on: ?*Expr,
};

pub const TableRef = struct {
    name: []const u8,
    alias: ?[]const u8,
};

// ═══════════════════════════════════════════════════════════════════════════
// Statements
// ═══════════════════════════════════════════════════════════════════════════

pub const Statement = union(enum) {
    select: Select,
    insert: Insert,
    update: Update,
    delete: Delete,
    create_table: CreateTable,
    drop_table: DropTable,
    create_index: CreateIndex,
    drop_index: DropIndex,
    begin: TransactionMode,
    commit: void,
    rollback: void,
    explain: *Statement,

    pub const Select = struct {
        columns: []const SelectColumn,
        from: ?TableRef,
        joins: []const JoinClause,
        where: ?*Expr,
        group_by: []const *Expr,
        having: ?*Expr,
        order_by: []const OrderByClause,
        limit: ?*Expr,
    };

    pub const SelectColumn = union(enum) {
        expr: ExprColumn,
        all_columns: void, // *
        table_all: []const u8, // table.*

        pub const ExprColumn = struct {
            expr: *Expr,
            alias: ?[]const u8,
        };
    };

    pub const Insert = struct {
        table: []const u8,
        columns: ?[]const []const u8,
        values: []const []const *Expr, // rows of values
    };

    pub const Update = struct {
        table: []const u8,
        assignments: []const Assignment,
        where: ?*Expr,
    };

    pub const Assignment = struct {
        column: []const u8,
        value: *Expr,
    };

    pub const Delete = struct {
        table: []const u8,
        where: ?*Expr,
    };

    pub const CreateTable = struct {
        name: []const u8,
        columns: []const ColumnDef,
        if_not_exists: bool,
    };

    pub const ColumnDef = struct {
        name: []const u8,
        type_name: ?[]const u8,
        not_null: bool,
        primary_key: bool,
        default_value: ?*Expr,
    };

    pub const DropTable = struct {
        name: []const u8,
        if_exists: bool,
    };

    pub const CreateIndex = struct {
        name: []const u8,
        table: []const u8,
        columns: []const []const u8,
        unique: bool,
        if_not_exists: bool,
    };

    pub const DropIndex = struct {
        name: []const u8,
        if_exists: bool,
    };

    pub const TransactionMode = enum {
        deferred,
        immediate,
        exclusive,
    };
};

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

test "Expr creation" {
    const expr = Expr{ .integer_literal = 42 };
    try std.testing.expectEqual(@as(i64, 42), expr.integer_literal);
}

test "Statement creation" {
    const stmt = Statement{ .commit = {} };
    try std.testing.expect(stmt == .commit);
}
