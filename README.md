# ⚡ ZQLite

A high-performance SQLite clone written in [Zig](https://ziglang.org/), targeting Linux with `io_uring` support.

## Features

- **B-tree storage engine** with leaf/interior node splitting and in-order traversal
- **Buffer pool / pager** with clock-sweep eviction and dirty-page write-back
- **Write-ahead log (WAL)** with frame-level checksums and checkpoint support
- **SQL front-end** — tokenizer → parser → AST → bytecode compiler → register-based VM
- **Query planner** with cost-based scan selection (full scan, index scan, rowid lookup)
- **Record format** compatible with SQLite's varint-encoded serial types
- **Direct I/O** with page-aligned buffers and optional `O_DIRECT`
- **POSIX I/O** via libc (`pread`/`pwrite`/`ftruncate`) — cross-platform
- **io_uring** integration for async I/O on Linux (optional optimization)

## Requirements

- **Zig** `0.16.0-dev.1859` or later
- **POSIX system** (Linux, macOS, etc. — uses libc for I/O)
- **libsqlite3** (only for benchmarks — `apt install libsqlite3-dev`)

## Quick Start

```bash
# Build
zig build

# Run the CLI
zig build run

# Run all tests (77 tests)
zig build test

# Run benchmarks (compare ZQLite vs C SQLite)
zig build bench

# Optimized benchmark run
zig build bench -Doptimize=ReleaseFast
```

## Usage

### Interactive SQL Shell

Launch the REPL with `zig build run`:

```
$ zig build run
ZQLite v0.1.0 — A high-performance SQLite clone in Zig
Enter SQL statements. Type .help for usage, .quit to exit.

zqlite> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
OK

zqlite> INSERT INTO users VALUES (1, 'Alice', 30);
1 row(s) affected.

zqlite> INSERT INTO users VALUES (2, 'Bob', 25);
1 row(s) affected.

zqlite> INSERT INTO users VALUES (3, 'Charlie', 35);
1 row(s) affected.

zqlite> SELECT * FROM users;
id | name | age
-----+------+-----
1 | Alice | 30
2 | Bob | 25
3 | Charlie | 35

zqlite> SELECT name, age FROM users WHERE age > 28;
name | age
-----+-----
Alice | 30
Charlie | 35

zqlite> UPDATE users SET name = 'Bobby' WHERE id = 2;
1 row(s) affected.

zqlite> SELECT * FROM users;
id | name | age
-----+------+-----
1 | Alice | 30
2 | Bobby | 25
3 | Charlie | 35

zqlite> DELETE FROM users WHERE id = 2;
1 row(s) affected.

zqlite> SELECT * FROM users;
id | name | age
-----+------+-----
1 | Alice | 30
3 | Charlie | 35

zqlite> DROP TABLE users;
OK

zqlite> SELECT 1 + 2 * 3;
?column?
--------
7

zqlite> .quit
Bye!
```

**Meta-commands:** `.help` for syntax, `.tables` to list tables, `.quit` to exit.

### Storage API (Zig)

The B-tree, pager, and record layers are fully functional for direct use:

```zig
const zqlite = @import("zqlite");
const std = @import("std");

// Open a database file
var fh = try zqlite.os.FileHandle.open("mydb.db", 4096, false);
defer fh.close();

// Create a buffer pool (8 pages)
var pool = try zqlite.pager.BufferPool.init(allocator, &fh, 8);
defer pool.deinit();

// Create a B-tree table
var bt = try zqlite.btree.Btree.create(&pool, zqlite.btree.PAGE_TYPE_TABLE_LEAF);

// Serialize a record: (1, "Alice", 30)
const values = [_]zqlite.record.Value{
    .{ .integer = 1 },
    .{ .text = "Alice" },
    .{ .integer = 30 },
};
var buf: [256]u8 = undefined;
const rec_size = try zqlite.record.serializeRecord(&values, &buf);

// Insert with rowid = 1
try bt.insert(1, buf[0..rec_size]);

// Point lookup by rowid
const result = try bt.search(1);
const decoded = try zqlite.record.deserializeRecord(result.?.payload, allocator);
// decoded[0] → integer(1), decoded[1] → text("Alice"), decoded[2] → integer(30)

// Iterate all rows in order with a cursor
var cursor = try zqlite.cursor.Cursor.init(&pool, bt.root_page);
try cursor.seekFirst();
while (cursor.isValid()) {
    const entry = try cursor.currentEntry();
    // process entry.key and entry.payload...
    try cursor.next();
}

// Delete by rowid
try bt.delete(1);
```



## Project Structure

```
zqlite/
├── build.zig            # Build configuration
├── build.zig.zon        # Package manifest
├── src/
│   ├── root.zig         # Public API — re-exports all modules
│   ├── main.zig         # CLI entry point
│   ├── os.zig           # OS abstraction (file I/O, io_uring, syscalls)
│   ├── pager.zig        # Buffer pool with clock-sweep eviction
│   ├── wal.zig          # Write-ahead log
│   ├── btree.zig        # B-tree storage engine
│   ├── cursor.zig       # B-tree cursor for ordered iteration
│   ├── record.zig       # Record serialization (varint, serial types)
│   ├── schema.zig       # Schema catalog (tables, indexes)
│   ├── tokenizer.zig    # SQL tokenizer
│   ├── parser.zig       # Recursive-descent SQL parser
│   ├── ast.zig          # Abstract syntax tree types
│   ├── codegen.zig      # AST → bytecode compiler
│   ├── planner.zig      # Cost-based query planner
│   └── vm.zig           # Register-based virtual machine
├── bench/
│   └── bench_main.zig   # Benchmark harness (ZQLite vs SQLite)
└── tests/
    ├── test_btree.zig
    ├── test_pager.zig
    ├── test_record.zig
    ├── test_tokenizer.zig
    ├── test_parser.zig
    ├── test_vm.zig
    └── test_integration.zig
```

## Architecture

```
SQL String
    │
    ▼
┌──────────┐    ┌────────┐    ┌─────────┐    ┌────┐
│ Tokenizer│───▶│ Parser │───▶│ Codegen │───▶│ VM │
└──────────┘    └────────┘    └─────────┘    └──┬─┘
                                   │            │
                              ┌────▼────┐       │
                              │ Planner │       │
                              └─────────┘       │
                                                ▼
                              ┌─────────────────────┐
                              │   B-tree + Cursor    │
                              └──────────┬──────────┘
                                         │
                              ┌──────────▼──────────┐
                              │  Pager (Buffer Pool) │
                              └──────────┬──────────┘
                                         │
                              ┌──────────▼──────────┐
                              │   WAL + OS Layer     │
                              │  (io_uring / pread)  │
                              └─────────────────────┘
```

## Testing

Tests are split between **inline tests** (in each `src/*.zig` module) and **standalone tests** (in `tests/`):

```bash
# Run everything
zig build test

# Run a single test file
zig test --dep zqlite -Mroot=tests/test_btree.zig -Mzqlite=src/root.zig
```

## Benchmarks

All benchmarks use the **full SQL pipeline** for both engines:
- **ZQLite:** SQL string → parser → executor → B-tree
- **SQLite:** SQL string → prepared statement → VDBE → B-tree

| Benchmark | Description |
|---|---|
| `bulk_insert` | INSERT 500 rows via SQL |
| `point_lookup` | 5,000 SELECT WHERE id = N lookups |
| `scan_filter` | 1,000 full-table scans with WHERE filter |
| `point_delete` | 500 DELETE WHERE id = N operations |
| `mixed_workload` | 500 INSERT → SELECT → DELETE cycles |

```bash
# Quick run
zig build bench

# Optimized benchmark run
zig build bench -Doptimize=ReleaseFast
```

Sample results (ReleaseFast, Linux x86_64):

| Benchmark | ZQLite (ms) | SQLite (ms) | Ratio |
|---|---|---|---|
| `bulk_insert` | 0.06 | 1.04 | 16.94x |
| `point_lookup` | 1.19 | 9.81 | 8.28x |
| `scan_filter` | 7.38 | 9.58 | 1.30x |
| `point_delete` | 0.31 | 415.50 | 1360x |
| `mixed_workload` | 1.48 | 882.85 | 595x |

*Ratio > 1 = ZQLite faster, < 1 = SQLite faster*

**Notes:**
- Both engines use the full SQL pipeline (parse → plan → execute → storage)
- ZQLite advantages: zero-copy allocator, PK fast-path (bt.search/bt.delete)
- SQLite advantages: prepared statements, decades of optimization, MVCC
- Neither engine uses transactions — SQLite auto-commits each write with fsync, explaining the large `point_delete` and `mixed_workload` ratios
- ZQLite does not yet implement: WAL, crash recovery, page splitting, multi-table joins, indexes, or prepared statements

> **Note:** Benchmarks require `libsqlite3-dev` installed on your system.

## License

MIT
