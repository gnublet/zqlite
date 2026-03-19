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

The benchmark suite compares ZQLite against C SQLite on:

| Benchmark | Description |
|---|---|
| `bulk_insert` | Insert 1,000 rows into a table |
| `point_lookup` | 10,000 point lookups by key |
| `record_serialize` | 100,000 record serialization ops |
| `tokenizer` | 100,000 SQL tokenization passes |

```bash
# Quick run
zig build bench

# Production benchmark (optimized)
zig build bench -Doptimize=ReleaseFast
```

> **Note:** Benchmarks require `libsqlite3-dev` installed on your system.

## License

MIT
