const std = @import("std");
const zqlite = @import("zqlite");
const c = std.c;

fn writeAll(buf: []const u8) void {
    var written: usize = 0;
    while (written < buf.len) {
        const rc = c.write(1, buf[written..].ptr, buf[written..].len);
        if (rc <= 0) return;
        written += @intCast(rc);
    }
}

fn print(comptime fmt: []const u8, args: anytype) void {
    var buf: [4096]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, fmt, args) catch return;
    writeAll(s);
}

pub fn main() void {
    print("ZQLite v0.1.0 — A high-performance SQLite clone in Zig\n", .{});
    print("Enter SQL statements. Type .help for usage, .quit to exit.\n\n", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Open the database file
    const db_path = "/tmp/zqlite.db";
    var fh = zqlite.os.FileHandle.open(db_path, zqlite.os.DEFAULT_PAGE_SIZE, false) catch {
        print("Error: could not open database at {s}\n", .{db_path});
        return;
    };
    defer fh.close();

    // Initialize buffer pool and schema
    var pool = zqlite.pager.BufferPool.init(allocator, &fh, 64) catch {
        print("Error: could not initialize buffer pool\n", .{});
        return;
    };
    defer pool.deinit();

    var schema_store = zqlite.schema.Schema.init(allocator);
    defer schema_store.deinit();

    // Persistent arena for executor-owned strings (table names, column names).
    // Freed on exit — avoids leak warnings from the GPA debug checker.
    var exec_arena = std.heap.ArenaAllocator.init(allocator);
    defer exec_arena.deinit();

    var exec = zqlite.executor.Executor.init(exec_arena.allocator(), &pool, &schema_store);

    var line_buf: [4096]u8 = undefined;

    while (true) {
        writeAll("zqlite> ");

        // Read a line from stdin
        var pos: usize = 0;
        while (pos < line_buf.len - 1) {
            const rc = c.read(0, line_buf[pos..][0..1].ptr, 1);
            if (rc <= 0) {
                if (pos == 0) {
                    print("\nBye!\n", .{});
                    return;
                }
                break;
            }
            if (line_buf[pos] == '\n') break;
            pos += 1;
        }
        const line = std.mem.trim(u8, line_buf[0..pos], " \t\r");
        if (line.len == 0) continue;

        // Meta-commands
        if (line[0] == '.') {
            if (std.mem.eql(u8, line, ".quit") or std.mem.eql(u8, line, ".exit")) {
                print("Bye!\n", .{});
                return;
            }
            if (std.mem.eql(u8, line, ".help")) {
                writeAll(
                    \\.help             Show this help
                    \\.tables           List all tables
                    \\.quit / .exit     Exit the REPL
                    \\
                    \\Supported SQL:
                    \\  CREATE TABLE name (col TYPE, ...);
                    \\  INSERT INTO name VALUES (...);
                    \\  SELECT col, ... FROM name [WHERE ...];
                    \\  SELECT * FROM name;
                    \\  DELETE FROM name [WHERE ...];
                    \\  DROP TABLE name;
                    \\  SELECT expr, expr, ...;  (constant expressions)
                    \\
                );
                continue;
            }
            if (std.mem.eql(u8, line, ".tables")) {
                var it = schema_store.tables.iterator();
                var count: usize = 0;
                while (it.next()) |entry| {
                    print("{s}\n", .{entry.key_ptr.*});
                    count += 1;
                }
                if (count == 0) {
                    writeAll("(no tables)\n");
                }
                continue;
            }
            print("Unknown command: {s} (try .help)\n", .{line});
            continue;
        }

        // Parse
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        var parser = zqlite.parser.Parser.init(line, arena.allocator());
        const stmt = parser.parseStatement() catch {
            print("Error: could not parse SQL.\n", .{});
            continue;
        };

        // Route: SELECT without FROM → codegen/VM, everything else → executor
        const use_vm = switch (stmt) {
            .select => |sel| sel.from == null,
            else => false,
        };

        if (use_vm) {
            // Constant expression SELECT — use codegen + VM
            var compiler = zqlite.codegen.Compiler.init(allocator);
            defer compiler.deinit();

            const program = compiler.compile(stmt) catch {
                print("Error: compilation failed.\n", .{});
                continue;
            };

            var vm = zqlite.vm.VM.init(allocator, program) catch {
                print("Error: VM initialization failed.\n", .{});
                continue;
            };
            defer vm.deinit();

            vm.execute() catch {
                print("Error: execution failed.\n", .{});
                continue;
            };

            // Print column headers
            if (program.column_names.len > 0) {
                for (program.column_names, 0..) |name, i| {
                    if (i > 0) writeAll(" | ");
                    print("{s}", .{name});
                }
                writeAll("\n");
                for (program.column_names, 0..) |name, i| {
                    if (i > 0) writeAll("-+-");
                    for (0..@max(name.len, 4)) |_| writeAll("-");
                }
                writeAll("\n");
            }

            for (vm.results.items) |row| {
                for (row.values, 0..) |val, i| {
                    if (i > 0) writeAll(" | ");
                    switch (val) {
                        .integer => |v| print("{d}", .{v}),
                        .real => |v| print("{d}", .{v}),
                        .text => |v| print("{s}", .{v}),
                        .null_val => writeAll("NULL"),
                        .blob => writeAll("[blob]"),
                        .boolean => |v| print("{s}", .{if (v) "true" else "false"}),
                    }
                }
                writeAll("\n");
            }
            if (vm.results.items.len == 0) writeAll("OK\n");
        } else {
            // Table-touching statement → executor
            const result = exec.execute(stmt, arena.allocator()) catch |err| {
                switch (err) {
                    zqlite.executor.ExecError.TableNotFound => print("Error: table not found.\n", .{}),
                    zqlite.executor.ExecError.TableAlreadyExists => print("Error: table already exists.\n", .{}),
                    zqlite.executor.ExecError.ColumnCountMismatch => print("Error: column count mismatch.\n", .{}),
                    zqlite.executor.ExecError.UnsupportedStatement => print("Error: statement type not yet supported.\n", .{}),
                    zqlite.executor.ExecError.TypeError => print("Error: type error in expression.\n", .{}),
                    zqlite.executor.ExecError.ColumnNotFound => print("Error: column not found.\n", .{}),
                    else => print("Error: execution failed.\n", .{}),
                }
                continue;
            };

            if (result.rows.len > 0) {
                // Print column headers
                if (result.column_names.len > 0) {
                    for (result.column_names, 0..) |name, i| {
                        if (i > 0) writeAll(" | ");
                        // Pad name to at least 4 chars
                        print("{s}", .{name});
                    }
                    writeAll("\n");
                    for (result.column_names, 0..) |name, i| {
                        if (i > 0) writeAll("-+-");
                        for (0..@max(name.len, 4)) |_| writeAll("-");
                    }
                    writeAll("\n");
                }

                // Print rows
                for (result.rows) |row| {
                    for (row.values, 0..) |val, i| {
                        if (i > 0) writeAll(" | ");
                        switch (val) {
                            .integer => |v| print("{d}", .{v}),
                            .real => |v| print("{d}", .{v}),
                            .text => |v| print("{s}", .{v}),
                            .null_val => writeAll("NULL"),
                        }
                    }
                    writeAll("\n");
                }
            } else if (result.rows_affected > 0) {
                print("{d} row(s) affected.\n", .{result.rows_affected});
            } else {
                if (result.message) |msg| {
                    print("{s}\n", .{msg});
                } else {
                    writeAll("OK\n");
                }
            }
        }
    }
}
