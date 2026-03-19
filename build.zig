const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── ZQLite library module ──
    const zqlite_mod = b.addModule("zqlite", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });

    // ── CLI REPL executable ──
    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe_mod.addImport("zqlite", zqlite_mod);
    const exe = b.addExecutable(.{
        .name = "zqlite",
        .root_module = exe_mod,
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run", "Run the ZQLite REPL");
    run_step.dependOn(&run_cmd.step);

    // ── Unit tests ──
    const test_files = [_][]const u8{
        "tests/test_pager.zig",
        "tests/test_btree.zig",
        "tests/test_record.zig",
        "tests/test_tokenizer.zig",
        "tests/test_parser.zig",
        "tests/test_vm.zig",
        "tests/test_integration.zig",
        "tests/test_executor.zig",
    };

    const test_step = b.step("test", "Run all ZQLite tests");

    for (test_files) |test_file| {
        const t_mod = b.createModule(.{
            .root_source_file = b.path(test_file),
            .target = target,
            .optimize = optimize,
        });
        t_mod.addImport("zqlite", zqlite_mod);
        const t = b.addTest(.{
            .root_module = t_mod,
        });
        const run_t = b.addRunArtifact(t);
        test_step.dependOn(&run_t.step);
    }

    // Also run library-internal tests
    const lib_test_mod = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    const lib_test = b.addTest(.{
        .root_module = lib_test_mod,
    });
    const run_lib_test = b.addRunArtifact(lib_test);
    test_step.dependOn(&run_lib_test.step);

    // ── Benchmarks ──
    const bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/bench_main.zig"),
        .target = target,
        .optimize = optimize,
    });
    bench_mod.addImport("zqlite", zqlite_mod);
    bench_mod.linkSystemLibrary("sqlite3", .{});
    bench_mod.link_libc = true;

    const bench_exe = b.addExecutable(.{
        .name = "zqlite-bench",
        .root_module = bench_mod,
    });

    b.installArtifact(bench_exe);

    const bench_cmd = b.addRunArtifact(bench_exe);
    bench_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        bench_cmd.addArgs(args);
    }
    const bench_step = b.step("bench", "Run benchmarks (use -Doptimize=ReleaseFast)");
    bench_step.dependOn(&bench_cmd.step);
}
