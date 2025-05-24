const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    const shimmer_mod = b.addModule("shimmer", .{
        .root_source_file = b.path("src/shimmer/shimmer.zig"),
    });

    const processing = b.addModule("processing", .{
        .root_source_file = b.path("src/processing/main.zig"),
    });

    const exe = b.addExecutable(.{
        .name = "viktor",
        .root_module = exe_mod,
    });

    exe.root_module.addImport("shimmer", shimmer_mod);
    exe.root_module.addImport("processing", processing);

    b.installArtifact(exe);
    const run_cmd = b.addRunArtifact(exe);

    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);
}
