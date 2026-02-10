const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const websocket_dep = b.dependency("websocket", .{});

    const exe = b.addExecutable(.{
        .name = "MicroRush",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    exe.root_module.addImport("websocket", websocket_dep.module("websocket"));
    exe.linkLibC();

    exe.addCSourceFile(.{ .file = b.path("src/signal_engine/simd.c"), .flags = &.{ "-mavx2" } });

    const nvcc = b.findProgram(&.{"nvcc"}, &.{}) catch null;
    if (nvcc) |nvcc_path| {
        const nvcc_cmd = b.addSystemCommand(&.{
            nvcc_path,
            "-c",
            "-O2",
            "-arch=sm_75",
            "-I",
            "src/stat_calc",
            "src/stat_calc/kernel.cu",
            "-o",
        });
        const cuda_obj = nvcc_cmd.addOutputFileArg("kernel_cuda.o");
        exe.step.dependOn(&nvcc_cmd.step);
        exe.addObjectFile(cuda_obj);
        exe.linkSystemLibrary("cudart");
        exe.addCSourceFile(.{ .file = b.path("src/stat_calc/cuda_stub.c"), .flags = &.{ "-DENABLE_CUDA_BACKEND=1" } });
    } else {
        exe.addCSourceFile(.{ .file = b.path("src/stat_calc/cuda_stub.c"), .flags = &.{} });
    }

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| run_cmd.addArgs(args);

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);
}
