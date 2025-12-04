const std = @import("std");

/// Simple error object used by the CUDA wrapper.
pub const KernelError = struct {
    code: i32,
    message: []const u8,
};

/// Fake device info – enough for logging.
pub const DeviceInfo = struct {
    name: [64]u8,
    compute_capability_major: i32,
    compute_capability_minor: i32,
    total_global_mem: u64,
};

/// Opaque GPU batch types – we don't use real CUDA on this pod.
pub const GPUOHLCDataBatch = opaque {};
pub const GPUOHLCDataBatch_C = opaque {};

/// Namespace for "CUDA" operations. In this stub implementation we
/// always behave as if there are **0 CUDA devices**, so the rest of the
/// code will switch to the CPU fallback path (which you already saw in
/// the logs: "No CUDA devices detected; using CPU fallback").
pub const CudaWrapper = struct {
    /// Called by selectBestCUDADevice() in lib.zig
    pub fn getDeviceCount(out_count: *i32) KernelError {
        // No CUDA devices -> CPU fallback
        out_count.* = 0;
        return KernelError{ .code = 0, .message = "OK" };
    }

    /// Also used by lib.zig for logging device info.
    /// We just fill in a fake "CPU Fallback Device".
    pub fn getDeviceInfo(device_index: i32, info: *DeviceInfo) KernelError {
        _ = device_index;

        info.* = DeviceInfo{
            .name = undefined,
            .compute_capability_major = 0,
            .compute_capability_minor = 0,
            .total_global_mem = 0,
        };

        const label = "CPU Fallback Device";
        var i: usize = 0;
        while (i < label.len and i < info.name.len) : (i += 1) {
            info.name[i] = label[i];
        }
        if (i < info.name.len) info.name[i] = 0; // C-style terminator

        return KernelError{ .code = 0, .message = "OK" };
    }

    /// These are placeholders in case lib.zig or other files declare them.
    /// They do nothing but return success so that everything compiles.
    pub fn initOHLCBatch(batch: *GPUOHLCDataBatch, max_symbols: usize) KernelError {
        _ = batch;
        _ = max_symbols;
        return KernelError{ .code = 0, .message = "OK" };
    }

    pub fn freeOHLCBatch(batch: *GPUOHLCDataBatch) void {
        _ = batch;
    }
};
