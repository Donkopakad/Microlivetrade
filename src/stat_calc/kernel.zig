const std = @import("std");
const ERR = @import("../errors.zig");
const types = @import("../types.zig");
const DeviceInfo = types.DeviceInfo;
const GPUOHLCDataBatch = types.GPUOHLCDataBatch;
const GPUPercentageChangeDeviceBatch = types.GPUPercentageChangeDeviceBatch;

pub const KERNEL_SUCCESS = ERR.KernelError{ .code = 0, .message = "Success" };

pub const CudaWrapper = struct {
    fn success() ERR.KernelError {
        return KERNEL_SUCCESS;
    }

    pub fn initDevice(device_id: c_int) ERR.KernelError {
        _ = device_id;
        return success();
    }

    pub fn resetDevice() ERR.KernelError {
        return success();
    }

    pub fn getDeviceCount(count: *c_int) ERR.KernelError {
        count.* = 0;
        return success();
    }

    pub fn getDeviceInfo(device_id: c_int, info: *DeviceInfo) ERR.KernelError {
        _ = device_id;
        info.* = DeviceInfo{
            .name = [_]u8{0} ** 256,
            .major = 0,
            .minor = 0,
            .totalGlobalMem = 0,
        };
        const label = "CPU Fallback Device";
        const len = @min(label.len, info.name.len - 1);
        @memcpy(info.name[0..len], label[0..len]);
        return success();
    }

    pub fn selectBestDevice(best_device_id: *c_int) ERR.KernelError {
        best_device_id.* = 0;
        return success();
    }

    pub fn allocateMemory(d_ohlc_batch: **GPUOHLCDataBatch, d_pct_result: **GPUPercentageChangeDeviceBatch) ERR.KernelError {
        d_ohlc_batch.*.* = std.mem.zeroes(GPUOHLCDataBatch);
        d_pct_result.*.* = std.mem.zeroes(GPUPercentageChangeDeviceBatch);
        return success();
    }

    pub fn freeMemory(d_ohlc_batch: ?*GPUOHLCDataBatch, d_pct_result: ?*GPUPercentageChangeDeviceBatch) ERR.KernelError {
        _ = d_ohlc_batch;
        _ = d_pct_result;
        return success();
    }

    pub fn runPercentageChangeBatch(
        d_ohlc_batch_ptr: *GPUOHLCDataBatch,
        d_pct_results_ptr: *GPUPercentageChangeDeviceBatch,
        h_ohlc_batch: *const GPUOHLCDataBatch,
        h_pct_results: *GPUPercentageChangeDeviceBatch,
        num_symbols: c_int,
    ) ERR.KernelError {
        _ = d_ohlc_batch_ptr;
        _ = d_pct_results_ptr;
        _ = h_ohlc_batch;
        _ = h_pct_results;
        _ = num_symbols;
        return success();
    }
};
