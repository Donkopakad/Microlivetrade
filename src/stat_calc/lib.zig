const std = @import("std");
const cuda_lib = @import("kernel.h");
const types = @import("../types.zig");
const errors = @import("../errors.zig");
const SymbolMap = @import("../symbol-map.zig").SymbolMap;
const Symbol = types.Symbol;
const OHLC = types.OHLC;

pub const MAX_SYMBOLS: usize = 256;

const KernelError = cuda_lib.KernelError;
const CudaWrapper = cuda_lib.CudaWrapper;
const DeviceInfo = cuda_lib.DeviceInfo;
const GPUOHLCDataBatch = cuda_lib.GPUOHLCDataBatch;
const GPUOHLCDataBatch_C = cuda_lib.GPUOHLCDataBatch_C;
const GPUPercentageChangeResultBatch = types.GPUPercentageChangeResultBatch;
const GPUPercentageChangeDeviceBatch = types.GPUPercentageChangeDeviceBatch;
const GPUBatchResult = types.GPUBatchResult;
const StatCalcError = errors.StatCalcError;

pub fn selectBestCUDADevice() !i32 {
    var device_count: i32 = 0;
    const kerr = CudaWrapper.getDeviceCount(&device_count);
    if (kerr.code != 0) {
        std.log.warn("Failed to get CUDA device count: {} ({s}); defaulting to device 0", .{ kerr.code, kerr.message });
        return 0;
    }

    if (device_count == 0) {
        std.log.warn("No CUDA devices found; defaulting to device 0", .{});
        return 0;
    }

    var best_device: i32 = 0;
    var max_compute_capability: i32 = -1;
    var i: i32 = 0;
    while (i < device_count) : (i += 1) {
        var props: cuda_lib.CUDADeviceProperties = undefined;
        const err = CudaWrapper.getDeviceProperties(i, &props);
        if (err.code != 0) {
            std.log.warn("Failed to get properties for device {}: {} ({s})", .{ i, err.code, err.message });
            continue;
        }

        const compute_capability = props.major * 10 + props.minor;
        if (compute_capability > max_compute_capability) {
            max_compute_capability = compute_capability;
            best_device = i;
        }
    }

    std.log.info("Selected CUDA device {} with compute capability {}", .{ best_device, max_compute_capability });
    return best_device;
}

pub const StatCalc = struct {
    allocator: std.mem.Allocator,
    device_id: i32,
    gpu_enabled: bool,

    d_ohlc_batch: ?*GPUOHLCDataBatch,
    d_pct_result: ?*GPUPercentageChangeDeviceBatch,

    h_pct_device: GPUPercentageChangeDeviceBatch,

    pub fn init(allocator: std.mem.Allocator, device_id: i32) !StatCalc {
        var self = StatCalc{
            .allocator = allocator,
            .device_id = device_id,
            .gpu_enabled = false,

            .d_ohlc_batch = null,
            .d_pct_result = null,

            .h_pct_device = std.mem.zeroes(GPUPercentageChangeDeviceBatch),
        };

        self.initCUDADevice() catch |err| {
            _ = err;
            std.log.warn("Falling back to CPU calculations due to CUDA init failure", .{});
            return self;
        };

        self.allocateDeviceMemory() catch |err| {
            _ = err;
            std.log.warn("Falling back to CPU calculations due to CUDA memory allocation failure", .{});
            self.gpu_enabled = false;
            return self;
        };

        return self;
    }

    pub fn deinit(self: *StatCalc) void {
        if (self.gpu_enabled) {
            _ = CudaWrapper.freeMemory(self.d_ohlc_batch, self.d_pct_result);
        }

        if (self.d_ohlc_batch) |ptr| {
            self.allocator.destroy(ptr);
        }
        if (self.d_pct_result) |ptr| {
            self.allocator.destroy(ptr);
        }
    }

    fn initCUDADevice(self: *StatCalc) !void {
        const init_err = CudaWrapper.initDevice(self.device_id);
        if (init_err.code != 0) {
            std.log.warn("Failed to initialize CUDA device (CPU fallback): {} ({s})", .{ init_err.code, init_err.message });
            self.gpu_enabled = false;
            return;
        }

        var info: DeviceInfo = undefined;
        const info_err = CudaWrapper.getDeviceInfo(self.device_id, &info);
        if (info_err.code != 0) {
            std.log.warn("Failed to get device info via wrapper: {} ({s})", .{ info_err.code, info_err.message });
            self.gpu_enabled = false;
            return;
        }

        self.gpu_enabled = true;
        std.log.info("Using CUDA device (or CPU fallback): {s}", .{info.name});
        std.log.info("Compute capability: {}.{}", .{ info.major, info.minor });
        std.log.info("Global memory: {} MB", .{@divTrunc(info.totalGlobalMem, 1024 * 1024)});
    }

    fn allocateDeviceMemory(self: *StatCalc) !void {
        self.d_ohlc_batch = try self.allocator.create(GPUOHLCDataBatch);
        self.d_pct_result = try self.allocator.create(GPUPercentageChangeDeviceBatch);

        self.d_ohlc_batch.?.* = std.mem.zeroes(GPUOHLCDataBatch);
        self.d_pct_result.?.* = std.mem.zeroes(GPUPercentageChangeDeviceBatch);

        const kerr = CudaWrapper.allocateMemory(@ptrCast(&self.d_ohlc_batch), @ptrCast(&self.d_pct_result));
        if (kerr.code != 0) {
            std.log.warn("CUDA memory allocation failed via wrapper: {} ({s})", .{ kerr.code, kerr.message });
        }
    }

    pub fn calculateSymbolMapBatch(self: *StatCalc, symbol_map: *const SymbolMap, start_index: usize) !GPUBatchResult {
        _ = start_index;
        const symbol_count = symbol_map.count();
        const max_symbols_to_process = @min(symbol_count, MAX_SYMBOLS);

        var symbols_slice = try self.allocator.alloc(Symbol, max_symbols_to_process);
        defer self.allocator.free(symbols_slice);
        var symbol_names = try self.allocator.alloc([]const u8, max_symbols_to_process);
        defer self.allocator.free(symbol_names);

        var iterator = symbol_map.iterator();
        var all_idx: usize = 0;

        while (iterator.next()) |entry| {
            if (all_idx >= max_symbols_to_process) {
                break;
            }
            symbols_slice[all_idx] = entry.value_ptr.*;
            symbol_names[all_idx] = entry.key_ptr.*;
            all_idx += 1;
        }

        if (symbol_count > MAX_SYMBOLS) {
            std.log.warn(
                "Total symbols ({}) exceeds MAX_SYMBOLS ({}), processing only first {} symbols",
                .{ symbol_count, MAX_SYMBOLS, max_symbols_to_process },
            );
        }

        const num_symbols_to_process = all_idx;

        var result = GPUPercentageChangeResultBatch{
            .device = std.mem.zeroes(GPUPercentageChangeDeviceBatch),
            .symbols = [_][]const u8{&[_]u8{}} ** MAX_SYMBOLS,
            .count = num_symbols_to_process,
        };

        if (num_symbols_to_process > 0) {
            result.device = try self.calculatePercentageChangeBatch(symbols_slice[0..num_symbols_to_process]);
            for (0..num_symbols_to_process) |i| {
                result.symbols[i] = symbol_names[i];
            }
        }

        return GPUBatchResult{
            .percentage_change = result,
        };
    }

    fn calculatePercentageChangeBatch(self: *StatCalc, symbols: []const Symbol) !GPUPercentageChangeDeviceBatch {
        const num_symbols = @min(symbols.len, MAX_SYMBOLS);
        if (num_symbols == 0) return self.h_pct_device;

        self.h_pct_device = std.mem.zeroes(GPUPercentageChangeDeviceBatch);

        const now_ms: i64 = @intCast(@divTrunc(std.time.nanoTimestamp(), 1_000_000));
        for (0..num_symbols) |i| {
            const sym = symbols[i];
            const latest_idx = if (sym.count == 0) 0 else (sym.head + 15 - 1) % 15;
            const current_price_f64 = if (sym.count == 0)
                0.0
            else
                sym.ticker_queue[latest_idx].close_price;
            const current_price = if (sym.current_price != 0.0) sym.current_price else current_price_f64;
            const candle_open = if (sym.candle_open_price != 0.0)
                sym.candle_open_price
            else
                current_price;

            const pct = if (candle_open != 0.0)
                ((current_price - candle_open) / candle_open) * 100.0
            else
                0.0;

            self.h_pct_device.percentage_change[i] = @as(f32, @floatCast(pct));
            self.h_pct_device.current_price[i] = @as(f32, @floatCast(current_price));
            self.h_pct_device.candle_open_price[i] = @as(f32, @floatCast(candle_open));
            self.h_pct_device.candle_timestamp[i] = if (sym.candle_start_time != 0) sym.candle_start_time else now_ms;
        }
        return self.h_pct_device;
    }

    pub fn getDeviceInfo(self: *StatCalc) !void {
        var info: DeviceInfo = undefined;
        const kerr = CudaWrapper.getDeviceInfo(self.device_id, &info);
        if (kerr.code != 0) {
            std.log.warn("Failed to get device info via wrapper: {} ({s})", .{ kerr.code, kerr.message });
            return StatCalcError.CUDAGetPropertiesFailed;
        }

        std.log.info("=== CUDA Device Information ===", .{});
        std.log.info("Device Name: {s}", .{info.name});
        std.log.info("Compute Capability: {}.{}", .{ info.major, info.minor });
        std.log.info("Total Global Memory: {} MB", .{@divTrunc(info.totalGlobalMem, 1024 * 1024)});
        std.log.info("==============================", .{});
    }

    pub fn warmUp(self: *StatCalc) !void {
        var warm_map = SymbolMap.init(self.allocator);
        defer warm_map.deinit();

        var dummy_symbol = Symbol.init();
        const dummy_ohlc = OHLC{
            .open_price = 100.0,
            .high_price = 105.0,
            .low_price = 99.0,
            .close_price = 103.0,
            .volume = 1000.0,
        };

        dummy_symbol.addTicker(dummy_ohlc);
        dummy_symbol.candle_open_price = dummy_ohlc.open_price;
        dummy_symbol.current_price = dummy_ohlc.close_price;
        dummy_symbol.candle_start_time = @intCast(@divTrunc(std.time.nanoTimestamp(), 1_000_000));

        try warm_map.put("WARMUP", dummy_symbol);
        _ = try self.calculateSymbolMapBatch(&warm_map, 0);
    }
};
