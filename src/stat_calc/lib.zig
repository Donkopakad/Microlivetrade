const std = @import("std");
const SymbolMap = @import("../symbol-map.zig").SymbolMap;
const types = @import("../types.zig");
const Symbol = types.Symbol;
const OHLC = types.OHLC;
const ERR = @import("../errors.zig");
const kernel = @import("kernel.zig");
const StatCalcError = ERR.StatCalcError;
const DeviceInfo = types.DeviceInfo;
const GPUOHLCDataBatch = types.GPUOHLCDataBatch;
const GPUPercentageChangeDeviceBatch = types.GPUPercentageChangeDeviceBatch;
const GPUPercentageChangeResultBatch = types.GPUPercentageChangeResultBatch;
const MAX_SYMBOLS = types.MAX_SYMBOLS;
const GPUBatchResult = types.GPUBatchResult;
const CudaWrapper = kernel.CudaWrapper;
const KERNEL_SUCCESS = kernel.KERNEL_SUCCESS;

pub fn selectBestCUDADevice() !c_int {
    var best_device_id: c_int = 0;
    var device_count: c_int = 0;

    const count_err = CudaWrapper.getDeviceCount(&device_count);
    if (count_err.code != 0) {
        std.log.warn("Failed to query CUDA devices (fallback to CPU): {s}", .{count_err.message});
        return 0;
    }

    if (device_count == 0) {
        std.log.warn("No CUDA devices detected; using CPU fallback", .{});
        return 0;
    }

    const select_err = CudaWrapper.selectBestDevice(&best_device_id);
    if (select_err.code != 0) {
        std.log.warn("Failed to select CUDA device (fallback to CPU): {s}", .{select_err.message});
        return 0;
    }

    return best_device_id;
}

pub const StatCalc = struct {
    allocator: std.mem.Allocator,
    device_id: c_int,
    gpu_enabled: bool,

    d_ohlc_batch: ?*GPUOHLCDataBatch,
    d_pct_result: ?*GPUPercentageChangeDeviceBatch,

    h_pct_device: GPUPercentageChangeDeviceBatch,

    pub fn init(allocator: std.mem.Allocator, device_id: c_int) !StatCalc {
        var calc = StatCalc{
            .allocator = allocator,
            .device_id = device_id,
            .gpu_enabled = false,
            .d_ohlc_batch = null,
            .d_pct_result = null,
            .h_pct_device = std.mem.zeroes(GPUPercentageChangeDeviceBatch),
        };

        try calc.initCUDADevice();
        try calc.allocateDeviceMemory();

        return calc;
    }

    pub fn deinit(self: *StatCalc) void {
        if (self.d_ohlc_batch) |ptr| {
            self.allocator.destroy(ptr);
        }
        if (self.d_pct_result) |ptr| {
            self.allocator.destroy(ptr);
        }

        self.d_ohlc_batch = null;
        self.d_pct_result = null;

        const reset_err = CudaWrapper.resetDevice();
        if (reset_err.code != 0) {
            std.log.warn("CUDA device reset failed via wrapper: {} ({s})", .{ reset_err.code, reset_err.message });
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
