const std = @import("std");
const SymbolMap = @import("../symbol-map.zig").SymbolMap;
const types = @import("../types.zig");
const Symbol = types.Symbol;
const OHLC = types.OHLC;
const ERR = @import("../errors.zig");
const StatCalcError = ERR.StatCalcError;
const GPUOHLCDataBatch = types.GPUOHLCDataBatch;
const GPUPercentageChangeDeviceBatch = types.GPUPercentageChangeDeviceBatch;
const GPUPercentageChangeResultBatch = types.GPUPercentageChangeResultBatch;
const MAX_SYMBOLS = types.MAX_SYMBOLS;
const GPUBatchResult = types.GPUBatchResult;

pub const KERNEL_SUCCESS = ERR.KernelError{ .code = 0, .message = "Success" };

pub const BackendPreference = enum {
    auto,
    force_cpu,
    force_gpu,
};

extern fn cuda_get_device_count(count: *c_int) c_int;
extern fn cuda_select_device(device_id: c_int) c_int;
extern fn cuda_init_context() c_int;
extern fn cuda_run_percentage_change_batch(
    open_prices: [*]const f64,
    current_prices: [*]const f64,
    out_pct: [*]f64,
    n: c_int,
) c_int;
extern fn cuda_last_error_string() [*:0]const u8;
extern fn cuda_cleanup() void;

const CudaWrapper = struct {
    fn success() ERR.KernelError {
        return KERNEL_SUCCESS;
    }

    fn initDevice(device_id: c_int) ERR.KernelError {
        const select_rc = cuda_select_device(device_id);
        if (select_rc != 0) {
            return ERR.KernelError{ .code = select_rc, .message = cuda_last_error_string() };
        }

        const init_rc = cuda_init_context();
        if (init_rc != 0) {
            return ERR.KernelError{ .code = init_rc, .message = cuda_last_error_string() };
        }

        return success();
    }

    fn resetDevice() ERR.KernelError {
        cuda_cleanup();
        return success();
    }

    fn getDeviceCount(count: *c_int) ERR.KernelError {
        const rc = cuda_get_device_count(count);
        if (rc != 0) {
            return ERR.KernelError{ .code = rc, .message = cuda_last_error_string() };
        }
        return success();
    }

    fn runPercentageChangeBatch(open_prices: [*]const f64, current_prices: [*]const f64, out_pct: [*]f64, num_symbols: c_int) ERR.KernelError {
        const rc = cuda_run_percentage_change_batch(open_prices, current_prices, out_pct, num_symbols);
        if (rc != 0) {
            return ERR.KernelError{ .code = rc, .message = cuda_last_error_string() };
        }
        return success();
    }
};

pub fn selectBestCUDADevice() !c_int {
    var device_count: c_int = 0;
    const count_err = CudaWrapper.getDeviceCount(&device_count);
    if (count_err.code != 0 or device_count == 0) return 0;
    return 0;
}

pub const StatCalc = struct {
    allocator: std.mem.Allocator,
    device_id: c_int,
    gpu_enabled: bool,
    backend_preference: BackendPreference,
    cuda_failure_logged: bool,

    d_ohlc_batch: ?*GPUOHLCDataBatch,
    d_pct_result: ?*GPUPercentageChangeDeviceBatch,
    h_pct_device: GPUPercentageChangeDeviceBatch,
    trading_start_bucket_ms: i64,

    pub fn init(allocator: std.mem.Allocator, device_id: c_int, backend_preference: BackendPreference) !StatCalc {
        var calc = StatCalc{
            .allocator = allocator,
            .device_id = device_id,
            .gpu_enabled = false,
            .backend_preference = backend_preference,
            .cuda_failure_logged = false,
            .d_ohlc_batch = null,
            .d_pct_result = null,
            .h_pct_device = std.mem.zeroes(GPUPercentageChangeDeviceBatch),
            .trading_start_bucket_ms = 0,
        };

        const now_ms: i64 = @intCast(@divTrunc(std.time.nanoTimestamp(), 1_000_000));
        const current_bucket_idx: i64 = @divTrunc(now_ms, 900_000);
        const current_bucket_ms: i64 = current_bucket_idx * 900_000;
        calc.trading_start_bucket_ms = current_bucket_ms + 900_000;

        std.log.info("StatCalc: trading will start from 15m window starting at {d} ms", .{calc.trading_start_bucket_ms});

        try calc.initCUDADevice();
        try calc.allocateDeviceMemory();
        return calc;
    }

    pub fn deinit(self: *StatCalc) void {
        if (self.d_ohlc_batch) |ptr| self.allocator.destroy(ptr);
        if (self.d_pct_result) |ptr| self.allocator.destroy(ptr);
        self.d_ohlc_batch = null;
        self.d_pct_result = null;
        _ = CudaWrapper.resetDevice();
    }

    fn initCUDADevice(self: *StatCalc) !void {
        var device_count: c_int = 0;
        const count_err = CudaWrapper.getDeviceCount(&device_count);
        if (count_err.code != 0) {
            std.log.info("CUDA devices detected: 0", .{});
            std.log.info("Backend selected: CPU", .{});
            if (self.backend_preference == .force_gpu) {
                std.log.err("--force-gpu requested but CUDA device query failed", .{});
                return StatCalcError.NoCUDADevicesFound;
            }
            return;
        }

        std.log.info("CUDA devices detected: {}", .{device_count});

        if (self.backend_preference == .force_cpu) {
            std.log.info("Backend selected: CPU", .{});
            return;
        }

        if (device_count <= 0) {
            std.log.info("Backend selected: CPU", .{});
            if (self.backend_preference == .force_gpu) {
                std.log.err("--force-gpu requested but no CUDA devices were detected", .{});
                return StatCalcError.NoCUDADevicesFound;
            }
            return;
        }

        const init_err = CudaWrapper.initDevice(self.device_id);
        if (init_err.code != 0) {
            std.log.warn("CUDA init failed; using CPU fallback: {s}", .{init_err.message});
            if (self.backend_preference == .force_gpu) {
                std.log.err("--force-gpu requested but CUDA initialization failed", .{});
                return StatCalcError.CUDAInitFailed;
            }
            std.log.info("Backend selected: CPU", .{});
            return;
        }

        self.gpu_enabled = true;
        std.log.info("Backend selected: GPU", .{});
    }

    fn allocateDeviceMemory(self: *StatCalc) !void {
        self.d_ohlc_batch = try self.allocator.create(GPUOHLCDataBatch);
        self.d_pct_result = try self.allocator.create(GPUPercentageChangeDeviceBatch);
        self.d_ohlc_batch.?.* = std.mem.zeroes(GPUOHLCDataBatch);
        self.d_pct_result.?.* = std.mem.zeroes(GPUPercentageChangeDeviceBatch);
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
            if (all_idx >= max_symbols_to_process) break;
            symbols_slice[all_idx] = entry.value_ptr.*;
            symbol_names[all_idx] = entry.key_ptr.*;
            all_idx += 1;
        }

        var result = GPUPercentageChangeResultBatch{
            .device = std.mem.zeroes(GPUPercentageChangeDeviceBatch),
            .symbols = [_][]const u8{&[_]u8{}} ** MAX_SYMBOLS,
            .count = all_idx,
        };

        if (all_idx > 0) {
            result.device = try self.calculatePercentageChangeBatch(symbols_slice[0..all_idx]);
            for (0..all_idx) |i| result.symbols[i] = symbol_names[i];
        }

        return GPUBatchResult{ .percentage_change = result };
    }

    fn calculatePercentageChangeBatch(self: *StatCalc, symbols: []const Symbol) !GPUPercentageChangeDeviceBatch {
        const num_symbols = @min(symbols.len, MAX_SYMBOLS);
        if (num_symbols == 0) return self.h_pct_device;

        const now_ms: i64 = @intCast(@divTrunc(std.time.nanoTimestamp(), 1_000_000));
        const bucket_index: i64 = @divTrunc(now_ms, 900_000);
        const current_bucket_ms: i64 = bucket_index * 900_000;
        const first_trading_bucket_ms: i64 = self.trading_start_bucket_ms;

        var open_prices = [_]f64{0.0} ** MAX_SYMBOLS;
        var current_prices = [_]f64{0.0} ** MAX_SYMBOLS;
        var gpu_pct = [_]f64{0.0} ** MAX_SYMBOLS;
        var tradeable_bucket = [_]bool{false} ** MAX_SYMBOLS;

        for (0..num_symbols) |i| {
            const sym = symbols[i];
            const latest_idx: usize = if (sym.count == 0) 0 else (sym.head + 15 - 1) % 15;
            const last_close_f64: f64 = if (sym.count == 0) 0.0 else sym.ticker_queue[latest_idx].close_price;
            const current_price_f64: f64 = if (sym.current_price != 0.0) sym.current_price else last_close_f64;

            const prev_bucket: i64 = self.h_pct_device.candle_timestamp[i];
            var open_price_f64: f64 = @as(f64, self.h_pct_device.candle_open_price[i]);

            if (prev_bucket != current_bucket_ms) {
                open_price_f64 = current_price_f64;
                self.h_pct_device.candle_timestamp[i] = current_bucket_ms;
                self.h_pct_device.candle_open_price[i] = @as(f32, @floatCast(open_price_f64));
            } else if (open_price_f64 == 0.0) {
                open_price_f64 = if (current_price_f64 != 0.0)
                    current_price_f64
                else if (sym.count > 0)
                    sym.ticker_queue[(sym.head + 15 - sym.count) % 15].close_price
                else
                    0.0;
                self.h_pct_device.candle_open_price[i] = @as(f32, @floatCast(open_price_f64));
                self.h_pct_device.candle_timestamp[i] = current_bucket_ms;
            }

            open_prices[i] = open_price_f64;
            current_prices[i] = current_price_f64;
            tradeable_bucket[i] = current_bucket_ms >= first_trading_bucket_ms;
            self.h_pct_device.current_price[i] = @as(f32, @floatCast(current_price_f64));
        }

        var used_gpu = false;
        if (self.gpu_enabled) {
            const run_err = CudaWrapper.runPercentageChangeBatch(&open_prices, &current_prices, &gpu_pct, @intCast(num_symbols));
            if (run_err.code == 0) {
                used_gpu = true;
            } else {
                if (!self.cuda_failure_logged) {
                    std.log.warn("CUDA run failed; falling back to CPU for this session: {s}", .{run_err.message});
                    std.log.info("Backend selected: CPU", .{});
                    self.cuda_failure_logged = true;
                }
                self.gpu_enabled = false;
            }
        }

        for (0..num_symbols) |i| {
            const pct_raw: f64 = if (used_gpu)
                gpu_pct[i]
            else if (open_prices[i] != 0.0)
                ((current_prices[i] - open_prices[i]) / open_prices[i]) * 100.0
            else
                0.0;
            const pct_final: f64 = if (tradeable_bucket[i]) pct_raw else 0.0;
            self.h_pct_device.percentage_change[i] = @as(f32, @floatCast(pct_final));
        }

        return self.h_pct_device;
    }

    pub fn getDeviceInfo(self: *StatCalc) !void {
        _ = self;
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

test "GPU percentage change matches CPU reference" {
    var device_count: c_int = 0;
    const count_err = CudaWrapper.getDeviceCount(&device_count);
    if (count_err.code != 0 or device_count <= 0) return error.SkipZigTest;

    const init_err = CudaWrapper.initDevice(0);
    if (init_err.code != 0) return error.SkipZigTest;

    var prng = std.Random.DefaultPrng.init(42);
    const random = prng.random();

    const n: usize = 256;
    var open = [_]f64{0.0} ** n;
    var current = [_]f64{0.0} ** n;
    var cpu = [_]f64{0.0} ** n;
    var gpu = [_]f64{0.0} ** n;

    for (0..n) |i| {
        open[i] = if (i % 29 == 0)
            std.math.nan(f64)
        else if (i % 23 == 0)
            0.0
        else
            (random.float(f64) * 1000.0) - 500.0;
        current[i] = (random.float(f64) * 2000.0) - 1000.0;
        cpu[i] = if (open[i] != 0.0) ((current[i] - open[i]) / open[i]) * 100.0 else 0.0;
    }

    const run_err = CudaWrapper.runPercentageChangeBatch(&open, &current, &gpu, @intCast(n));
    if (run_err.code != 0) return error.SkipZigTest;

    var max_abs: f64 = 0.0;
    for (0..n) |i| {
        const d = @abs(cpu[i] - gpu[i]);
        if (d > max_abs) max_abs = d;
    }

    try std.testing.expect(max_abs < 1e-9);
}
