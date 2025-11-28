const std = @import("std");
const stat_calc_lib = @import("../stat_calc/lib.zig");
const StatCalc = stat_calc_lib.StatCalc;
const SymbolMap = @import("../symbol-map.zig").SymbolMap;
const types = @import("../types.zig");
const GPUPercentageChangeResultBatch = types.GPUPercentageChangeResultBatch;
const GPUBatchResult = types.GPUBatchResult;
const TradingSignal = types.TradingSignal;
const SignalType = types.SignalType;
const TradeHandler = @import("../trade_handler/lib.zig").TradeHandler;
const BatchThread = @import("batch_thread.zig").BatchThread;
const engine_types = @import("types.zig");

pub const SignalEngine = struct {
    allocator: std.mem.Allocator,
    symbol_map: *const SymbolMap,
    stat_calc: *StatCalc,
    trade_handler: TradeHandler,

    run_flag: std.atomic.Value(bool),
    processing_thread: ?std.Thread,
    batch_thread: ?std.Thread,

    batch_queue: std.ArrayList(GPUBatchResult),
    batch_mutex: std.Thread.Mutex,
    batch_condition: std.Thread.Condition,

    pub fn init(allocator: std.mem.Allocator, symbol_map: *const SymbolMap) !SignalEngine {
        const device_id = try stat_calc_lib.selectBestCUDADevice();
        var stat_calc = try allocator.create(StatCalc);
        stat_calc.* = try StatCalc.init(allocator, device_id);
        try stat_calc.getDeviceInfo();
        try stat_calc.warmUp();

        const trade_handler = TradeHandler.init(allocator, symbol_map);

        return SignalEngine{
            .allocator = allocator,
            .symbol_map = symbol_map,
            .stat_calc = stat_calc,
            .trade_handler = trade_handler,
            .run_flag = std.atomic.Value(bool).init(true),
            .processing_thread = null,
            .batch_thread = null,
            .batch_queue = std.ArrayList(GPUBatchResult).init(allocator),
            .batch_mutex = .{},
            .batch_condition = .{},
        };
    }

    pub fn deinit(self: *SignalEngine) void {
        self.run_flag.store(false, .seq_cst);
        self.batch_condition.signal();

        if (self.batch_thread) |t| t.join();
        if (self.processing_thread) |t| t.join();

        self.trade_handler.deinit();
        self.batch_queue.deinit();

        self.stat_calc.deinit();
        self.allocator.destroy(self.stat_calc);
    }

    pub fn run(self: *SignalEngine) !void {
        try self.trade_handler.start();
        try self.startBatchThread();
        try self.startProcessingThread();
    }

    fn startBatchThread(self: *SignalEngine) !void {
        const ctx = BatchThread{
            .stat_calc = self.stat_calc,
            .symbol_map = self.symbol_map,
            .run_flag = &self.run_flag,
            .queue_mutex = &self.batch_mutex,
            .queue_cond = &self.batch_condition,
            .queue = &self.batch_queue,
        };
        self.batch_thread = try std.Thread.spawn(.{ .allocator = self.allocator }, BatchThread.loop, .{ctx});
    }

    fn startProcessingThread(self: *SignalEngine) !void {
        self.processing_thread = try std.Thread.spawn(.{ .allocator = self.allocator }, processingThreadFunction, .{self});
    }

    fn processingThreadFunction(self: *SignalEngine) void {
        std.log.info("Signal processing thread started", .{});
        while (self.run_flag.load(.seq_cst)) {
            self.batch_mutex.lock();
            while (self.batch_queue.items.len == 0 and self.run_flag.load(.seq_cst)) {
                self.batch_condition.wait(&self.batch_mutex);
            }
            if (!self.run_flag.load(.seq_cst)) {
                self.batch_mutex.unlock();
                break;
            }
            var batch = self.batch_queue.orderedRemove(0);
            self.batch_mutex.unlock();

            self.processSignalsParallel(&batch.percentage_change) catch |err| {
                std.log.err("Error processing GPU signals: {}", .{err});
            };
        }
        std.log.info("Signal processing thread stopped", .{});
    }

    fn processSignalsParallel(self: *SignalEngine, pct_results: *GPUPercentageChangeResultBatch) !void {
        try self.generateSignalsFromGpuResults(pct_results);
    }

    pub fn generateSignalsFromGpuResults(self: *SignalEngine, results: *GPUPercentageChangeResultBatch) !void {
        const now_ts: i128 = @intCast(std.time.nanoTimestamp());
        for (0..results.count) |i| {
            const pct = results.device.percentage_change[i];
            const symbol_name = results.symbols[i];
            if (symbol_name.len == 0) continue;

            if (pct >= engine_types.BUY_THRESHOLD) {
                const signal = TradingSignal{
                    .symbol_name = symbol_name,
                    .signal_type = SignalType.BUY,
                    .rsi_value = pct,
                    .orderbook_percentage = pct,
                    .timestamp = now_ts,
                    .signal_strength = @min(@abs(pct) / 20.0, 1.0),
                    .leverage = 1.0,
                };
                try self.trade_handler.addSignal(signal);
            } else if (pct <= engine_types.SELL_THRESHOLD) {
                const signal = TradingSignal{
                    .symbol_name = symbol_name,
                    .signal_type = SignalType.SELL,
                    .rsi_value = pct,
                    .orderbook_percentage = pct,
                    .timestamp = now_ts,
                    .signal_strength = @min(@abs(pct) / 20.0, 1.0),
                    .leverage = 1.0,
                };
                try self.trade_handler.addSignal(signal);
            }
        }
    }
};
