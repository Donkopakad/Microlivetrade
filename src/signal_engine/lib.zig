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
const binance = @import("../trade_handler/binance_futures_client.zig");
const closed_strategy = @import("../strategy/closed_candle_5pct.zig");

const ObservedCandle = struct {
    open_price: f64,
    latest_price: f64,
    start_ms: i64,
};

pub const SignalEngine = struct {
    allocator: std.mem.Allocator,
    symbol_map: *const SymbolMap,
    stat_calc: *StatCalc,
    trade_handler: TradeHandler,
    binance_client: *binance.BinanceFuturesClient,

    run_flag: std.atomic.Value(bool),
    processing_thread: ?std.Thread,
    batch_thread: ?std.Thread,

    batch_queue: std.ArrayList(GPUBatchResult),
    batch_mutex: std.Thread.Mutex,
    batch_condition: std.Thread.Condition,
    observed_candles: std.StringHashMap(ObservedCandle),

    pub fn init(allocator: std.mem.Allocator, symbol_map: *const SymbolMap, binance_client: *binance.BinanceFuturesClient) !SignalEngine {
        const device_id = try stat_calc_lib.selectBestCUDADevice();
        var stat_calc = try allocator.create(StatCalc);
        stat_calc.* = try StatCalc.init(allocator, device_id);
        try stat_calc.getDeviceInfo();
        try stat_calc.warmUp();

        const trade_handler = TradeHandler.init(allocator, symbol_map, binance_client);

        return SignalEngine{
            .allocator = allocator,
            .symbol_map = symbol_map,
            .stat_calc = stat_calc,
            .trade_handler = trade_handler,
            .binance_client = binance_client,
            .run_flag = std.atomic.Value(bool).init(true),
            .processing_thread = null,
            .batch_thread = null,
            .batch_queue = std.ArrayList(GPUBatchResult).init(allocator),
            .batch_mutex = .{},
            .batch_condition = .{},
            .observed_candles = std.StringHashMap(ObservedCandle).init(allocator),
        };
    }

    pub fn deinit(self: *SignalEngine) void {
        self.run_flag.store(false, .seq_cst);
        self.batch_condition.signal();

        if (self.batch_thread) |t| t.join();
        if (self.processing_thread) |t| t.join();

        self.trade_handler.deinit();
        self.batch_queue.deinit();
        self.observed_candles.deinit();

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
        const candle_duration_ms: i64 = 15 * 60 * 1000;
        var candidates = std.ArrayList(TradingSignal).init(self.allocator);
        defer candidates.deinit();

        for (0..results.count) |i| {
            const symbol_name = results.symbols[i];
            const candle_open = @as(f64, results.device.candle_open_price[i]);
            const current_price = @as(f64, results.device.current_price[i]);
            const candle_start_ms = results.device.candle_timestamp[i];

            if (symbol_name.len == 0 or candle_open <= 0.0 or current_price <= 0.0 or candle_start_ms <= 0) continue;
            if (!std.math.isFinite(candle_open) or !std.math.isFinite(current_price)) continue;

            if (self.observed_candles.getPtr(symbol_name)) |observed| {
                if (observed.start_ms == candle_start_ms) {
                    // Keep the latest observed price. At the timestamp rollover this is
                    // treated as the completed candle close supplied by the REST feed.
                    observed.latest_price = current_price;
                    continue;
                }

                if (candle_start_ms < observed.start_ms) continue;

                const completed = closed_strategy.evaluate(.{
                    .open_price = observed.open_price,
                    .close_price = observed.latest_price,
                    .start_ms = observed.start_ms,
                    .end_ms = observed.start_ms + candle_duration_ms,
                }, .{});

                std.log.info(
                    "[CLOSED_CANDLE] symbol={s} start_ms={d} open={d:.8} close={d:.8} change_pct={d:.4} direction={s} next_start_ms={d}",
                    .{
                        symbol_name,
                        observed.start_ms,
                        observed.open_price,
                        observed.latest_price,
                        completed.candle_change_fraction * 100.0,
                        @tagName(completed.direction),
                        candle_start_ms,
                    },
                );

                // Advance state before queueing the signal so this completed candle
                // can never emit twice even when signal handling is delayed.
                observed.* = .{
                    .open_price = candle_open,
                    .latest_price = current_price,
                    .start_ms = candle_start_ms,
                };

                if (completed.direction == .none) continue;

                const signal_type: SignalType = switch (completed.direction) {
                    .long => .BUY,
                    .short => .SELL,
                    .none => unreachable,
                };
                const entry_start_ns: i128 = @as(i128, candle_start_ms) * 1_000_000;
                const entry_end_ns: i128 = @as(i128, candle_start_ms + candle_duration_ms) * 1_000_000;

                std.log.info(
                    "[CLOSED_CANDLE_SIGNAL] symbol={s} source_change_pct={d:.4} action={s} pivot={d:.8} entry_start_ms={d} entry_end_ms={d}",
                    .{
                        symbol_name,
                        completed.candle_change_fraction * 100.0,
                        @tagName(signal_type),
                        completed.pivot_price,
                        candle_start_ms,
                        candle_start_ms + candle_duration_ms,
                    },
                );

                try candidates.append(.{
                    .symbol_name = symbol_name,
                    .signal_type = signal_type,
                    .rsi_value = @floatCast(completed.candle_change_fraction * 100.0),
                    .orderbook_percentage = 0.0,
                    .timestamp = now_ts,
                    .signal_strength = @floatCast(@min(@abs(completed.candle_change_fraction) / 0.20, 1.0)),
                    .leverage = 1.0,
                    .pivot_price = completed.pivot_price,
                    .entry_candle_start_ns = entry_start_ns,
                    .entry_candle_end_ns = entry_end_ns,
                });
            } else {
                try self.observed_candles.put(symbol_name, .{
                    .open_price = candle_open,
                    .latest_price = current_price,
                    .start_ms = candle_start_ms,
                });
                std.log.info(
                    "[CLOSED_CANDLE_SEED] symbol={s} start_ms={d} open={d:.8} current={d:.8}",
                    .{ symbol_name, candle_start_ms, candle_open, current_price },
                );
            }
        }

        std.mem.sort(TradingSignal, candidates.items, {}, signalLessThan);
        for (candidates.items) |signal| {
            try self.trade_handler.addSignal(signal);
        }
    }

    fn signalLessThan(_: void, a: TradingSignal, b: TradingSignal) bool {
        if (a.timestamp != b.timestamp) return a.timestamp < b.timestamp;
        return std.mem.lessThan(u8, a.symbol_name, b.symbol_name);
    }
};
