#!/usr/bin/env python3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace_once(path: Path, old: str, new: str, label: str) -> None:
    text = path.read_text()
    if old not in text:
        raise SystemExit(f"[failed] {label}: expected text not found in {path}")
    path.write_text(text.replace(old, new, 1))
    print(f"[apply] {label}")


types_path = ROOT / "src/types.zig"
signal_path = ROOT / "src/signal_engine/lib.zig"
portfolio_path = ROOT / "src/trade_handler/portfolio_manager.zig"

replace_once(
    types_path,
    """    signal_strength: f32,\n    leverage: f32,\n};""",
    """    signal_strength: f32,\n    leverage: f32,\n    // Strategy-provided fixed midpoint and exact entry-candle boundaries.\n    // Defaults preserve compatibility with older signal producers.\n    pivot_price: f64 = 0.0,\n    entry_candle_start_ns: i128 = 0,\n    entry_candle_end_ns: i128 = 0,\n};""",
    "extend TradingSignal with midpoint and candle boundaries",
)

replace_once(
    signal_path,
    'const binance = @import("../trade_handler/binance_futures_client.zig");\n',
    'const binance = @import("../trade_handler/binance_futures_client.zig");\nconst closed_strategy = @import("../strategy/closed_candle_5pct.zig");\n',
    "import closed candle strategy",
)

replace_once(
    signal_path,
    """pub const SignalEngine = struct {\n""",
    """const ObservedCandle = struct {\n    open_price: f64,\n    latest_price: f64,\n    start_ms: i64,\n};\n\npub const SignalEngine = struct {\n""",
    "add observed candle state",
)

replace_once(
    signal_path,
    """    batch_condition: std.Thread.Condition,\n""",
    """    batch_condition: std.Thread.Condition,\n    observed_candles: std.StringHashMap(ObservedCandle),\n""",
    "add candle state map",
)

replace_once(
    signal_path,
    """            .batch_condition = .{},\n""",
    """            .batch_condition = .{},\n            .observed_candles = std.StringHashMap(ObservedCandle).init(allocator),\n""",
    "initialize candle state map",
)

replace_once(
    signal_path,
    """        self.batch_queue.deinit();\n\n        self.stat_calc.deinit();\n""",
    """        self.batch_queue.deinit();\n        self.observed_candles.deinit();\n\n        self.stat_calc.deinit();\n""",
    "deinitialize candle state map",
)

start = signal_path.read_text().index("    pub fn generateSignalsFromGpuResults")
end = signal_path.read_text().index("    fn signalLessThan", start)
text = signal_path.read_text()
new_fn = r'''    pub fn generateSignalsFromGpuResults(self: *SignalEngine, results: *GPUPercentageChangeResultBatch) !void {
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

'''
signal_path.write_text(text[:start] + new_fn + text[end:])
print("[apply] replace forming-candle threshold logic with completed-candle rollover logic")

replace_once(
    portfolio_path,
    """        const price = try symbol_map.getLastClosePrice(self.symbol_map, signal.symbol_name);\n        const candle_start_ns = self.currentCandleStart(signal.symbol_name, signal.timestamp);\n""",
    """        const price = try symbol_map.getLastClosePrice(self.symbol_map, signal.symbol_name);\n        const candle_start_ns = if (signal.entry_candle_start_ns > 0)\n            signal.entry_candle_start_ns\n        else\n            self.currentCandleStart(signal.symbol_name, signal.timestamp);\n""",
    "use strategy-provided entry candle start",
)

replace_once(
    portfolio_path,
    """        const candle_end_ns = candle_start_ns + self.candle_duration_ns;\n""",
    """        const candle_end_ns = if (signal.entry_candle_end_ns > candle_start_ns)\n            signal.entry_candle_end_ns\n        else\n            candle_start_ns + self.candle_duration_ns;\n        const strategy_pivot = if (signal.pivot_price > 0.0) signal.pivot_price else price;\n""",
    "use strategy-provided candle end and midpoint",
)

# Replace only the two normal-entry record calls; toggle entries already preserve pivot.
text = portfolio_path.read_text()
text = text.replace(
    "self.recordPosition(signal, side, amount, entry_price, candle_start_ns, candle_end_ns, actual_notional, order.order_id, entry_price);",
    "self.recordPosition(signal, side, amount, entry_price, candle_start_ns, candle_end_ns, actual_notional, order.order_id, strategy_pivot);",
    2,
)
text = text.replace(
    "self.recordPosition(signal, side, amount, price, candle_start_ns, candle_end_ns, position_size_usdt, null, price);",
    "self.recordPosition(signal, side, amount, price, candle_start_ns, candle_end_ns, position_size_usdt, null, strategy_pivot);",
    1,
)
portfolio_path.write_text(text)
print("[apply] preserve previous closed candle close as fixed toggle midpoint")

print("\nPatch applied successfully.")
print("Run:")
print("  /opt/zig-0.14.1/zig test src/strategy/closed_candle_5pct.zig")
print("  /opt/zig-0.14.1/zig test src/trade_handler/toggle_protection.zig")
print("  /opt/zig-0.14.1/zig build")
print("  /opt/zig-0.14.1/zig build test")
