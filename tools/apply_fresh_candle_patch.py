#!/usr/bin/env python3
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
REST = ROOT / "src/data_aggregator/rest_market_data.zig"
PORT = ROOT / "src/trade_handler/portfolio_manager.zig"
MAIN = ROOT / "src/main.zig"

text = REST.read_text()

if "FRESH_CANDLE_DELAY_MS" not in text:
    text = text.replace(
        "const HEARTBEAT_MS: i64 = 30_000;",
        "const HEARTBEAT_MS: i64 = 30_000;\nconst FRESH_CANDLE_DELAY_MS: i64 = 5_000;\nconst KLINE_WORKER_COUNT: usize = 16;",
    )

if "const StagedKline" not in text:
    text = text.replace(
        "const TickerStats = struct {\n    returned: usize = 0,\n    matched: usize = 0,\n    ignored: usize = 0,\n};",
        "const TickerStats = struct {\n    returned: usize = 0,\n    matched: usize = 0,\n    ignored: usize = 0,\n};\n\nconst StagedKline = struct {\n    symbol: []const u8,\n    kline: ?Kline = null,\n};",
    )

new_run = r'''    fn run(self: *RestMarketData) !void {
        var client = std.http.Client{ .allocator = self.allocator };
        defer client.deinit();

        var retry_ms: u64 = 500;
        var loaded_candle_period: i64 = -1;
        var target_period = candlePeriod(nowMs()) + 1;
        var activation_ms = target_period * CANDLE_MS + FRESH_CANDLE_DELAY_MS;
        var last_heartbeat_ms = nowMs();
        var unhealthy_logged = false;

        self.invalidateCandles(target_period);
        std.log.info(
            "Fresh-candle gate active: waiting until next 15m candle + 5 seconds (target_start_ms={d})",
            .{target_period * CANDLE_MS},
        );

        while (!self.shutdown.load(.seq_cst)) {
            const poll_started = nowMs();
            const stats = self.fetchPrices(&client) catch |err| {
                _ = self.total_failures.fetchAdd(1, .seq_cst);
                std.log.warn("Binance Futures all-price request failed: {}; retrying in {}ms", .{ err, retry_ms });
                self.sleepInterruptible(retry_ms);
                retry_ms = @min(retry_ms * 2, 5000);
                continue;
            };
            retry_ms = 500;
            self.last_price_success_ms.store(nowMs(), .seq_cst);
            self.matched_symbols.store(stats.matched, .seq_cst);

            const current = nowMs();
            const current_period = candlePeriod(current);

            // After the first completed cycle, immediately disable new entries at
            // every 15-minute boundary. The portfolio manager can still use the
            // continuously refreshed price to close the old position.
            if (loaded_candle_period >= 0 and current_period != loaded_candle_period and target_period != current_period) {
                target_period = current_period;
                activation_ms = target_period * CANDLE_MS + FRESH_CANDLE_DELAY_MS;
                self.invalidateCandles(target_period);
                std.log.info(
                    "New 15m candle detected; entries paused until +5 seconds (target_start_ms={d})",
                    .{target_period * CANDLE_MS},
                );
            } else if (loaded_candle_period < 0 and current_period > target_period) {
                // Handle a clock jump or a very long network pause before the first cycle.
                target_period = current_period;
                activation_ms = target_period * CANDLE_MS + FRESH_CANDLE_DELAY_MS;
                self.invalidateCandles(target_period);
            }

            if (current >= activation_ms and loaded_candle_period != target_period) {
                self.invalidateCandles(target_period);
                const staged = self.loadCurrentKlinesConcurrent(target_period);
                std.log.info(
                    "Staged official 15m candles for {} of {} symbols",
                    .{ staged, self.symbol_map.count() },
                );

                const minimum_ready_symbols = @max(@as(usize, 1), self.symbol_map.count() / 2);
                if (staged >= minimum_ready_symbols and stats.matched > 0) {
                    const refreshed_stats = self.fetchPrices(&client) catch |err| {
                        _ = self.total_failures.fetchAdd(1, .seq_cst);
                        std.log.warn("Final Futures price refresh after kline loading failed: {}", .{err});
                        self.ready.store(false, .seq_cst);
                        self.sleepInterruptible(retry_ms);
                        continue;
                    };

                    self.last_price_success_ms.store(nowMs(), .seq_cst);
                    self.matched_symbols.store(refreshed_stats.matched, .seq_cst);

                    if (refreshed_stats.matched > 0) {
                        const activated = self.activateStagedCandles(target_period);
                        self.candle_ready_symbols.store(activated, .seq_cst);
                        if (activated >= minimum_ready_symbols) {
                            loaded_candle_period = target_period;
                            self.ready.store(true, .seq_cst);
                            std.log.info(
                                "Fresh 15m cycle ready: activated {} symbols after boundary +5 seconds",
                                .{activated},
                            );
                        } else {
                            self.ready.store(false, .seq_cst);
                            std.log.warn("Fresh candle activation below minimum; entries remain disabled", .{});
                        }
                    }
                } else {
                    self.ready.store(false, .seq_cst);
                    std.log.warn("Fresh candle staging below minimum; entries remain disabled", .{});
                }
            }

            const healthy = self.isHealthy();
            if (!healthy and !unhealthy_logged) {
                std.log.warn("Binance Futures REST market-data feed is not trade-ready; suppressing new entries", .{});
                unhealthy_logged = true;
            } else if (healthy) {
                unhealthy_logged = false;
            }

            if (current - last_heartbeat_ms >= HEARTBEAT_MS) {
                const last = self.last_price_success_ms.load(.seq_cst);
                std.log.info(
                    "REST market-data heartbeat: matched_symbols={} candle_ready_symbols={} last_price_poll_age_ms={} healthy={} total_failures={}",
                    .{ self.matched_symbols.load(.seq_cst), self.candle_ready_symbols.load(.seq_cst), if (last > 0) current - last else -1, healthy, self.total_failures.load(.seq_cst) },
                );
                last_heartbeat_ms = current;
            }

            const elapsed = nowMs() - poll_started;
            if (elapsed < @as(i64, @intCast(self.poll_ms))) {
                self.sleepInterruptible(self.poll_ms - @as(u64, @intCast(elapsed)));
            }
        }
    }

'''

text, count = re.subn(
    r"    fn run\(self: \*RestMarketData\) !void \{.*?\n    fn fetchPrices",
    new_run + "    fn fetchPrices",
    text,
    count=1,
    flags=re.S,
)
if count != 1:
    raise SystemExit("Could not replace RestMarketData.run")

new_loader = r'''    const KlineLoadContext = struct {
        owner: *RestMarketData,
        symbols: []const []const u8,
        results: []StagedKline,
        next_index: *std.atomic.Value(usize),
        expected_period: i64,
    };

    fn klineWorker(ctx: *KlineLoadContext) void {
        var client = std.http.Client{ .allocator = ctx.owner.allocator };
        defer client.deinit();

        while (!ctx.owner.shutdown.load(.seq_cst)) {
            const index = ctx.next_index.fetchAdd(1, .seq_cst);
            if (index >= ctx.symbols.len) break;

            const symbol = ctx.symbols[index];
            const kline = ctx.owner.fetchKline(&client, symbol) catch |err| {
                _ = ctx.owner.total_failures.fetchAdd(1, .seq_cst);
                std.log.warn("Official 15m kline request failed for {s}: {}", .{ symbol, err });
                continue;
            };

            if (candlePeriod(kline.start_ms) != ctx.expected_period) {
                std.log.debug("Skipping {s}: no official kline for the current 15m period", .{symbol});
                continue;
            }

            ctx.results[index].kline = kline;
        }
    }

    fn loadCurrentKlinesConcurrent(self: *RestMarketData, expected_period: i64) usize {
        const symbol_count = self.symbol_map.count();
        if (symbol_count == 0) return 0;

        const symbols = self.allocator.alloc([]const u8, symbol_count) catch return 0;
        defer self.allocator.free(symbols);
        const results = self.allocator.alloc(StagedKline, symbol_count) catch return 0;
        defer self.allocator.free(results);

        var iterator = self.symbol_map.iterator();
        var index: usize = 0;
        while (iterator.next()) |entry| : (index += 1) {
            symbols[index] = entry.key_ptr.*;
            results[index] = .{ .symbol = entry.key_ptr.*, .kline = null };
        }

        var next_index = std.atomic.Value(usize).init(0);
        var context = KlineLoadContext{
            .owner = self,
            .symbols = symbols,
            .results = results,
            .next_index = &next_index,
            .expected_period = expected_period,
        };

        const worker_count = @min(KLINE_WORKER_COUNT, symbol_count);
        const threads = self.allocator.alloc(std.Thread, worker_count) catch return 0;
        defer self.allocator.free(threads);

        var started: usize = 0;
        while (started < worker_count) : (started += 1) {
            threads[started] = std.Thread.spawn(.{}, klineWorker, .{&context}) catch break;
        }
        for (threads[0..started]) |thread| thread.join();

        // Stage fields while candle_ready remains false. This prevents the signal
        // engine from seeing partially initialized candle data.
        var loaded: usize = 0;
        for (results) |result| {
            if (result.kline) |kline| {
                if (self.symbol_map.getPtr(result.symbol)) |symbol| {
                    symbol.candle_ready = false;
                    symbol.candle_start_time = kline.start_ms;
                    symbol.candle_end_time = kline.end_ms;
                    symbol.candle_open_price = kline.open;
                    symbol.candle_close_price = kline.close;
                    symbol.last_kline_update_time = 0;
                    loaded += 1;
                }
            }
        }
        return loaded;
    }

    fn activateStagedCandles(self: *RestMarketData, expected_period: i64) usize {
        const timestamp = nowMs();
        var activated: usize = 0;
        var iterator = self.symbol_map.iterator();
        while (iterator.next()) |entry| {
            const symbol = entry.value_ptr;
            if (candlePeriod(symbol.candle_start_time) == expected_period and
                std.math.isFinite(symbol.candle_open_price) and symbol.candle_open_price > 0)
            {
                symbol.last_kline_update_time = timestamp;
                symbol.candle_ready = true;
                activated += 1;
            }
        }
        return activated;
    }

'''

text, count = re.subn(
    r"    fn loadCurrentKlines\(self: \*RestMarketData, client: \*std\.http\.Client, expected_period: i64\) usize \{.*?\n    fn fetchKline",
    new_loader + "    fn fetchKline",
    text,
    count=1,
    flags=re.S,
)
if count != 1:
    raise SystemExit("Could not replace sequential kline loader")

# Always clear the prior cycle immediately. Prices continue updating, but no
# symbol can become eligible until activateStagedCandles runs after +5 seconds.
text, count = re.subn(
    r"    fn invalidateCandles\(self: \*RestMarketData, expected_period: i64\) void \{.*?\n    fn sleepInterruptible",
    '''    fn invalidateCandles(self: *RestMarketData, expected_period: i64) void {
        _ = expected_period;
        var iterator = self.symbol_map.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.candle_ready = false;
            entry.value_ptr.candle_open_price = 0;
            entry.value_ptr.candle_close_price = 0;
            entry.value_ptr.candle_start_time = 0;
            entry.value_ptr.candle_end_time = 0;
            entry.value_ptr.last_kline_update_time = 0;
        }
        self.candle_ready_symbols.store(0, .seq_cst);
        self.ready.store(false, .seq_cst);
    }

    fn sleepInterruptible''',
    text,
    count=1,
    flags=re.S,
)
if count != 1:
    raise SystemExit("Could not replace candle invalidation")

REST.write_text(text)

port = PORT.read_text()
port = port.replace("pos.pivot_entry_price * 1.001", "pos.pivot_entry_price * 1.002")
port = port.replace("pos.pivot_entry_price * 0.999", "pos.pivot_entry_price * 0.998")
PORT.write_text(port)

main = MAIN.read_text()
main = main.replace("aggregator.waitUntilReady(120_000)", "aggregator.waitUntilReady(1_000_000)")
main = main.replace(
    'std.log.err("Binance Futures REST market-data readiness failed; signal processing and new-entry logic will not start", .{});',
    'std.log.err("Fresh-candle readiness timed out; signal processing and new-entry logic will not start", .{});',
)
MAIN.write_text(main)

print("Fresh-candle patch applied.")
print("Next: zig fmt, build, and tests.")
