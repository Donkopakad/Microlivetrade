const std = @import("std");
const SymbolMap = @import("../symbol-map.zig").SymbolMap;

pub const DEFAULT_POLL_MS: u64 = 1000;
pub const MIN_POLL_MS: u64 = 500;
pub const DEFAULT_STALE_MS: u64 = 5000;
const CANDLE_MS: i64 = 15 * 60 * 1000;
const MAX_TICKER_BODY = 4 * 1024 * 1024;
const MAX_KLINE_BODY = 64 * 1024;
const HEARTBEAT_MS: i64 = 30_000;
const FRESH_CANDLE_DELAY_MS: i64 = 5_000;
const KLINE_WORKER_COUNT: usize = 16;

pub const MarketDataError = error{
    HttpStatus,
    InvalidResponse,
    BinanceError,
    MissingField,
    InvalidPrice,
    OversizedResponse,
};

const Kline = struct {
    start_ms: i64,
    end_ms: i64,
    open: f64,
    close: f64,
};

const TickerStats = struct {
    returned: usize = 0,
    matched: usize = 0,
    ignored: usize = 0,
};

const StagedKline = struct {
    symbol: []const u8,
    kline: ?Kline = null,
};

pub const RestMarketData = struct {
    allocator: std.mem.Allocator,
    symbol_map: *SymbolMap,
    endpoint: []const u8,
    poll_ms: u64,
    stale_ms: u64,
    shutdown: std.atomic.Value(bool),
    ready: std.atomic.Value(bool),
    worker: ?std.Thread,
    last_price_success_ms: std.atomic.Value(i64),
    matched_symbols: std.atomic.Value(usize),
    candle_ready_symbols: std.atomic.Value(usize),
    total_failures: std.atomic.Value(usize),

    pub fn init(allocator: std.mem.Allocator, symbol_map: *SymbolMap, endpoint: []const u8) RestMarketData {
        return .{
            .allocator = allocator,
            .symbol_map = symbol_map,
            .endpoint = endpoint,
            .poll_ms = envMillis(allocator, "MARKET_DATA_POLL_MS", DEFAULT_POLL_MS, MIN_POLL_MS),
            .stale_ms = envMillis(allocator, "MARKET_DATA_STALE_MS", DEFAULT_STALE_MS, MIN_POLL_MS),
            .shutdown = std.atomic.Value(bool).init(false),
            .ready = std.atomic.Value(bool).init(false),
            .worker = null,
            .last_price_success_ms = std.atomic.Value(i64).init(0),
            .matched_symbols = std.atomic.Value(usize).init(0),
            .candle_ready_symbols = std.atomic.Value(usize).init(0),
            .total_failures = std.atomic.Value(usize).init(0),
        };
    }

    pub fn start(self: *RestMarketData) !void {
        if (self.worker != null) return error.AlreadyStarted;
        var iterator = self.symbol_map.iterator();
        while (iterator.next()) |entry| entry.value_ptr.market_data_stale_ms = self.stale_ms;
        self.shutdown.store(false, .seq_cst);
        self.worker = try std.Thread.spawn(.{}, workerMain, .{self});
    }

    pub fn deinit(self: *RestMarketData) void {
        self.shutdown.store(true, .seq_cst);
        if (self.worker) |thread| {
            thread.join();
            self.worker = null;
        }
    }

    pub fn waitUntilReady(self: *const RestMarketData, timeout_ms: u64) bool {
        const started = nowMs();
        while (!self.ready.load(.seq_cst) and nowMs() - started < @as(i64, @intCast(timeout_ms))) {
            std.time.sleep(50 * std.time.ns_per_ms);
        }
        return self.ready.load(.seq_cst);
    }

    pub fn isHealthy(self: *const RestMarketData) bool {
        const last = self.last_price_success_ms.load(.seq_cst);
        return self.ready.load(.seq_cst) and last > 0 and nowMs() - last <= @as(i64, @intCast(self.stale_ms));
    }

    fn workerMain(self: *RestMarketData) void {
        self.run() catch |err| {
            if (!self.shutdown.load(.seq_cst)) {
                std.log.err("Binance Futures REST market-data worker exited unexpectedly: {}", .{err});
            }
        };
    }

    fn run(self: *RestMarketData) !void {
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

    fn fetchPrices(self: *RestMarketData, client: *std.http.Client) !TickerStats {
        var url_buf: [256]u8 = undefined;
        const url = try std.fmt.bufPrint(&url_buf, "{s}/fapi/v1/ticker/price", .{self.endpoint});
        const body = try request(client, self.allocator, url, MAX_TICKER_BODY);
        defer self.allocator.free(body);
        return parseTickerResponse(self.allocator, body, self.symbol_map, nowMs());
    }

    const KlineLoadContext = struct {
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

    fn fetchKline(self: *RestMarketData, client: *std.http.Client, symbol: []const u8) !Kline {
        var url_buf: [512]u8 = undefined;
        const url = try std.fmt.bufPrint(&url_buf, "{s}/fapi/v1/klines?symbol={s}&interval=15m&limit=2", .{ self.endpoint, symbol });
        const body = try request(client, self.allocator, url, MAX_KLINE_BODY);
        defer self.allocator.free(body);
        return parseKlineResponse(self.allocator, body);
    }

    fn invalidateCandles(self: *RestMarketData, expected_period: i64) void {
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

    fn sleepInterruptible(self: *const RestMarketData, duration_ms: u64) void {
        var remaining = duration_ms;
        while (remaining > 0 and !self.shutdown.load(.seq_cst)) {
            const part: u64 = @min(remaining, 100);
            std.time.sleep(part * @as(u64, std.time.ns_per_ms));
            remaining -= part;
        }
    }
};

fn request(client: *std.http.Client, allocator: std.mem.Allocator, url: []const u8, max_body: usize) ![]u8 {
    const uri = try std.Uri.parse(url);
    var header_buffer: [16 * 1024]u8 = undefined;
    var req = try client.open(.GET, uri, .{ .server_header_buffer = &header_buffer });
    defer req.deinit();
    try req.send();
    try req.wait();
    try validateStatus(req.response.status);
    return req.reader().readAllAlloc(allocator, max_body) catch |err| switch (err) {
        error.StreamTooLong => MarketDataError.OversizedResponse,
        else => err,
    };
}

fn validateStatus(status: std.http.Status) !void {
    if (status != .ok) return MarketDataError.HttpStatus;
}

fn parseTickerResponse(allocator: std.mem.Allocator, body: []const u8, symbol_map: *SymbolMap, timestamp_ms: i64) !TickerStats {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, body, .{}) catch return MarketDataError.InvalidResponse;
    defer parsed.deinit();
    if (parsed.value == .object and parsed.value.object.get("code") != null) return MarketDataError.BinanceError;
    if (parsed.value != .array) return MarketDataError.InvalidResponse;
    var stats = TickerStats{ .returned = parsed.value.array.items.len };
    for (parsed.value.array.items) |item| {
        if (item != .object) return MarketDataError.InvalidResponse;
        const symbol_value = item.object.get("symbol") orelse return MarketDataError.MissingField;
        const price_value = item.object.get("price") orelse return MarketDataError.MissingField;
        if (symbol_value != .string or price_value != .string) return MarketDataError.InvalidResponse;
        const price = std.fmt.parseFloat(f64, price_value.string) catch return MarketDataError.InvalidPrice;
        if (!std.math.isFinite(price) or price <= 0) return MarketDataError.InvalidPrice;
        if (symbol_map.getPtr(symbol_value.string)) |symbol| {
            symbol.updateCurrentPrice(price, timestamp_ms);
            symbol.addTicker(.{ .open_price = price, .high_price = price, .low_price = price, .close_price = price, .volume = 0 });
            stats.matched += 1;
        } else stats.ignored += 1;
    }
    return stats;
}

fn parseKlineResponse(allocator: std.mem.Allocator, body: []const u8) !Kline {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, body, .{}) catch return MarketDataError.InvalidResponse;
    defer parsed.deinit();
    if (parsed.value == .object and parsed.value.object.get("code") != null) return MarketDataError.BinanceError;
    if (parsed.value != .array or parsed.value.array.items.len == 0) return MarketDataError.InvalidResponse;
    const row = parsed.value.array.items[parsed.value.array.items.len - 1];
    if (row != .array or row.array.items.len < 7) return MarketDataError.InvalidResponse;
    if (row.array.items[0] != .integer or row.array.items[6] != .integer or row.array.items[1] != .string or row.array.items[4] != .string) return MarketDataError.InvalidResponse;
    const open = std.fmt.parseFloat(f64, row.array.items[1].string) catch return MarketDataError.InvalidPrice;
    const close = std.fmt.parseFloat(f64, row.array.items[4].string) catch return MarketDataError.InvalidPrice;
    if (!std.math.isFinite(open) or !std.math.isFinite(close) or open <= 0 or close <= 0) return MarketDataError.InvalidPrice;
    return .{ .start_ms = row.array.items[0].integer, .end_ms = row.array.items[6].integer, .open = open, .close = close };
}

fn storeKline(symbol: anytype, kline: Kline, timestamp_ms: i64) void {
    symbol.candle_start_time = kline.start_ms;
    symbol.candle_end_time = kline.end_ms;
    symbol.candle_open_price = kline.open;
    symbol.candle_close_price = kline.close;
    symbol.last_kline_update_time = timestamp_ms;
    symbol.candle_ready = true;
}

pub fn symbolEligible(symbol: anytype, now_ms: i64, stale_ms: u64) bool {
    return symbol.candle_ready and symbol.current_price > 0 and symbol.candle_open_price > 0 and
        symbol.last_price_update_time > 0 and now_ms - symbol.last_price_update_time <= @as(i64, @intCast(stale_ms)) and
        symbol.last_kline_update_time > 0 and now_ms - symbol.last_kline_update_time <= CANDLE_MS + @as(i64, @intCast(stale_ms)) and
        candlePeriod(symbol.candle_start_time) == candlePeriod(now_ms);
}

fn candlePeriod(timestamp_ms: i64) i64 {
    if (timestamp_ms < 0) return -1;
    return @divFloor(timestamp_ms, CANDLE_MS);
}

fn nowMs() i64 {
    return std.time.milliTimestamp();
}

fn envMillis(allocator: std.mem.Allocator, name: []const u8, default: u64, minimum: u64) u64 {
    const value = std.process.getEnvVarOwned(allocator, name) catch return default;
    defer allocator.free(value);
    const parsed = std.fmt.parseUnsigned(u64, value, 10) catch return default;
    if (parsed < minimum or parsed > std.time.ms_per_day) return default;
    return parsed;
}

test "ticker parsing matches known symbols and ignores unknown symbols" {
    var map = SymbolMap.init(std.testing.allocator);
    defer map.deinit();
    try map.put("BTCUSDT", @import("../types.zig").Symbol.init());
    const stats = try parseTickerResponse(std.testing.allocator, "[{\"symbol\":\"BTCUSDT\",\"price\":\"65000.5\"},{\"symbol\":\"UNKNOWN\",\"price\":\"1\"}]", &map, 1234);
    try std.testing.expectEqual(@as(usize, 2), stats.returned);
    try std.testing.expectEqual(@as(usize, 1), stats.matched);
    try std.testing.expectEqual(@as(usize, 1), stats.ignored);
    try std.testing.expectEqual(@as(f64, 65000.5), map.get("BTCUSDT").?.current_price);
}

test "official Futures kline parsing and storage" {
    const kline = try parseKlineResponse(std.testing.allocator, "[[1710000000000,\"100.0\",\"110\",\"90\",\"105.5\",\"12\",1710000899999],[1710000900000,\"105.5\",\"112\",\"101\",\"109.0\",\"8\",1710001799999]]");
    try std.testing.expectEqual(@as(i64, 1710000900000), kline.start_ms);
    try std.testing.expectEqual(@as(i64, 1710001799999), kline.end_ms);
    try std.testing.expectEqual(@as(f64, 105.5), kline.open);
    try std.testing.expectEqual(@as(f64, 109.0), kline.close);
    var symbol = @import("../types.zig").Symbol.init();
    storeKline(&symbol, kline, 1710001000000);
    try std.testing.expect(symbol.candle_ready);
    try std.testing.expectEqual(kline.open, symbol.candle_open_price);
    symbol.current_price = 110.775;
    try std.testing.expectApproxEqAbs(@as(f64, 5.0), symbol.getPercentageChange(), 0.000001);
}

test "eligibility suppresses stale price stale kline and old candle" {
    const now: i64 = 1710001000000;
    var symbol = @import("../types.zig").Symbol.init();
    symbol.current_price = 101;
    symbol.candle_open_price = 100;
    symbol.candle_start_time = @divFloor(now, CANDLE_MS) * CANDLE_MS;
    symbol.candle_end_time = symbol.candle_start_time + CANDLE_MS - 1;
    symbol.last_price_update_time = now;
    symbol.last_kline_update_time = now;
    symbol.candle_ready = true;
    try std.testing.expect(symbolEligible(&symbol, now, 5000));
    symbol.last_price_update_time = now - 5001;
    try std.testing.expect(!symbolEligible(&symbol, now, 5000));
    symbol.last_price_update_time = now;
    symbol.last_kline_update_time = now - CANDLE_MS - 5001;
    try std.testing.expect(!symbolEligible(&symbol, now, 5000));
    symbol.last_kline_update_time = now;
    symbol.candle_start_time -= CANDLE_MS;
    try std.testing.expect(!symbolEligible(&symbol, now, 5000));
}

test "malformed and Binance error responses are rejected" {
    var map = SymbolMap.init(std.testing.allocator);
    defer map.deinit();
    try std.testing.expectError(MarketDataError.InvalidResponse, parseTickerResponse(std.testing.allocator, "{}", &map, 1));
    try std.testing.expectError(MarketDataError.BinanceError, parseTickerResponse(std.testing.allocator, "{\"code\":-1000,\"msg\":\"bad\"}", &map, 1));
    try std.testing.expectError(MarketDataError.MissingField, parseTickerResponse(std.testing.allocator, "[{\"symbol\":\"BTCUSDT\"}]", &map, 1));
    try std.testing.expectError(MarketDataError.InvalidResponse, parseKlineResponse(std.testing.allocator, "[[1]]"));
    try std.testing.expectError(MarketDataError.BinanceError, parseKlineResponse(std.testing.allocator, "{\"code\":-1000}"));
    try std.testing.expectError(MarketDataError.HttpStatus, validateStatus(.bad_request));
}

test "readiness starts false and shutdown without worker is safe" {
    var map = SymbolMap.init(std.testing.allocator);
    defer map.deinit();
    var rest = RestMarketData.init(std.testing.allocator, &map, "https://fapi.binance.com");
    try std.testing.expect(!rest.ready.load(.seq_cst));
    rest.matched_symbols.store(1, .seq_cst);
    rest.candle_ready_symbols.store(1, .seq_cst);
    rest.ready.store(rest.matched_symbols.load(.seq_cst) > 0 and rest.candle_ready_symbols.load(.seq_cst) == 1, .seq_cst);
    try std.testing.expect(rest.ready.load(.seq_cst));
    rest.deinit();
}
