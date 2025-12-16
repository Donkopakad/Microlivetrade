const std = @import("std");
const types = @import("../types.zig");
const symbol_map = @import("../symbol-map.zig");
const trade_log = @import("trade_log.zig");
const orderbook_log = @import("orderbook_log.zig");
const SymbolMap = symbol_map.SymbolMap;
const TradingSignal = types.TradingSignal;
const SignalType = types.SignalType;
const margin = @import("margin_enforcer.zig");
const binance = @import("binance_futures_client.zig");
const OrderbookLogger = orderbook_log.OrderbookLogger;

pub const PositionSide = enum {
    none,
    long,
    short,
};

// ✅ Fixed config: each trade uses 50 USDT notional with 5x leverage
const TRADE_NOTIONAL_USDT: f64 = 50.0; // position size per trade
const TRADE_LEVERAGE: f64 = 1.0;        // 1x leverage

// Dust and exposure controls
const DUST_NOTIONAL_THRESHOLD_USD: f64 = 1.0;
const MAX_OPEN_POSITIONS: usize = 10;

const PortfolioPosition = struct {
    symbol: []const u8,
    amount: f64,
    avg_entry_price: f64,
    entry_timestamp: i128,
    candle_start_timestamp: i128,
    candle_end_timestamp: i128,
    position_size_usdt: f64,
    is_open: bool,
    side: PositionSide,
    leverage: f32,
    order_id: ?i64,
};

pub const PortfolioManager = struct {
    allocator: std.mem.Allocator,
    symbol_map: *const SymbolMap,
    binance_client: *binance.BinanceFuturesClient,

    balance_usdt: f64,
    fee_rate: f64,

    positions: std.StringHashMap(PortfolioPosition),
    last_traded_candle_start_ns: std.StringHashMap(i128),
    last_skip_log_candle: std.StringHashMap(i128),
    // NEW: remember which 15m candle we already logged for each symbol
    orderbook_logged_candle_start_ns: std.StringHashMap(i128),
    margin_enforcer: margin.MarginEnforcer,

    trade_logger: ?*trade_log.TradeLogger,

    orderbook_logger: ?*OrderbookLogger,

    candle_duration_ns: i128,

    pub fn init(allocator: std.mem.Allocator, sym_map: *const SymbolMap, binance_client: *binance.BinanceFuturesClient) PortfolioManager {
        var logger: ?*trade_log.TradeLogger = null;
        logger = trade_log.TradeLogger.init(allocator) catch |err| {
            std.log.err("Failed to initialize trade logger: {}", .{ err });
            return PortfolioManager{
                .allocator = allocator,
                .symbol_map = sym_map,
                .binance_client = binance_client,
                .balance_usdt = 1000.0,
                .fee_rate = 0.001,
                .positions = std.StringHashMap(PortfolioPosition).init(allocator),
                .last_traded_candle_start_ns = std.StringHashMap(i128).init(allocator),
                .last_skip_log_candle = std.StringHashMap(i128).init(allocator),
                .orderbook_logged_candle_start_ns = std.StringHashMap(i128).init(allocator),
                .margin_enforcer = margin.MarginEnforcer.init(allocator, true, binance_client),
                .trade_logger = null,
                .orderbook_logger = null,
                .candle_duration_ns = 15 * 60 * 1_000_000_000,
            };
        };

        var orderbook_logger: ?*OrderbookLogger = null;
        orderbook_logger = OrderbookLogger.init(allocator, "orderbook_snapshots_5pct.csv") catch |err| {
            std.log.err("Failed to initialize orderbook logger: {}", .{ err });
            return PortfolioManager{
                .allocator = allocator,
                .symbol_map = sym_map,
                .binance_client = binance_client,
                .balance_usdt = 1000.0,
                .fee_rate = 0.001,
                .positions = std.StringHashMap(PortfolioPosition).init(allocator),
                .last_traded_candle_start_ns = std.StringHashMap(i128).init(allocator),
                .last_skip_log_candle = std.StringHashMap(i128).init(allocator),
                .orderbook_logged_candle_start_ns = std.StringHashMap(i128).init(allocator),
                .margin_enforcer = margin.MarginEnforcer.init(allocator, true, binance_client),
                .trade_logger = logger,
                .orderbook_logger = null,
                .candle_duration_ns = 15 * 60 * 1_000_000_000,
            };
        };

        var starting_balance: f64 = 1000.0;
        if (binance_client.isLive()) {
            var balance_opt: ?f64 = null;

            if (binance_client.fetchUsdtBalance()) |val| {
                // val has type ?f64
                balance_opt = val;
            } else |err| {
                std.log.err("Failed to fetch USDT balance from Binance: {}", .{ err });
            }

            if (balance_opt) |balance| {
                starting_balance = balance;
                std.log.info("Initialized live balance from Binance: ${d:.2} USDT", .{ balance });
            } else {
                std.log.warn("Binance balance unavailable, defaulting to simulated balance ${d:.2}", .{ starting_balance });
            }
        } else {
            std.log.err("Binance futures credentials missing, running in dry-run mode for order placement", .{});
        }

        return PortfolioManager{
            .allocator = allocator,
            .symbol_map = sym_map,
            .binance_client = binance_client,
            .balance_usdt = starting_balance,
            .fee_rate = 0.001,
            .positions = std.StringHashMap(PortfolioPosition).init(allocator),
            .last_traded_candle_start_ns = std.StringHashMap(i128).init(allocator),
            .last_skip_log_candle = std.StringHashMap(i128).init(allocator),
            .orderbook_logged_candle_start_ns = std.StringHashMap(i128).init(allocator),
            .margin_enforcer = margin.MarginEnforcer.init(allocator, true, binance_client),
            .trade_logger = logger,
            .orderbook_logger = orderbook_logger,
            .candle_duration_ns = 15 * 60 * 1_000_000_000,
        };
    }

    pub fn deinit(self: *PortfolioManager) void {
        self.cleanupPositions();
        self.cleanupLastTraded();
        self.cleanupOrderbookLogged();
        if (self.trade_logger) |logger| {
            logger.deinit();
        }
        if (self.orderbook_logger) |logger| {
            logger.deinit();
        }
        self.positions.deinit();
        self.last_traded_candle_start_ns.deinit();
        self.last_skip_log_candle.deinit();
        self.orderbook_logged_candle_start_ns.deinit();
        self.margin_enforcer.deinit();
    }

    pub fn processSignal(self: *PortfolioManager, signal: TradingSignal) !void {
        const price = try symbol_map.getLastClosePrice(self.symbol_map, signal.symbol_name);
        const candle_start_ns = self.currentCandleStart(signal.symbol_name, signal.timestamp);

        // ✅ Only log the FIRST 5% event per symbol per 15-minute candle
        if (!self.canLogSnapshotThisCandle(signal.symbol_name, candle_start_ns)) {
            // We've already captured a 5% event for this symbol in this timeframe
            return;
        }

        self.logOrderbookSnapshot(signal, price, candle_start_ns) catch |err| {
            std.log.warn("Failed to log orderbook snapshot for {s}: {}", .{ signal.symbol_name, err });
            // On failure, do NOT mark as logged so a later event can still be captured
            return;
        };

        // Mark this candle as already logged for this symbol
        self.markSnapshotLogged(signal.symbol_name, candle_start_ns);

        // Research branch: no actual trading
        switch (signal.signal_type) {
            .BUY => {},
            .SELL => {},
            .HOLD => {},
        }
    }

    fn logOrderbookSnapshot(self: *PortfolioManager, signal: TradingSignal, price: f64, candle_start_ns: i128) !void {
        const candle_end_ns = candle_start_ns + self.candle_duration_ns;

        const ohlc = symbol_map.getLastOhlc(self.symbol_map, signal.symbol_name) catch |err| {
            std.log.warn("Skipping snapshot for {s}: failed to fetch OHLC: {}", .{ signal.symbol_name, err });
            return;
        };

        const candle_open = ohlc.open_price;
        const candle_high = ohlc.high_price;
        const candle_low = ohlc.low_price;
        const candle_last_price_at_signal = ohlc.close_price;

        const rsi_14 = @as(f64, signal.rsi_value);

        const sym_opt = self.symbol_map.get(signal.symbol_name);
        if (sym_opt == null) {
            std.log.warn("Skipping snapshot for {s}: symbol not found in map", .{ signal.symbol_name });
            return;
        }
        const sym = sym_opt.?;

        var bid_levels = [_]types.PriceLevel{types.PriceLevel{ .price = 0.0, .quantity = 0.0 }} ** types.MAX_ORDERBOOK_SIZE;
        var ask_levels = [_]types.PriceLevel{types.PriceLevel{ .price = 0.0, .quantity = 0.0 }} ** types.MAX_ORDERBOOK_SIZE;

        const bid_limit = if (sym.orderbook.bid_count > types.MAX_ORDERBOOK_SIZE) types.MAX_ORDERBOOK_SIZE else sym.orderbook.bid_count;
        const ask_limit = if (sym.orderbook.ask_count > types.MAX_ORDERBOOK_SIZE) types.MAX_ORDERBOOK_SIZE else sym.orderbook.ask_count;

        var i: usize = 0;
        while (i < bid_limit) : (i += 1) {
            const actual_idx = (sym.orderbook.bid_head + i) % types.MAX_ORDERBOOK_SIZE;
            bid_levels[i] = sym.orderbook.bids[actual_idx];
        }

        i = 0;
        while (i < ask_limit) : (i += 1) {
            const actual_idx = (sym.orderbook.ask_head + i) % types.MAX_ORDERBOOK_SIZE;
            ask_levels[i] = sym.orderbook.asks[actual_idx];
        }

        var bid_total_20: f64 = 0.0;
        var ask_total_20: f64 = 0.0;
        for (0..types.MAX_ORDERBOOK_SIZE) |idx| {
            bid_total_20 += bid_levels[idx].quantity;
            ask_total_20 += ask_levels[idx].quantity;
        }

        const denom = bid_total_20 + ask_total_20;
        const imbalance: f64 = if (denom == 0) 0 else (bid_total_20 - ask_total_20) / denom;

        const dominant_side: []const u8 = if (imbalance > 0.0)
            "BIDS"
        else if (imbalance < 0.0)
            "ASKS"
        else
            "NEUTRAL";

        const candle_open_price = sym.candle_open_price;
        const current_price = sym.current_price;
        const pct_change = if (candle_open_price == 0.0)
            0.0
        else
            ((current_price - candle_open_price) / candle_open_price) * 100.0;

        const direction: []const u8 = switch (signal.signal_type) {
            .BUY => "UP_5",
            .SELL => "DOWN_5",
            .HOLD => "HOLD",
        };

        var buf: [64]u8 = undefined;
        const event_time_iso = trade_log.formatTimestamp(signal.timestamp, &buf) catch |err| {
            std.log.warn("Failed to format timestamp for {s}: {}", .{ signal.symbol_name, err });
            return;
        };

        if (self.orderbook_logger) |logger| {
            try logger.logSnapshot(
                signal.timestamp,
                event_time_iso,
                signal.symbol_name,
                direction,
                pct_change,
                price,
                candle_start_ns,
                candle_end_ns,
                candle_open,
                candle_high,
                candle_low,
                candle_last_price_at_signal,
                rsi_14,
                bid_total_20,
                ask_total_20,
                imbalance,
                dominant_side,
                &bid_levels,
                &ask_levels,
            );
        }
    }

    pub fn checkStopLossConditions(self: *PortfolioManager) !void {
        const now_ns = std.time.nanoTimestamp();
        var to_close = std.ArrayList([]const u8).init(self.allocator);
        defer to_close.deinit();

        var it = self.positions.iterator();
        while (it.next()) |entry| {
            const position = entry.value_ptr;
            if (!position.is_open) continue;
            if (now_ns >= position.candle_end_timestamp) {
                try to_close.append(entry.key_ptr.*);
            }
        }

        for (to_close.items) |sym_name| {
            if (self.positions.getPtr(sym_name)) |pos| {
                const price = try symbol_map.getLastClosePrice(self.symbol_map, sym_name);
                if (pos.side == .long) {
                    self.closeLong(pos, price);
                } else if (pos.side == .short) {
                    self.closeShort(pos, price);
                }
            }
        }

        // Dust cleanup at 15-minute boundaries (or whenever this routine runs)
        var dust_it = self.positions.iterator();
        while (dust_it.next()) |entry| {
            const pos = entry.value_ptr;
            if (!pos.is_open) continue;

            const price = symbol_map.getLastClosePrice(self.symbol_map, entry.key_ptr.*) catch {
                continue;
            };
            const notional = @abs(pos.amount * price);
            if (notional < DUST_NOTIONAL_THRESHOLD_USD) {
                std.log.info(
                    "Closing dust position for {s}: notional={d:.4} USDT < 1.0",
                    .{ entry.key_ptr.*, notional },
                );
                if (pos.side == .long) {
                    self.closeLong(pos, price);
                } else if (pos.side == .short) {
                    self.closeShort(pos, price);
                }
            }
        }
    }

    pub fn getPositionSide(self: *PortfolioManager, symbol_name: []const u8) PositionSide {
        if (self.positions.getPtr(symbol_name)) |pos| {
            if (pos.is_open) return pos.side;
        }
        return .none;
    }

    fn canTradeThisCandle(self: *PortfolioManager, symbol_name: []const u8, candle_start_ns: i128) bool {
        if (self.last_traded_candle_start_ns.get(symbol_name)) |last| {
            if (last == candle_start_ns) {
                return false;
            }
        }
        return true;
    }

    fn markCandleTraded(self: *PortfolioManager, symbol_name: []const u8, candle_start_ns: i128) void {
        if (self.last_traded_candle_start_ns.getPtr(symbol_name)) |ptr| {
            ptr.* = candle_start_ns;
            return;
        }

        const key_copy = self.allocator.dupe(u8, symbol_name) catch |err| {
            std.log.err("Failed to record traded candle for {s}: {}", .{ symbol_name, err });
            return;
        };
        self.last_traded_candle_start_ns.put(key_copy, candle_start_ns) catch |err| {
            std.log.err("Failed to insert traded candle record for {s}: {}", .{ symbol_name, err });
        };
    }

    pub fn countOpenPositions(self: *PortfolioManager) usize {
        var count: usize = 0;
        var it = self.positions.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.is_open) {
                count += 1;
            }
        }
        return count;
    }

    fn canLogSnapshotThisCandle(self: *PortfolioManager, symbol_name: []const u8, candle_start_ns: i128) bool {
        if (self.orderbook_logged_candle_start_ns.get(symbol_name)) |last| {
            if (last == candle_start_ns) {
                // already logged for this symbol in this 15m window
                return false;
            }
        }
        return true;
    }

    fn markSnapshotLogged(self: *PortfolioManager, symbol_name: []const u8, candle_start_ns: i128) void {
        if (self.orderbook_logged_candle_start_ns.getPtr(symbol_name)) |ptr| {
            ptr.* = candle_start_ns;
            return;
        }

        const key_copy = self.allocator.dupe(u8, symbol_name) catch |err| {
            std.log.err("Failed to record logged snapshot candle for {s}: {}", .{ symbol_name, err });
            return;
        };

        self.orderbook_logged_candle_start_ns.put(key_copy, candle_start_ns) catch |err| {
            std.log.err("Failed to insert logged snapshot candle for {s}: {}", .{ symbol_name, err });
        };
    }

    fn executeBuy(self: *PortfolioManager, signal: TradingSignal, price: f64, candle_start_ns: i128) void {
        self.margin_enforcer.ensureIsolatedMargin(signal.symbol_name) catch |err| {
            std.log.err("Failed to enforce isolated margin for {s}: {}", .{ signal.symbol_name, err });
            return;
        };

        if (self.positions.getPtr(signal.symbol_name)) |pos| {
            if (pos.is_open and pos.side == .short) {
                self.closeShort(pos, price);
            }
            if (pos.is_open and pos.side == .long) {
                return;
            }
        }

        self.openPosition(signal, price, .long, candle_start_ns);
    }

    fn executeSell(self: *PortfolioManager, signal: TradingSignal, price: f64, candle_start_ns: i128) void {
        self.margin_enforcer.ensureIsolatedMargin(signal.symbol_name) catch |err| {
            std.log.err("Failed to enforce isolated margin for {s}: {}", .{ signal.symbol_name, err });
            return;
        };

        if (self.positions.getPtr(signal.symbol_name)) |pos| {
            if (pos.is_open and pos.side == .long) {
                self.closeLong(pos, price);
            }
            if (pos.is_open and pos.side == .short) {
                return;
            }
        }

        self.openPosition(signal, price, .short, candle_start_ns);
    }

    fn openPosition(self: *PortfolioManager, signal: TradingSignal, price: f64, side: PositionSide, candle_start_ns: i128) void {
        // ✅ Fixed leverage & notional
        const leverage: f64 = TRADE_LEVERAGE;                // 5x
        const position_size_usdt: f64 = TRADE_NOTIONAL_USDT; // 125 USDT position

        // Enforce global open position cap
        const open_positions = self.countOpenPositions();
        if (open_positions >= MAX_OPEN_POSITIONS) {
            std.log.warn("Max open positions ({d}) reached; ignoring signal for {s}", .{ MAX_OPEN_POSITIONS, signal.symbol_name });
            return;
        }

        // Margin needed ≈ notional / leverage (≈ 25 USDT)
        const required_margin = position_size_usdt / leverage;

        // In dry-run we enforce this check; in live trading Binance itself will reject if insufficient
        if (self.balance_usdt < required_margin and !self.binance_client.isLive()) {
            std.log.warn(
                "Insufficient balance to open {s} {s}",
                .{ (if (side == .long) "LONG" else "SHORT"), signal.symbol_name },
            );
            return;
        }

        const candle_end_ns = candle_start_ns + self.candle_duration_ns;

        if (self.binance_client.isLive()) {
            if (side == .long) {
                self.binance_client.setLeverage(signal.symbol_name, @intFromFloat(leverage)) catch {};
                const order = self.binance_client.openLong(signal.symbol_name, position_size_usdt, leverage) catch |err| {
                    std.log.err("Failed to open LONG {s} on Binance: {}", .{ signal.symbol_name, err });
                    return;
                };
                defer self.binance_client.freeOrderResult(order);
                const amount = if (order.executed_qty > 0) order.executed_qty else position_size_usdt / price;
                const entry_price = if (order.avg_price > 0) order.avg_price else price;
                const actual_notional = if (order.cum_quote > 0) order.cum_quote else position_size_usdt;
                self.recordPosition(signal, side, amount, entry_price, candle_start_ns, candle_end_ns, actual_notional, order.order_id);
                self.markCandleTraded(signal.symbol_name, candle_start_ns);
                std.log.info("Opened LONG on Binance {s} orderId={} qty={d:.6} price=${d:.4}", .{ signal.symbol_name, order.order_id, amount, entry_price });
            } else {
                self.binance_client.setLeverage(signal.symbol_name, @intFromFloat(leverage)) catch {};
                const order = self.binance_client.openShort(signal.symbol_name, position_size_usdt, leverage) catch |err| {
                    std.log.err("Failed to open SHORT {s} on Binance: {}", .{ signal.symbol_name, err });
                    return;
                };
                defer self.binance_client.freeOrderResult(order);
                const amount = if (order.executed_qty > 0) order.executed_qty else position_size_usdt / price;
                const entry_price = if (order.avg_price > 0) order.avg_price else price;
                const actual_notional = if (order.cum_quote > 0) order.cum_quote else position_size_usdt;
                self.recordPosition(signal, side, amount, entry_price, candle_start_ns, candle_end_ns, actual_notional, order.order_id);
                self.markCandleTraded(signal.symbol_name, candle_start_ns);
                std.log.info("Opened SHORT on Binance {s} orderId={} qty={d:.6} price=${d:.4}", .{ signal.symbol_name, order.order_id, amount, entry_price });
            }
        } else {
            const amount = position_size_usdt / price;
            self.recordPosition(signal, side, amount, price, candle_start_ns, candle_end_ns, position_size_usdt, null);
            self.markCandleTraded(signal.symbol_name, candle_start_ns);
            std.log.info("Opened simulated {s} {s} qty={d:.6} price=${d:.4}", .{ (if (side == .long) "LONG" else "SHORT"), signal.symbol_name, amount, price });
        }
    }

    fn recordPosition(
        self: *PortfolioManager,
        signal: TradingSignal,
        side: PositionSide,
        amount: f64,
        entry_price: f64,
        candle_start_ns: i128,
        candle_end_ns: i128,
        position_size_usdt: f64,
        order_id: ?i64,
    ) void {
        // Use symbol_name directly as the hash-map key (no optional issues).
        if (!self.positions.contains(signal.symbol_name)) {
            self.positions.put(signal.symbol_name, PortfolioPosition{
                .symbol = signal.symbol_name,
                .amount = 0.0,
                .avg_entry_price = 0.0,
                .entry_timestamp = 0,
                .candle_start_timestamp = 0,
                .candle_end_timestamp = 0,
                .position_size_usdt = 0.0,
                .is_open = false,
                .side = .none,
                .leverage = 1.0,
                .order_id = null,
            }) catch unreachable;
        }

        var pos = self.positions.getPtr(signal.symbol_name).?;
        pos.symbol = signal.symbol_name;
        pos.amount = amount;
        pos.avg_entry_price = entry_price;
        pos.entry_timestamp = signal.timestamp;
        pos.candle_start_timestamp = candle_start_ns;
        pos.candle_end_timestamp = candle_end_ns;
        pos.position_size_usdt = position_size_usdt;
        pos.is_open = true;
        pos.side = side;
        pos.leverage = @floatCast(signal.leverage); // kept as-is; just for record
        pos.order_id = order_id;

        if (self.trade_logger) |_| {
            // Trade logging temporarily disabled to avoid mismatches with TradeLogger API.
            // TODO: re-enable once TradeLogger has a matching log function.
        }
    }

    fn currentCandleStart(self: *PortfolioManager, symbol_name: []const u8, timestamp: i128) i128 {
        _ = symbol_name;
        const duration_ns = self.candle_duration_ns;
        const elapsed_since_epoch = timestamp;
        const remainder = @mod(elapsed_since_epoch, duration_ns);
        return elapsed_since_epoch - remainder;
    }

    fn closeLong(self: *PortfolioManager, pos: *PortfolioPosition, price: f64) void {
        if (!pos.is_open or pos.side != .long) return;
        const pnl = (price - pos.avg_entry_price) * pos.amount;
        self.balance_usdt += pnl - (pos.amount * pos.avg_entry_price * self.fee_rate) - (pos.amount * price * self.fee_rate);

        if (self.binance_client.isLive()) {
            const order = self.binance_client.closeLong(pos.symbol, pos.amount) catch |err| {
                std.log.err("Failed to close LONG {s} on Binance: {}", .{ pos.symbol, err });
                return;
            };
            defer self.binance_client.freeOrderResult(order);
            std.log.info("Closed LONG on Binance {s} orderId={} qty={d:.6} price=${d:.4}", .{ pos.symbol, order.order_id, pos.amount, price });
        } else {
            std.log.info("Closed simulated LONG {s} qty={d:.6} price=${d:.4} PnL=${d:.4}", .{ pos.symbol, pos.amount, price, pnl });
        }

        pos.is_open = false;
        pos.side = .none;
    }

    fn closeShort(self: *PortfolioManager, pos: *PortfolioPosition, price: f64) void {
        if (!pos.is_open or pos.side != .short) return;
        const pnl = (pos.avg_entry_price - price) * pos.amount;
        self.balance_usdt += pnl - (pos.amount * pos.avg_entry_price * self.fee_rate) - (pos.amount * price * self.fee_rate);

        if (self.binance_client.isLive()) {
            const order = self.binance_client.closeShort(pos.symbol, pos.amount) catch |err| {
                std.log.err("Failed to close SHORT {s} on Binance: {}", .{ pos.symbol, err });
                return;
            };
            defer self.binance_client.freeOrderResult(order);
            std.log.info("Closed SHORT on Binance {s} orderId={} qty={d:.6} price=${d:.4}", .{ pos.symbol, order.order_id, pos.amount, price });
        } else {
            std.log.info("Closed simulated SHORT {s} qty={d:.6} price=${d:.4} PnL=${d:.4}", .{ pos.symbol, pos.amount, price, pnl });
        }

        pos.is_open = false;
        pos.side = .none;
    }

    fn cleanupPositions(self: *PortfolioManager) void {
        var it = self.positions.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
    }

    fn cleanupLastTraded(self: *PortfolioManager) void {
        var it = self.last_traded_candle_start_ns.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
    }

    fn cleanupOrderbookLogged(self: *PortfolioManager) void {
        var it = self.orderbook_logged_candle_start_ns.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
    }
};
