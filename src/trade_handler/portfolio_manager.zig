const std = @import("std");
const types = @import("../types.zig");
const symbol_map = @import("../symbol-map.zig");
const trade_log = @import("trade_log.zig");
const SymbolMap = symbol_map.SymbolMap;
const TradingSignal = types.TradingSignal;
const SignalType = types.SignalType;
const margin = @import("margin_enforcer.zig");
const binance = @import("binance_futures_client.zig");

pub const PositionSide = enum {
    none,
    long,
    short,
};

// ✅ Fixed config: each trade uses 125 USDT notional with 5x leverage
const TRADE_NOTIONAL_USDT: f64 = 125.0; // position size per trade
const TRADE_LEVERAGE: f64 = 5.0;        // 5x leverage

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
    margin_enforcer: margin.MarginEnforcer,

    trade_logger: ?*trade_log.TradeLogger,

    candle_duration_ns: i128,

    pub fn init(allocator: std.mem.Allocator, sym_map: *const SymbolMap, binance_client: *binance.BinanceFuturesClient) PortfolioManager {
        var logger: ?*trade_log.TradeLogger = null;
        logger = trade_log.TradeLogger.init(allocator) catch |err| {
            std.log.err("Failed to initialize trade logger: {}", .{err});
            return PortfolioManager{
                .allocator = allocator,
                .symbol_map = sym_map,
                .binance_client = binance_client,
                .balance_usdt = 1000.0,
                .fee_rate = 0.001,
                .positions = std.StringHashMap(PortfolioPosition).init(allocator),
                .margin_enforcer = margin.MarginEnforcer.init(allocator, true, binance_client),
                .trade_logger = null,
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
                std.log.err("Failed to fetch USDT balance from Binance: {}", .{err});
            }

            if (balance_opt) |balance| {
                starting_balance = balance;
                std.log.info("Initialized live balance from Binance: ${d:.2} USDT", .{balance});
            } else {
                std.log.warn("Binance balance unavailable, defaulting to simulated balance ${d:.2}", .{starting_balance});
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
            .margin_enforcer = margin.MarginEnforcer.init(allocator, true, binance_client),
            .trade_logger = logger,
            .candle_duration_ns = 15 * 60 * 1_000_000_000,
        };
    }

    pub fn deinit(self: *PortfolioManager) void {
        self.cleanupPositions();
        if (self.trade_logger) |logger| {
            logger.deinit();
            self.allocator.destroy(logger);
        }
        self.positions.deinit();
        self.margin_enforcer.deinit();
    }

    pub fn processSignal(self: *PortfolioManager, signal: TradingSignal) !void {
        const price = try symbol_map.getLastClosePrice(self.symbol_map, signal.symbol_name);
        switch (signal.signal_type) {
            .BUY => self.executeBuy(signal, price),
            .SELL => self.executeSell(signal, price),
            .HOLD => {},
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
    }

    fn executeBuy(self: *PortfolioManager, signal: TradingSignal, price: f64) void {
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

        self.openPosition(signal, price, .long);
    }

    fn executeSell(self: *PortfolioManager, signal: TradingSignal, price: f64) void {
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

        self.openPosition(signal, price, .short);
    }

    fn openPosition(self: *PortfolioManager, signal: TradingSignal, price: f64, side: PositionSide) void {
        // ✅ Fixed leverage & notional
        const leverage: f64 = TRADE_LEVERAGE;              // 5x
        const position_size_usdt: f64 = TRADE_NOTIONAL_USDT; // 125 USDT position

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

        const candle_start_ns = self.currentCandleStart(signal.symbol_name, signal.timestamp);
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
                std.log.info("Opened SHORT on Binance {s} orderId={} qty={d:.6} price=${d:.4}", .{ signal.symbol_name, order.order_id, amount, entry_price });
            }
        } else {
            const amount = position_size_usdt / price;
            self.recordPosition(signal, side, amount, price, candle_start_ns, candle_end_ns, position_size_usdt, null);
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
};
