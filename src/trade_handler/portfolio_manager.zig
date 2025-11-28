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
            binance_client.fetchUsdtBalance() catch |err| {
                std.log.err("Failed to fetch USDT balance from Binance: {}", .{err});
            } else |balance_opt| {
                if (balance_opt) |balance| {
                    starting_balance = balance;
                    std.log.info("Initialized live balance from Binance: ${d:.2} USDT", .{balance});
                } else {
                    std.log.warn("Binance balance unavailable, defaulting to simulated balance ${d:.2}", .{starting_balance});
                }
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
        const leverage = if (signal.leverage > 0) signal.leverage else 1.0;
        const base_notional = @as(f64, @floatCast(@max(10.0, self.balance_usdt * 0.05)));
        const position_size_usdt = base_notional * @as(f64, @floatCast(leverage));
        if (self.balance_usdt < position_size_usdt and !self.binance_client.isLive()) {
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
                return;
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
                return;
            }
        }

        const amount = position_size_usdt / (price * (1.0 + self.fee_rate));
        self.recordPosition(signal, side, amount, price, candle_start_ns, candle_end_ns, position_size_usdt, null);
        std.log.info("Opened {s} on {s} at ${d:.4} size ${d:.2} candle_end={d}", .{
            if (side == .long) "LONG" else "SHORT",
            signal.symbol_name,
            price,
            position_size_usdt,
            candle_end_ns,
        });
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
        const position = PortfolioPosition{
            .symbol = signal.symbol_name,
            .amount = amount,
            .avg_entry_price = entry_price,
            .entry_timestamp = signal.timestamp,
            .candle_start_timestamp = candle_start_ns,
            .candle_end_timestamp = candle_end_ns,
            .position_size_usdt = position_size_usdt,
            .is_open = true,
            .side = side,
            .leverage = if (signal.leverage > 0) signal.leverage else 1.0,
            .order_id = order_id,
        };

        self.positions.put(signal.symbol_name, position) catch |err| {
            std.log.err("Failed to record position: {}", .{err});
            return;
        };

        self.balance_usdt -= position_size_usdt;

        const candle_snapshot = self.getCandleSnapshot(signal.symbol_name, entry_price);
        const pct_change_from_open_at_entry = if (candle_snapshot.open != 0.0)
            ((entry_price - candle_snapshot.open) / candle_snapshot.open) * 100.0
        else
            0.0;

        if (self.trade_logger) |logger| {
            logger.logOpenTrade(
                signal.timestamp,
                signal.symbol_name,
                if (side == .long) "LONG" else "SHORT",
                position.leverage,
                amount,
                position_size_usdt,
                self.fee_rate,
                entry_price,
                candle_start_ns,
                candle_end_ns,
                candle_snapshot.open,
                candle_snapshot.high,
                candle_snapshot.low,
                entry_price,
                pct_change_from_open_at_entry,
            ) catch |err| {
                std.log.err("Failed to log open trade: {}", .{err});
            };
        }
    }

    fn closeLong(self: *PortfolioManager, position: *PortfolioPosition, price: f64) void {
        var close_price = price;
        var executed_qty = position.amount;
        if (self.binance_client.isLive()) {
            const order = self.binance_client.closeLong(position.symbol, position.amount) catch |err| {
                std.log.err("Failed to close LONG {s} on Binance: {}", .{ position.symbol, err });
                return;
            };
            defer self.binance_client.freeOrderResult(order);
            if (order.executed_qty > 0) executed_qty = order.executed_qty;
            if (order.avg_price > 0) close_price = order.avg_price;
            if (order.cum_quote > 0) {
                // Adjust notional for accurate balance tracking
                position.position_size_usdt = order.cum_quote;
            }
        }

        const trade_volume = executed_qty * close_price;
        const fee = trade_volume * self.fee_rate;
        const net = trade_volume - fee;
        const pnl = net - position.position_size_usdt;

        position.is_open = false;
        position.side = .none;
        self.balance_usdt += net;

        std.log.info("Closed LONG {s} at ${d:.4} pnl ${d:.4}", .{ position.symbol, close_price, pnl });

        const candle_snapshot = self.getCandleSnapshot(position.symbol, close_price);
        const pct_change_from_open_at_entry = if (candle_snapshot.open != 0.0)
            ((position.avg_entry_price - candle_snapshot.open) / candle_snapshot.open) * 100.0
        else
            0.0;
        const pct_change_from_open_at_exit = if (candle_snapshot.open != 0.0)
            ((close_price - candle_snapshot.open) / candle_snapshot.open) * 100.0
        else
            0.0;

        if (self.trade_logger) |logger| {
            logger.logCloseTrade(
                std.time.nanoTimestamp(),
                position.symbol,
                "LONG",
                position.leverage,
                executed_qty,
                position.position_size_usdt,
                self.fee_rate,
                position.avg_entry_price,
                close_price,
                position.candle_start_timestamp,
                position.candle_end_timestamp,
                candle_snapshot.open,
                candle_snapshot.high,
                candle_snapshot.low,
                close_price,
                pnl,
                pct_change_from_open_at_entry,
                pct_change_from_open_at_exit,
            ) catch |err| {
                std.log.err("Failed to log close trade: {}", .{err});
            };
        }
    }

    fn closeShort(self: *PortfolioManager, position: *PortfolioPosition, price: f64) void {
        var close_price = price;
        var executed_qty = position.amount;
        if (self.binance_client.isLive()) {
            const order = self.binance_client.closeShort(position.symbol, position.amount) catch |err| {
                std.log.err("Failed to close SHORT {s} on Binance: {}", .{ position.symbol, err });
                return;
            };
            defer self.binance_client.freeOrderResult(order);
            if (order.executed_qty > 0) executed_qty = order.executed_qty;
            if (order.avg_price > 0) close_price = order.avg_price;
            if (order.cum_quote > 0) {
                position.position_size_usdt = order.cum_quote;
            }
        }

        const cover_cost = executed_qty * close_price;
        const fee = cover_cost * self.fee_rate;
        const total = cover_cost + fee;
        const pnl = position.position_size_usdt - total;

        position.is_open = false;
        position.side = .none;
        self.balance_usdt += position.position_size_usdt + pnl;

        std.log.info("Closed SHORT {s} at ${d:.4} pnl ${d:.4}", .{ position.symbol, close_price, pnl });

        const candle_snapshot = self.getCandleSnapshot(position.symbol, close_price);
        const pct_change_from_open_at_entry = if (candle_snapshot.open != 0.0)
            ((position.avg_entry_price - candle_snapshot.open) / candle_snapshot.open) * 100.0
        else
            0.0;
        const pct_change_from_open_at_exit = if (candle_snapshot.open != 0.0)
            ((close_price - candle_snapshot.open) / candle_snapshot.open) * 100.0
        else
            0.0;

        if (self.trade_logger) |logger| {
            logger.logCloseTrade(
                std.time.nanoTimestamp(),
                position.symbol,
                "SHORT",
                position.leverage,
                executed_qty,
                position.position_size_usdt,
                self.fee_rate,
                position.avg_entry_price,
                close_price,
                position.candle_start_timestamp,
                position.candle_end_timestamp,
                candle_snapshot.open,
                candle_snapshot.high,
                candle_snapshot.low,
                close_price,
                pnl,
                pct_change_from_open_at_entry,
                pct_change_from_open_at_exit,
            ) catch |err| {
                std.log.err("Failed to log close trade: {}", .{err});
            };
        }
    }

    pub fn getPositionSide(self: *const PortfolioManager, symbol_name: []const u8) PositionSide {
        if (self.positions.get(symbol_name)) |pos| {
            if (pos.is_open) return pos.side;
        }
        return .none;
    }

    fn currentCandleStart(self: *PortfolioManager, symbol_name: []const u8, timestamp_ns: i128) i128 {
        if (self.symbol_map.get(symbol_name)) |symbol| {
            if (symbol.candle_start_time > 0) {
                return @as(i128, symbol.candle_start_time) * 1_000_000;
            }
        }
        const duration_ms = @divFloor(self.candle_duration_ns, 1_000_000);
        const ts_ms = @divFloor(timestamp_ns, 1_000_000);
        const bucket = @divTrunc(ts_ms, duration_ms);
        const start_ms = bucket * duration_ms;
        return start_ms * 1_000_000;
    }

    const CandleSnapshot = struct {
        open: f64,
        high: f64,
        low: f64,
        close: f64,
    };

    fn getCandleSnapshot(self: *const PortfolioManager, symbol_name: []const u8, fallback_close: f64) CandleSnapshot {
        var open_price = fallback_close;
        var high_price = fallback_close;
        var low_price = fallback_close;
        var close_price = fallback_close;

        if (self.symbol_map.get(symbol_name)) |symbol| {
            if (symbol.count > 0) {
                const latest_idx = if (symbol.count == 15)
                    (symbol.head + 15 - 1) % 15
                else
                    (symbol.head + symbol.count - 1) % 15;
                const latest = symbol.ticker_queue[latest_idx];
                open_price = if (symbol.candle_open_price != 0.0) symbol.candle_open_price else latest.open_price;
                high_price = latest.high_price;
                low_price = latest.low_price;
                close_price = latest.close_price;
            } else {
                open_price = if (symbol.candle_open_price != 0.0) symbol.candle_open_price else open_price;
            }
        }

        return CandleSnapshot{
            .open = open_price,
            .high = high_price,
            .low = low_price,
            .close = close_price,
        };
    }

    fn cleanupPositions(self: *PortfolioManager) void {
        var it = self.positions.iterator();
        while (it.next()) |entry| {
            _ = entry;
        }
    }
};
