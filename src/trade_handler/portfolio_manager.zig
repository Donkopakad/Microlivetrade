const std = @import("std");
const types = @import("../types.zig");
const symbol_map = @import("../symbol-map.zig");
const trade_log = @import("trade_log.zig");
const SymbolMap = symbol_map.SymbolMap;
const TradingSignal = types.TradingSignal;
const SignalType = types.SignalType;
const margin = @import("margin_enforcer.zig");

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
};

pub const PortfolioManager = struct {
    allocator: std.mem.Allocator,
    symbol_map: *const SymbolMap,

    balance_usdt: f64,
    fee_rate: f64,

    positions: std.StringHashMap(PortfolioPosition),
    margin_enforcer: margin.MarginEnforcer,

    trade_logger: ?*trade_log.TradeLogger,

    candle_duration_ns: i128,

    pub fn init(allocator: std.mem.Allocator, sym_map: *const SymbolMap) PortfolioManager {
        var logger: ?*trade_log.TradeLogger = null;
        logger = trade_log.TradeLogger.init(allocator) catch |err| {
            std.log.err("Failed to initialize trade logger: {}", .{err});
            return PortfolioManager{
                .allocator = allocator,
                .symbol_map = sym_map,
                .balance_usdt = 1000.0,
                .fee_rate = 0.001,
                .positions = std.StringHashMap(PortfolioPosition).init(allocator),
                .margin_enforcer = margin.MarginEnforcer.init(allocator, true),
                .trade_logger = null,
                .candle_duration_ns = 15 * 60 * 1_000_000_000,
            };
        };

        return PortfolioManager{
            .allocator = allocator,
            .symbol_map = sym_map,
            .balance_usdt = 1000.0,
            .fee_rate = 0.001,
            .positions = std.StringHashMap(PortfolioPosition).init(allocator),
            .margin_enforcer = margin.MarginEnforcer.init(allocator, true),
            .trade_logger = logger,
            .candle_duration_ns = 15 * 60 * 1_000_000_000,
        };
    }

    pub fn deinit(self: *PortfolioManager) void {
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
            return;
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
            return;
        }

        self.openPosition(signal, price, .short);
    }

    fn openPosition(self: *PortfolioManager, signal: TradingSignal, price: f64, side: PositionSide) void {
        const leverage = if (signal.leverage > 0) signal.leverage else 1.0;
        const position_size_usdt = @as(f64, @floatCast(@max(10.0, self.balance_usdt * 0.05))) * @as(f64, @floatCast(leverage));
        if (self.balance_usdt < position_size_usdt) {
            std.log.warn(
                "Insufficient balance to open {s} {s}",
                .{ (if (side == .long) "LONG" else "SHORT"), signal.symbol_name },
            );
            return;
        }

        const amount = position_size_usdt / (price * (1.0 + self.fee_rate));
        const candle_start_ns = self.currentCandleStart(signal.symbol_name, signal.timestamp);
        const candle_end_ns = candle_start_ns + self.candle_duration_ns;

        const position = PortfolioPosition{
            .symbol = signal.symbol_name,
            .amount = amount,
            .avg_entry_price = price,
            .entry_timestamp = signal.timestamp,
            .candle_start_timestamp = candle_start_ns,
            .candle_end_timestamp = candle_end_ns,
            .position_size_usdt = position_size_usdt,
            .is_open = true,
            .side = side,
            .leverage = leverage,
        };

        self.positions.put(signal.symbol_name, position) catch |err| {
            std.log.err("Failed to record position: {}", .{err});
            return;
        };

        self.balance_usdt -= position_size_usdt;
        std.log.info("Opened {s} on {s} at ${d:.4} size ${d:.2} candle_end={d}", .{
            if (side == .long) "LONG" else "SHORT",
            signal.symbol_name,
            price,
            position_size_usdt,
            candle_end_ns,
        });

        const candle_snapshot = self.getCandleSnapshot(signal.symbol_name, price);
        const pct_change_from_open_at_entry = if (candle_snapshot.open != 0.0)
            ((price - candle_snapshot.open) / candle_snapshot.open) * 100.0
        else
            0.0;

        if (self.trade_logger) |logger| {
            logger.logOpenTrade(
                signal.timestamp,
                signal.symbol_name,
                if (side == .long) "LONG" else "SHORT",
                leverage,
                amount,
                position_size_usdt,
                self.fee_rate,
                price,
                candle_start_ns,
                candle_end_ns,
                candle_snapshot.open,
                candle_snapshot.high,
                candle_snapshot.low,
                price,
                pct_change_from_open_at_entry,
            ) catch |err| {
                std.log.err("Failed to log open trade: {}", .{err});
            };
        }
    }

    fn closeLong(self: *PortfolioManager, position: *PortfolioPosition, price: f64) void {
        const trade_volume = position.amount * price;
        const fee = trade_volume * self.fee_rate;
        const net = trade_volume - fee;
        const pnl = net - position.position_size_usdt;

        position.is_open = false;
        position.side = .none;
        self.balance_usdt += net;

        std.log.info("Closed LONG {s} at ${d:.4} pnl ${d:.4}", .{ position.symbol, price, pnl });

        const candle_snapshot = self.getCandleSnapshot(position.symbol, price);
        const pct_change_from_open_at_entry = if (candle_snapshot.open != 0.0)
            ((position.avg_entry_price - candle_snapshot.open) / candle_snapshot.open) * 100.0
        else
            0.0;
        const pct_change_from_open_at_exit = if (candle_snapshot.open != 0.0)
            ((price - candle_snapshot.open) / candle_snapshot.open) * 100.0
        else
            0.0;

        if (self.trade_logger) |logger| {
            logger.logCloseTrade(
                std.time.nanoTimestamp(),
                position.symbol,
                "LONG",
                position.leverage,
                position.amount,
                position.position_size_usdt,
                self.fee_rate,
                position.avg_entry_price,
                price,
                position.candle_start_timestamp,
                position.candle_end_timestamp,
                candle_snapshot.open,
                candle_snapshot.high,
                candle_snapshot.low,
                price,
                pnl,
                pct_change_from_open_at_entry,
                pct_change_from_open_at_exit,
            ) catch |err| {
                std.log.err("Failed to log close trade: {}", .{err});
            };
        }
    }

    fn closeShort(self: *PortfolioManager, position: *PortfolioPosition, price: f64) void {
        const cover_cost = position.amount * price;
        const fee = cover_cost * self.fee_rate;
        const total = cover_cost + fee;
        const pnl = position.position_size_usdt - total;

        position.is_open = false;
        position.side = .none;
        self.balance_usdt += position.position_size_usdt + pnl;

        std.log.info("Closed SHORT {s} at ${d:.4} pnl ${d:.4}", .{ position.symbol, price, pnl });

        const candle_snapshot = self.getCandleSnapshot(position.symbol, price);
        const pct_change_from_open_at_entry = if (candle_snapshot.open != 0.0)
            ((position.avg_entry_price - candle_snapshot.open) / candle_snapshot.open) * 100.0
        else
            0.0;
        const pct_change_from_open_at_exit = if (candle_snapshot.open != 0.0)
            ((price - candle_snapshot.open) / candle_snapshot.open) * 100.0
        else
            0.0;

        if (self.trade_logger) |logger| {
            logger.logCloseTrade(
                std.time.nanoTimestamp(),
                position.symbol,
                "SHORT",
                position.leverage,
                position.amount,
                position.position_size_usdt,
                self.fee_rate,
                position.avg_entry_price,
                price,
                position.candle_start_timestamp,
                position.candle_end_timestamp,
                candle_snapshot.open,
                candle_snapshot.high,
                candle_snapshot.low,
                price,
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
};
