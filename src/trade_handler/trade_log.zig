const std = @import("std");

// =============================================================================
// Trade Logger
// =============================================================================

pub const TradeLogger = struct {
    allocator: std.mem.Allocator,
    file: std.fs.File,

    // -------------------------------------------------------------------------
    // TradeRow structure
    // -------------------------------------------------------------------------
    pub const TradeRow = struct {
        event_time_ns: i128,
        event_type: []const u8,
        symbol: []const u8,
        side: []const u8,
        leverage: f64,
        amount: f64,
        position_size_usdt: f64,
        fee_rate: f64,
        entry_price: f64,
        exit_price: f64,
        candle_start_ns: i128,
        candle_end_ns: i128,
        candle_open: f64,
        candle_high: f64,
        candle_low: f64,
        candle_close_at_entry: f64,
        candle_close_at_exit: f64,
        pnl_usdt: f64,
        pct_entry: f64,
        pct_exit: f64,
    };

    // -------------------------------------------------------------------------
    // Initialize logger
    // -------------------------------------------------------------------------
    pub fn init(allocator: std.mem.Allocator) !*TradeLogger {
        try std.fs.cwd().makePath("logs");

        const path = "logs/trades.csv";
        var file = std.fs.cwd().openFile(path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => try std.fs.cwd().createFile(path, .{}),
            else => return err,
        };

        const end_pos = try file.getEndPos();
        try file.seekTo(end_pos);

        const logger = try allocator.create(TradeLogger);
        logger.* = .{
            .allocator = allocator,
            .file = file,
        };

        if (end_pos == 0) {
            try logger.writeHeader();
        }

        return logger;
    }

    pub fn deinit(self: *TradeLogger) void {
        self.file.close();
        self.allocator.destroy(self);
    }

    // -------------------------------------------------------------------------
    // Header Writer
    // -------------------------------------------------------------------------
    fn writeHeader(self: *TradeLogger) !void {
        const header =
            "event_time_utc,event_type,symbol,side,leverage,amount,position_size_usdt,"
            ++ "fee_rate,entry_price,exit_price,candle_start_utc,candle_end_utc,"
            ++ "candle_open,candle_high,candle_low,candle_close_at_entry,"
            ++ "candle_close_at_exit,pnl_usdt,pct_entry,pct_exit\n";

        try self.file.writeAll(header);
        try self.file.sync();
    }

    // -------------------------------------------------------------------------
    // PUBLIC: Log OPEN trade
    // -------------------------------------------------------------------------
    pub fn logOpenTrade(
        self: *TradeLogger,
        event_time_ns: i128,
        symbol: []const u8,
        side: []const u8,
        leverage: f64,
        amount: f64,
        position_size_usdt: f64,
        fee_rate: f64,
        entry_price: f64,
        candle_start_ns: i128,
        candle_end_ns: i128,
        candle_open: f64,
        candle_high: f64,
        candle_low: f64,
        candle_close_at_entry: f64,
        pct_entry: f64,
    ) !void {
        const row = TradeRow{
            .event_time_ns = event_time_ns,
            .event_type = "open",
            .symbol = symbol,
            .side = side,
            .leverage = leverage,
            .amount = amount,
            .position_size_usdt = position_size_usdt,
            .fee_rate = fee_rate,
            .entry_price = entry_price,
            .exit_price = 0.0,
            .candle_start_ns = candle_start_ns,
            .candle_end_ns = candle_end_ns,
            .candle_open = candle_open,
            .candle_high = candle_high,
            .candle_low = candle_low,
            .candle_close_at_entry = candle_close_at_entry,
            .candle_close_at_exit = candle_close_at_entry,
            .pnl_usdt = 0.0,
            .pct_entry = pct_entry,
            .pct_exit = 0.0,
        };
        try self.writeTradeRow(row);
    }

    // -------------------------------------------------------------------------
    // PUBLIC: Log CLOSE trade
    // -------------------------------------------------------------------------
    pub fn logCloseTrade(
        self: *TradeLogger,
        event_time_ns: i128,
        symbol: []const u8,
        side: []const u8,
        leverage: f64,
        amount: f64,
        position_size_usdt: f64,
        fee_rate: f64,
        entry_price: f64,
        exit_price: f64,
        candle_start_ns: i128,
        candle_end_ns: i128,
        candle_open: f64,
        candle_high: f64,
        candle_low: f64,
        candle_close_at_exit: f64,
        pnl_usdt: f64,
        pct_entry: f64,
        pct_exit: f64,
    ) !void {
        const row = TradeRow{
            .event_time_ns = event_time_ns,
            .event_type = "close",
            .symbol = symbol,
            .side = side,
            .leverage = leverage,
            .amount = amount,
            .position_size_usdt = position_size_usdt,
            .fee_rate = fee_rate,
            .entry_price = entry_price,
            .exit_price = exit_price,
            .candle_start_ns = candle_start_ns,
            .candle_end_ns = candle_end_ns,
            .candle_open = candle_open,
            .candle_high = candle_high,
            .candle_low = candle_low,
            .candle_close_at_entry = candle_open,
            .candle_close_at_exit = candle_close_at_exit,
            .pnl_usdt = pnl_usdt,
            .pct_entry = pct_entry,
            .pct_exit = pct_exit,
        };
        try self.writeTradeRow(row);
    }

    // -------------------------------------------------------------------------
    // Write a single CSV row
    // -------------------------------------------------------------------------
    fn writeTradeRow(self: *TradeLogger, row: TradeRow) !void {
        var buf1: [64]u8 = undefined;
        var buf2: [64]u8 = undefined;
        var buf3: [64]u8 = undefined;

        const t_event = try formatTimestamp(row.event_time_ns, &buf1);
        const t_start = try formatTimestamp(row.candle_start_ns, &buf2);
        const t_end = try formatTimestamp(row.candle_end_ns, &buf3);

        const w = self.file.writer();

        try w.print(
            "{s},{s},{s},{s},{d},{d},{d},{d},{d},{d},{s},{s},{d},{d},{d},{d},{d},{d},{d},{d}\n",
            .{
                t_event,
                row.event_type,
                row.symbol,
                row.side,
                row.leverage,
                row.amount,
                row.position_size_usdt,
                row.fee_rate,
                row.entry_price,
                row.exit_price,
                t_start,
                t_end,
                row.candle_open,
                row.candle_high,
                row.candle_low,
                row.candle_close_at_entry,
                row.candle_close_at_exit,
                row.pnl_usdt,
                row.pct_entry,
                row.pct_exit,
            },
        );

        try self.file.sync();
    }
};

// =============================================================================
// Timestamp formatter — Zig 0.14.0 compatible
// =============================================================================
pub fn formatTimestamp(timestamp_ns: i128, buffer: *[64]u8) ![]const u8 {
    const secs: i128 = @divFloor(timestamp_ns, 1_000_000_000);
    return try std.fmt.bufPrint(buffer, "{d}", .{secs});
}
