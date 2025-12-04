const std = @import("std");

pub const TradeLogger = struct {
    allocator: std.mem.Allocator,
    file: std.fs.File,

    pub const TradeRow = struct {
        trade_id: u64,
        symbol: []const u8,
        side: []const u8,
        status: []const u8,
        entry_time: i128,
        close_time: ?i128,
        open_price: f64,
        close_price: f64,
        amount: f64,
        notional: f64,
        pnl: f64,
    };

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
    }

    fn writeHeader(self: *TradeLogger) !void {
        const header = "trade_id,symbol,side,status,entry_time,close_time,open_price,close_price,amount,notional,pnl\n";
        try self.file.writeAll(header);
        try self.file.sync();
    }

    pub fn logOpen(
        self: *TradeLogger,
        trade_id: u64,
        symbol: []const u8,
        side: []const u8,
        entry_time: i128,
        open_price: f64,
        amount: f64,
        notional: f64,
    ) !void {
        const row = TradeRow{
            .trade_id = trade_id,
            .symbol = symbol,
            .side = side,
            .status = "opened",
            .entry_time = entry_time,
            .close_time = null,
            .open_price = open_price,
            .close_price = 0.0,
            .amount = amount,
            .notional = notional,
            .pnl = 0.0,
        };

        try self.writeTradeRow(row);
    }

    pub fn logClose(
        self: *TradeLogger,
        trade_id: u64,
        symbol: []const u8,
        side: []const u8,
        entry_time: i128,
        close_time: i128,
        open_price: f64,
        close_price: f64,
        amount: f64,
        notional: f64,
        pnl: f64,
    ) !void {
        const row = TradeRow{
            .trade_id = trade_id,
            .symbol = symbol,
            .side = side,
            .status = "closed",
            .entry_time = entry_time,
            .close_time = close_time,
            .open_price = open_price,
            .close_price = close_price,
            .amount = amount,
            .notional = notional,
            .pnl = pnl,
        };

        try self.writeTradeRow(row);
    }

    pub fn logInvalidClose(
        self: *TradeLogger,
        trade_id: u64,
        symbol: []const u8,
        entry_time: i128,
    ) !void {
        const now_ts: i128 = @intCast(std.time.nanoTimestamp());
        const row = TradeRow{
            .trade_id = trade_id,
            .symbol = symbol,
            .side = "",
            .status = "invalid_close",
            .entry_time = entry_time,
            .close_time = now_ts,
            .open_price = 0.0,
            .close_price = 0.0,
            .amount = 0.0,
            .notional = 0.0,
            .pnl = 0.0,
        };

        try self.writeTradeRow(row);
    }

    fn writeTradeRow(self: *TradeLogger, row: TradeRow) !void {
        var entry_buf: [64]u8 = undefined;
        var close_buf: [64]u8 = undefined;

        const entry_time = try formatTimestamp(row.entry_time, &entry_buf);
        const close_time = if (row.close_time) |ts|
            try formatTimestamp(ts, &close_buf)
        else
            "";

        const w = self.file.writer();
        try w.print(
            "{d},{s},{s},{s},{s},{s},{d:.4},{d:.4},{d:.6},{d:.4},{d:.4}\n",
            .{
                row.trade_id,
                row.symbol,
                row.side,
                row.status,
                entry_time,
                close_time,
                row.open_price,
                row.close_price,
                row.amount,
                row.notional,
                row.pnl,
            },
        );

        try self.file.sync();
    }
};

pub fn formatTimestamp(timestamp_ns: i128, buffer: *[64]u8) ![]const u8 {
    const secs: i128 = @divFloor(timestamp_ns, 1_000_000_000);
    return try std.fmt.bufPrint(buffer, "{d}", .{secs});
}
