const std = @import("std");
const types = @import("../types.zig");

pub const OrderbookLogger = struct {
    allocator: std.mem.Allocator,
    file: std.fs.File,
    has_written_header: bool,

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !*OrderbookLogger {
        var file = std.fs.cwd().openFile(path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => try std.fs.cwd().createFile(path, .{}),
            else => return err,
        };

        const end_pos = try file.getEndPos();
        try file.seekTo(end_pos);

        const logger = try allocator.create(OrderbookLogger);
        logger.* = .{
            .allocator = allocator,
            .file = file,
            .has_written_header = end_pos != 0,
        };

        return logger;
    }

    pub fn deinit(self: *OrderbookLogger) void {
        self.file.close();
        self.allocator.destroy(self);
    }

    fn writeHeader(self: *OrderbookLogger) !void {
        if (self.has_written_header) return;

        const header =
            "event_time_ns,event_time_iso,symbol,direction,percent_change,entry_price,"
            ++ "candle_start_ns,candle_end_ns,candle_open,candle_high,candle_low,candle_last_price_at_signal,"
            ++ "rsi_14,"
            ++ "bid_total_20,ask_total_20,imbalance,dominant_side,"
            ++ "bid1_price,bid1_qty,bid2_price,bid2_qty,bid3_price,bid3_qty,bid4_price,bid4_qty,bid5_price,bid5_qty,"
            ++ "bid6_price,bid6_qty,bid7_price,bid7_qty,bid8_price,bid8_qty,bid9_price,bid9_qty,bid10_price,bid10_qty,"
            ++ "bid11_price,bid11_qty,bid12_price,bid12_qty,bid13_price,bid13_qty,bid14_price,bid14_qty,bid15_price,bid15_qty,"
            ++ "bid16_price,bid16_qty,bid17_price,bid17_qty,bid18_price,bid18_qty,bid19_price,bid19_qty,bid20_price,bid20_qty,"
            ++ "ask1_price,ask1_qty,ask2_price,ask2_qty,ask3_price,ask3_qty,ask4_price,ask4_qty,ask5_price,ask5_qty,"
            ++ "ask6_price,ask6_qty,ask7_price,ask7_qty,ask8_price,ask8_qty,ask9_price,ask9_qty,ask10_price,ask10_qty,"
            ++ "ask11_price,ask11_qty,ask12_price,ask12_qty,ask13_price,ask13_qty,ask14_price,ask14_qty,ask15_price,ask15_qty,"
            ++ "ask16_price,ask16_qty,ask17_price,ask17_qty,ask18_price,ask18_qty,ask19_price,ask19_qty,ask20_price,ask20_qty\n";

        try self.file.writeAll(header);
        try self.file.sync();
        self.has_written_header = true;
    }

    pub fn logSnapshot(
        self: *OrderbookLogger,
        event_time_ns: i128,
        event_time_iso: []const u8,
        symbol: []const u8,
        direction: []const u8,
        percent_change: f64,
        entry_price: f64,
        candle_start_ns: i128,
        candle_end_ns: i128,
        candle_open: f64,
        candle_high: f64,
        candle_low: f64,
        candle_last_price_at_signal: f64,
        rsi_14: f64,
        bid_total_20: f64,
        ask_total_20: f64,
        imbalance: f64,
        dominant_side: []const u8,
        bids: []const types.PriceLevel,
        asks: []const types.PriceLevel,
    ) !void {
        try self.writeHeader();

        var writer = self.file.writer();

        try writer.print(
            "{d},{s},{s},{s},{d:.6},{d:.6},{d},{d},{d:.6},{d:.6},{d:.6},{d:.6},{d:.6},{d:.6},{d:.6},{d:.6},{s}",
            .{
                event_time_ns,
                event_time_iso,
                symbol,
                direction,
                percent_change,
                entry_price,
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
            },
        );

        for (0..types.MAX_ORDERBOOK_SIZE) |i| {
            const level = if (i < bids.len) bids[i] else types.PriceLevel{ .price = 0.0, .quantity = 0.0 };
            try writer.print(",{d:.8},{d:.8}", .{ level.price, level.quantity });
        }

        for (0..types.MAX_ORDERBOOK_SIZE) |i| {
            const level = if (i < asks.len) asks[i] else types.PriceLevel{ .price = 0.0, .quantity = 0.0 };
            try writer.print(",{d:.8},{d:.8}", .{ level.price, level.quantity });
        }

        try writer.print("\n", .{});
        try self.file.sync();
    }
};
