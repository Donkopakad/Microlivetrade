const std = @import("std");
const json = std.json;
const websocket = @import("websocket");
const http = std.http;

const SymbolMap = @import("../symbol-map.zig").SymbolMap;
const OHLC = @import("../types.zig").OHLC;
const Symbol = @import("../types.zig").Symbol;
const OrderBook = @import("../types.zig").OrderBook;
const TickerHandler = @import("ticker_handler.zig").TickerHandler;
const DepthHandler = @import("depth_handler.zig").DepthHandler;
const metrics = @import("../metrics.zig");

pub const WSClient = struct {
    ticker_client: websocket.Client,
    depth_client: websocket.Client,

    ticker_streams: std.ArrayList([]const u8),
    depth_streams: std.ArrayList([]const u8),

    allocator: std.mem.Allocator,
    ticker_handler: ?*TickerHandler = null,
    depth_handler: ?*DepthHandler = null,
    http_client: http.Client,

    metrics_collector: ?*metrics.MetricsCollector,
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, metrics_collector: ?*metrics.MetricsCollector) !WSClient {
        return WSClient{
            .ticker_client = undefined,
            .depth_client = undefined,

            .ticker_streams = std.ArrayList([]const u8).init(allocator),
            .depth_streams = std.ArrayList([]const u8).init(allocator),

            .allocator = allocator,
            .http_client = http.Client{ .allocator = allocator },

            .metrics_collector = metrics_collector,
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *WSClient) void {
        for (self.ticker_streams.items) |stream| self.allocator.free(stream);
        self.ticker_streams.deinit();

        for (self.depth_streams.items) |stream| self.allocator.free(stream);
        self.depth_streams.deinit();

        if (self.depth_handler) |h| h.deinit();
        if (self.ticker_handler) |h| h.deinit();

        self.ticker_client.deinit();
        self.depth_client.deinit();
    }

    pub fn startListener(self: *WSClient, symbol_map: *SymbolMap) !void {
        // ---- INIT BOTH WS CLIENTS ----
        self.ticker_client = try websocket.Client.init(self.allocator, .{
            .host = "stream.binance.com",
            .port = 9443,
            .tls = true,
        });

        self.depth_client = try websocket.Client.init(self.allocator, .{
            .host = "stream.binance.com",
            .port = 443,
            .tls = true,
        });

        try self.ticker_client.handshake("/ws", .{
            .timeout_ms = 5000,
            .headers = "Host: stream.binance.com:9443",
        });

        try self.depth_client.handshake("/ws", .{
            .timeout_ms = 5000,
            .headers = "Host: stream.binance.com:9443",
        });

        // ---- CONSTRUCT STREAM NAMES ----
        var it = symbol_map.iterator();
        while (it.next()) |entry| {
            const sym_lower = try std.ascii.allocLowerString(self.allocator, entry.key_ptr.*);
            defer self.allocator.free(sym_lower);

            const ticker = try std.fmt.allocPrint(self.allocator, "{s}@miniTicker", .{sym_lower});
            try self.ticker_streams.append(ticker);

            const depth = try std.fmt.allocPrint(self.allocator, "{s}@depth", .{sym_lower});
            try self.depth_streams.append(depth);
        }

        // ---- SUBSCRIBE TICKER ----
        const tmsg = .{
            .method = "SUBSCRIBE",
            .params = self.ticker_streams.items,
            .id = 1,
        };
        const tjson = try json.stringifyAlloc(self.allocator, tmsg, .{});
        defer self.allocator.free(tjson);
        try self.ticker_client.write(tjson);

        // ---- SUBSCRIBE DEPTH ----
        const dmsg = .{
            .method = "SUBSCRIBE",
            .params = self.depth_streams.items,
            .id = 2,
        };
        const djson = try json.stringifyAlloc(self.allocator, dmsg, .{});
        defer self.allocator.free(djson);
        try self.depth_client.write(djson);

        // ---- CREATE HANDLERS ----
        self.ticker_handler = try self.allocator.create(TickerHandler);
        self.ticker_handler.?.* = try TickerHandler.init(symbol_map, self.allocator, self.metrics_collector);

        self.depth_handler = try self.allocator.create(DepthHandler);
        self.depth_handler.?.* = try DepthHandler.init(symbol_map, self.allocator, &self.http_client, self.metrics_collector);

        // ---- START READ LOOPS IN THREADS ----
        _ = self.ticker_client.readLoopInNewThread(self.ticker_handler.?) catch |err| {
            std.log.err("Ticker WS loop crashed: {}", .{err});
            // TODO reconnectTicker()
        };

        _ = self.depth_client.readLoopInNewThread(self.depth_handler.?) catch |err| {
            std.log.err("Depth WS loop crashed: {}", .{err});
            // TODO reconnectDepth()
        };
    }

    pub fn stopListener(self: *WSClient) !void {
        // UNSUBSCRIBE ticker
        if (self.ticker_streams.items.len > 0) {
            const msg = .{
                .method = "UNSUBSCRIBE",
                .params = self.ticker_streams.items,
                .id = 3,
            };
            const j = try json.stringifyAlloc(self.allocator, msg, .{});
            defer self.allocator.free(j);
            try self.ticker_client.write(j);
        }

        // UNSUBSCRIBE depth
        if (self.depth_streams.items.len > 0) {
            const msg = .{
                .method = "UNSUBSCRIBE",
                .params = self.depth_streams.items,
                .id = 4,
            };
            const j = try json.stringifyAlloc(self.allocator, msg, .{});
            defer self.allocator.free(j);
            try self.depth_client.write(j);
        }

        try self.ticker_client.close(.{});
        try self.depth_client.close(.{});
    }
};
