const binance = @import("binance.zig");
const SymbolMap = @import("../symbol-map.zig").SymbolMap;
const metrics = @import("../metrics.zig");
const std = @import("std");
const rest_market_data = @import("rest_market_data.zig");
const binance_ws = @import("binance_ws.zig");

pub const DataAggregator = struct {
    symbol_map: *SymbolMap,
    binance: binance.Client,
    allocator: std.mem.Allocator,
    enable_metrics: bool,
    metrics_channel: ?*metrics.MetricsChannel,
    metrics_thread: ?*std.Thread,
    metrics_collector: ?metrics.MetricsCollector,
    rest_market_data: ?*rest_market_data.RestMarketData,
    ws_client: ?*binance_ws.WSClient,

    pub fn init(enable_metrics: bool, allocator: std.mem.Allocator) !DataAggregator {
        var metrics_channel: ?*metrics.MetricsChannel = null;
        var metrics_thread: ?*std.Thread = null;
        var metrics_collector: ?metrics.MetricsCollector = null;

        if (enable_metrics) {
            metrics_channel = try metrics.MetricsChannel.init(allocator);
            metrics_collector = metrics.MetricsCollector.init(metrics_channel.?);
            const thread = try allocator.create(std.Thread);
            thread.* = try std.Thread.spawn(.{}, metrics.metricsThread, .{metrics_channel.?});
            metrics_thread = thread;
        }

        const sym_map = try allocator.create(SymbolMap);
        sym_map.* = SymbolMap.init(allocator);

        const binance_client = try binance.Client.init(allocator, if (enable_metrics) &metrics_collector.? else null);

        return DataAggregator{
            .allocator = allocator,
            .enable_metrics = enable_metrics,
            .metrics_channel = metrics_channel,
            .metrics_thread = metrics_thread,
            .metrics_collector = metrics_collector,
            .symbol_map = sym_map,
            .binance = binance_client,
            .rest_market_data = null,
            .ws_client = null,
        };
    }

    pub fn deinit(self: *DataAggregator) void {
        if (self.ws_client) |ws| {
            ws.stopListener() catch |err| std.log.warn("Failed to stop Binance WS listener: {}", .{err});
            ws.deinit();
            self.allocator.destroy(ws);
            self.ws_client = null;
        }
        if (self.rest_market_data) |rest| {
            rest.deinit();
            self.allocator.destroy(rest);
            self.rest_market_data = null;
        }

        self.symbol_map.deinit();
        self.allocator.destroy(self.symbol_map);

        self.binance.deinit();

        if (self.enable_metrics) {
            if (self.metrics_channel) |channel| {
                channel.stop();
            }
            if (self.metrics_thread) |thread| {
                thread.join();
                self.allocator.destroy(thread);
            }
            if (self.metrics_channel) |channel| {
                channel.deinit();
            }
        }
    }

    pub fn connectToBinance(self: *DataAggregator) !void {
        try self.binance.connect();
        try self.binance.loadSymbols(self.symbol_map);
    }

    pub fn run(self: *DataAggregator) !void {
        const rest = try self.allocator.create(rest_market_data.RestMarketData);
        errdefer self.allocator.destroy(rest);
        rest.* = rest_market_data.RestMarketData.init(self.allocator, self.symbol_map, self.binance.selected_endpoint);
        try rest.start();
        self.rest_market_data = rest;

        const ws = try self.allocator.create(binance_ws.WSClient);
        ws.* = try binance_ws.WSClient.init(self.allocator, if (self.enable_metrics) &self.metrics_collector.? else null);
        ws.startListener(self.symbol_map) catch |err| {
            std.log.warn("Active Binance miniTicker WebSocket unavailable; continuing with REST only: {}", .{err});
            self.allocator.destroy(ws);
            return;
        };
        self.ws_client = ws;
        std.log.info("Active Binance miniTicker WebSocket started for event-driven toggle simulation", .{});
    }

    pub fn waitUntilReady(self: *DataAggregator, timeout_ms: u64) bool {
        return if (self.rest_market_data) |rest| rest.waitUntilReady(timeout_ms) else false;
    }
};
