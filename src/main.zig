const DataAggregator = @import("data_aggregator/lib.zig").DataAggregator;
const SignalEngine = @import("signal_engine/lib.zig").SignalEngine;
const std = @import("std");
const binance = @import("trade_handler/binance_futures_client.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var enable_metrics = false;
    for (args) |arg| {
        if (std.mem.eql(u8, arg, "--metrics"[0..]) or std.mem.eql(u8, arg, "metrics"[0..])) {
            enable_metrics = true;
            break;
        }
    }

    var futures_client = try allocator.create(binance.BinanceFuturesClient);
    futures_client.* = try binance.BinanceFuturesClient.initFromEnv(allocator);
    defer {
        futures_client.deinit();
        allocator.destroy(futures_client);
    }

    const smp_allocator = std.heap.smp_allocator;
    var aggregator = try DataAggregator.init(enable_metrics, smp_allocator);
    defer aggregator.deinit();

    var signal_engine = try SignalEngine.init(smp_allocator, aggregator.symbol_map, futures_client);
    defer signal_engine.deinit();

    try aggregator.connectToBinance();
    try aggregator.run();

    std.debug.print("WebSockets flowing, starting continuous Signal Engine and Trading...\n", .{});

    try signal_engine.run();

    std.log.info("Trading system is running continuously. Press Ctrl+C to terminate.", .{});
    while (true) {
        std.time.sleep(std.time.ns_per_s);
    }
}
