const std = @import("std");
const json = std.json;
const websocket = @import("websocket");

const SymbolMap = @import("../symbol-map.zig").SymbolMap;
const OHLC = @import("../types.zig").OHLC;
const metrics = @import("../metrics.zig");

pub const TickerHandler = struct {
    symbol_map: *SymbolMap,
    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex = .{},
    message_count: u64,
    last_reset_time: i64,
    metrics_collector: ?*metrics.MetricsCollector,

    pub fn init(symbol_map: *SymbolMap, allocator: std.mem.Allocator, metrics_collector: ?*metrics.MetricsCollector) !TickerHandler {
        return .{ .symbol_map = symbol_map, .allocator = allocator, .message_count = 0, .last_reset_time = std.time.milliTimestamp(), .metrics_collector = metrics_collector };
    }

    pub fn deinit(self: *TickerHandler) void { _ = self; }

    pub fn serverMessage(self: *TickerHandler, data: []u8, message_type: websocket.MessageType) !void {
        if (self.metrics_collector) |collector| {
            const start_time = std.time.nanoTimestamp();
            defer collector.recordTickerMessage(@as(f64, @floatFromInt(std.time.nanoTimestamp() - start_time)) / 1000.0);
        }
        if (message_type != .text) return;
        const parsed = json.parseFromSlice(json.Value, self.allocator, data, .{}) catch |err| {
            std.log.err("Failed to parse ticker JSON: {}", .{err});
            return;
        };
        defer parsed.deinit();
        const root = parsed.value;
        if (root != .object) return;
        if (root.object.get("k") != null) try self.handleKline(root) else try self.handleMiniTicker(root);
    }

    fn handleMiniTicker(self: *TickerHandler, root: json.Value) !void {
        const symbol_val = root.object.get("s") orelse return;
        const c_val = root.object.get("c") orelse return;
        const e_val = root.object.get("E") orelse return;
        if (symbol_val != .string or c_val != .string or e_val != .integer) return;
        const close_price = std.fmt.parseFloat(f64, c_val.string) catch return;
        const event_time_ms: i64 = @intCast(e_val.integer);
        if (self.symbol_map.getPtr(symbol_val.string)) |sym| {
            self.mutex.lock();
            defer self.mutex.unlock();
            sym.addTicker(.{ .open_price = close_price, .high_price = close_price, .low_price = close_price, .close_price = close_price, .volume = 0.0 });
            const candle_ms = @divFloor(event_time_ms, 900000) * 900000;
            if (sym.candle_start_time == 0) {
                // Temporary non-trading placeholder until the kline stream supplies the official open.
                sym.candle_start_time = candle_ms;
            } else if (candle_ms != sym.candle_start_time) {
                sym.startNewCandle(candle_ms);
            }
            sym.updateCurrentPrice(close_price, event_time_ms);
        }
    }

    fn handleKline(self: *TickerHandler, root: json.Value) !void {
        const k_val = root.object.get("k") orelse return;
        if (k_val != .object) return;
        const k = k_val.object;
        const symbol_val = k.get("s") orelse return;
        const open_val = k.get("o") orelse return;
        const start_val = k.get("t") orelse return;
        const close_val = k.get("c") orelse open_val;
        const event_val = root.object.get("E") orelse start_val;
        if (symbol_val != .string or open_val != .string or start_val != .integer) return;
        const official_open = std.fmt.parseFloat(f64, open_val.string) catch return;
        const close_price = if (close_val == .string) std.fmt.parseFloat(f64, close_val.string) catch official_open else official_open;
        const event_time_ms: i64 = if (event_val == .integer) @intCast(event_val.integer) else @intCast(start_val.integer);
        if (self.symbol_map.getPtr(symbol_val.string)) |sym| {
            self.mutex.lock();
            defer self.mutex.unlock();
            sym.setOfficialCandleOpen(official_open, @intCast(start_val.integer));
            sym.updateCurrentPrice(close_price, event_time_ms);
        }
    }
};
