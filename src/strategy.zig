const std = @import("std");

pub const Side = enum { flat, long, short };
pub const TradeState = enum { FLAT, OPENING, ACTIVE_LONG, ACTIVE_SHORT, CLOSING, REVERSING, RECOVERY };
pub const SignalDirection = enum { long, short };

pub const CandleWindow = struct { start_ms: i64, end_ms: i64 };
pub const SignalCandidate = struct { symbol: []const u8, timestamp_ms: i64, direction: SignalDirection, pct: f64 };
pub const ToggleLevels = struct { upper: f64, lower: f64 };

pub fn candleWindow(timestamp_ms: i64, timeframe_minutes: u16) CandleWindow {
    const interval_ms: i64 = @as(i64, timeframe_minutes) * 60 * 1000;
    const start = @divFloor(timestamp_ms, interval_ms) * interval_ms;
    return .{ .start_ms = start, .end_ms = start + interval_ms };
}

pub fn percentageChange(current_price: f64, official_open: f64) f64 {
    if (official_open <= 0.0) return 0.0;
    return ((current_price - official_open) / official_open) * 100.0;
}

pub fn signalFor(pct: f64, buy_threshold: f64, sell_threshold: f64) ?SignalDirection {
    if (pct >= buy_threshold) return .long;
    if (pct <= sell_threshold) return .short;
    return null;
}

pub fn toggleLevels(entry_reference_price: f64, toggle_percent: f64) ToggleLevels {
    return .{ .upper = entry_reference_price * (1.0 + toggle_percent), .lower = entry_reference_price * (1.0 - toggle_percent) };
}

pub fn requiredSide(price: f64, levels: ToggleLevels, current: Side) Side {
    if (price >= levels.upper) return .long;
    if (price <= levels.lower) return .short;
    return current;
}

pub fn shouldExit(now_ms: i64, candle_end_ms: i64) bool { return now_ms >= candle_end_ms; }

pub fn lessThanCandidate(_: void, a: SignalCandidate, b: SignalCandidate) bool {
    if (a.timestamp_ms != b.timestamp_ms) return a.timestamp_ms < b.timestamp_ms;
    return std.mem.lessThan(u8, a.symbol, b.symbol);
}

pub fn selectWinner(candidates: []SignalCandidate) ?SignalCandidate {
    if (candidates.len == 0) return null;
    std.mem.sort(SignalCandidate, candidates, {}, lessThanCandidate);
    return candidates[0];
}

pub const GlobalTradeLock = struct {
    value: std.atomic.Value(u8) = std.atomic.Value(u8).init(0),
    pub fn tryAcquire(self: *GlobalTradeLock) bool {
        return self.value.cmpxchgStrong(0, 1, .seq_cst, .seq_cst) == null;
    }
    pub fn release(self: *GlobalTradeLock) void { self.value.store(0, .seq_cst); }
    pub fn isLocked(self: *GlobalTradeLock) bool { return self.value.load(.seq_cst) != 0; }
};

pub const ReversalAction = enum { none, close_then_open_long, close_then_open_short };
pub fn reversalAction(current: Side, required: Side) ReversalAction {
    if (required == .long and current == .short) return .close_then_open_long;
    if (required == .short and current == .long) return .close_then_open_short;
    return .none;
}

test "15-minute candle-boundary calculation" {
    const w = candleWindow(10 * 60 * 1000 + 7 * 1000, 15);
    try std.testing.expectEqual(@as(i64, 0), w.start_ms);
    try std.testing.expectEqual(@as(i64, 900000), w.end_ms);
}
test "+5% long signal and -5% short signal" {
    try std.testing.expectEqual(SignalDirection.long, signalFor(percentageChange(105, 100), 5, -5).?);
    try std.testing.expectEqual(SignalDirection.short, signalFor(percentageChange(95, 100), 5, -5).?);
}
test "fixed upper and lower toggle calculations" {
    const l = toggleLevels(100, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 100.1), l.upper, 0.000001);
    try std.testing.expectApproxEqAbs(@as(f64, 99.9), l.lower, 0.000001);
}
test "global single-trade lock" {
    var lock = GlobalTradeLock{};
    try std.testing.expect(lock.tryAcquire());
    try std.testing.expect(!lock.tryAcquire());
    lock.release();
    try std.testing.expect(lock.tryAcquire());
}
test "deterministic winner selection" {
    var c = [_]SignalCandidate{ .{ .symbol = "ETHUSDT", .timestamp_ms = 2, .direction = .long, .pct = 5 }, .{ .symbol = "BTCUSDT", .timestamp_ms = 1, .direction = .short, .pct = -5 }, .{ .symbol = "ADAUSDT", .timestamp_ms = 1, .direction = .long, .pct = 5 } };
    const w = selectWinner(&c).?;
    try std.testing.expectEqualStrings("ADAUSDT", w.symbol);
}
test "reversal logic and final candle-end exit" {
    try std.testing.expectEqual(ReversalAction.close_then_open_long, reversalAction(.short, .long));
    try std.testing.expectEqual(ReversalAction.close_then_open_short, reversalAction(.long, .short));
    try std.testing.expect(shouldExit(900000, 900000));
}
