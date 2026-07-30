const std = @import("std");

pub const Direction = enum { none, long, short };

pub const Config = struct {
    threshold_fraction: f64 = 0.05,
    toggle_fraction: f64 = 0.002,
};

pub const ClosedCandle = struct {
    open_price: f64,
    close_price: f64,
    start_ms: i64,
    end_ms: i64,
};

pub const Signal = struct {
    direction: Direction,
    candle_change_fraction: f64,
    pivot_price: f64,
    source_candle_start_ms: i64,
    source_candle_end_ms: i64,
};

pub fn evaluate(candle: ClosedCandle, config: Config) Signal {
    if (!std.math.isFinite(candle.open_price) or
        !std.math.isFinite(candle.close_price) or
        candle.open_price <= 0.0 or
        candle.close_price <= 0.0)
    {
        return .{
            .direction = .none,
            .candle_change_fraction = 0.0,
            .pivot_price = 0.0,
            .source_candle_start_ms = candle.start_ms,
            .source_candle_end_ms = candle.end_ms,
        };
    }

    const change = (candle.close_price - candle.open_price) / candle.open_price;
    const direction: Direction = if (change >= config.threshold_fraction)
        .short
    else if (change <= -config.threshold_fraction)
        .long
    else
        .none;

    return .{
        .direction = direction,
        .candle_change_fraction = change,
        .pivot_price = candle.close_price,
        .source_candle_start_ms = candle.start_ms,
        .source_candle_end_ms = candle.end_ms,
    };
}

pub fn upperToggle(pivot_price: f64, config: Config) f64 {
    return pivot_price * (1.0 + config.toggle_fraction);
}

pub fn lowerToggle(pivot_price: f64, config: Config) f64 {
    return pivot_price * (1.0 - config.toggle_fraction);
}

test "five percent green candle creates short signal" {
    const signal = evaluate(.{
        .open_price = 100.0,
        .close_price = 105.0,
        .start_ms = 0,
        .end_ms = 900_000,
    }, .{});
    try std.testing.expectEqual(Direction.short, signal.direction);
    try std.testing.expectApproxEqAbs(@as(f64, 105.0), signal.pivot_price, 0.000001);
}

test "five percent red candle creates long signal" {
    const signal = evaluate(.{
        .open_price = 100.0,
        .close_price = 95.0,
        .start_ms = 0,
        .end_ms = 900_000,
    }, .{});
    try std.testing.expectEqual(Direction.long, signal.direction);
    try std.testing.expectApproxEqAbs(@as(f64, 95.0), signal.pivot_price, 0.000001);
}

test "sub threshold candle creates no signal" {
    const signal = evaluate(.{
        .open_price = 100.0,
        .close_price = 104.99,
        .start_ms = 0,
        .end_ms = 900_000,
    }, .{});
    try std.testing.expectEqual(Direction.none, signal.direction);
}

test "toggle levels use previous candle close as midpoint" {
    const config = Config{};
    try std.testing.expectApproxEqAbs(@as(f64, 100.2), upperToggle(100.0, config), 0.000001);
    try std.testing.expectApproxEqAbs(@as(f64, 99.8), lowerToggle(100.0, config), 0.000001);
}
