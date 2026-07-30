const std = @import("std");

pub const PositionSide = enum { long, short };
pub const ToggleAction = enum {
    none,
    reverse_to_long,
    reverse_to_short,
    close_only_gap,
    pause_stale,
};

pub const Config = struct {
    toggle_fraction: f64 = 0.002,
    stale_after_ms: i64 = 1_000,
    max_trigger_slippage_fraction: f64 = 0.003,
    max_single_update_jump_fraction: f64 = 0.005,
};

pub const PriceUpdate = struct {
    previous_price: f64,
    current_price: f64,
    exchange_event_ms: i64,
    local_receive_ms: i64,
};

pub const Decision = struct {
    action: ToggleAction,
    trigger_price: f64,
    data_age_ms: i64,
    jump_fraction: f64,
    trigger_slippage_fraction: f64,
    crossed: bool,
};

fn absFraction(new_value: f64, old_value: f64) f64 {
    if (old_value <= 0.0 or !std.math.isFinite(old_value) or !std.math.isFinite(new_value)) return std.math.inf(f64);
    return @abs((new_value - old_value) / old_value);
}

pub fn triggerFor(side: PositionSide, pivot: f64, config: Config) f64 {
    return switch (side) {
        .long => pivot * (1.0 - config.toggle_fraction),
        .short => pivot * (1.0 + config.toggle_fraction),
    };
}

pub fn decide(side: PositionSide, pivot: f64, update: PriceUpdate, config: Config) Decision {
    const trigger = triggerFor(side, pivot, config);
    const age = update.local_receive_ms - update.exchange_event_ms;
    const jump = absFraction(update.current_price, update.previous_price);

    if (age < 0 or age > config.stale_after_ms) {
        return .{
            .action = .pause_stale,
            .trigger_price = trigger,
            .data_age_ms = age,
            .jump_fraction = jump,
            .trigger_slippage_fraction = 0.0,
            .crossed = false,
        };
    }

    const crossed = switch (side) {
        .long => update.previous_price > trigger and update.current_price <= trigger,
        .short => update.previous_price < trigger and update.current_price >= trigger,
    };

    if (!crossed) {
        return .{
            .action = .none,
            .trigger_price = trigger,
            .data_age_ms = age,
            .jump_fraction = jump,
            .trigger_slippage_fraction = 0.0,
            .crossed = false,
        };
    }

    const slippage = absFraction(update.current_price, trigger);
    const is_gap = jump > config.max_single_update_jump_fraction or
        slippage > config.max_trigger_slippage_fraction;

    if (is_gap) {
        return .{
            .action = .close_only_gap,
            .trigger_price = trigger,
            .data_age_ms = age,
            .jump_fraction = jump,
            .trigger_slippage_fraction = slippage,
            .crossed = true,
        };
    }

    return .{
        .action = switch (side) {
            .long => .reverse_to_short,
            .short => .reverse_to_long,
        },
        .trigger_price = trigger,
        .data_age_ms = age,
        .jump_fraction = jump,
        .trigger_slippage_fraction = slippage,
        .crossed = true,
    };
}

test "long normal lower crossing reverses to short" {
    const result = decide(.long, 100.0, .{
        .previous_price = 99.81,
        .current_price = 99.79,
        .exchange_event_ms = 10_000,
        .local_receive_ms = 10_050,
    }, .{});
    try std.testing.expectEqual(ToggleAction.reverse_to_short, result.action);
}

test "short normal upper crossing reverses to long" {
    const result = decide(.short, 100.0, .{
        .previous_price = 100.19,
        .current_price = 100.21,
        .exchange_event_ms = 10_000,
        .local_receive_ms = 10_040,
    }, .{});
    try std.testing.expectEqual(ToggleAction.reverse_to_long, result.action);
}

test "large gap closes only and does not reverse" {
    const result = decide(.short, 100.0, .{
        .previous_price = 100.19,
        .current_price = 104.0,
        .exchange_event_ms = 10_000,
        .local_receive_ms = 10_050,
    }, .{});
    try std.testing.expectEqual(ToggleAction.close_only_gap, result.action);
}

test "stale data pauses client-side action" {
    const result = decide(.long, 100.0, .{
        .previous_price = 99.81,
        .current_price = 99.79,
        .exchange_event_ms = 10_000,
        .local_receive_ms = 11_500,
    }, .{});
    try std.testing.expectEqual(ToggleAction.pause_stale, result.action);
}
