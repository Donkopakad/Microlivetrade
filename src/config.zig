const std = @import("std");

pub const live_confirmation_phrase = "I_UNDERSTAND_THIS_IS_LIVE";

pub const Config = struct {
    trade_notional_usdt: f64 = 1000.0,
    leverage: u8 = 5,
    margin_mode: []const u8 = "ISOLATED",
    timeframe_minutes: u16 = 15,
    buy_threshold_percent: f64 = 5.0,
    sell_threshold_percent: f64 = -5.0,
    toggle_percent: f64 = 0.001,
    price_check_interval_ms: u64 = 1000,
    max_global_open_trades: u8 = 1,
    live_trading: bool = false,
    live_confirmed: bool = false,
    api_key_present: bool = false,
    api_secret_present: bool = false,

    pub fn load(allocator: std.mem.Allocator) !Config {
        _ = allocator;
        return .{
            .trade_notional_usdt = envFloat("TRADE_NOTIONAL_USDT", 1000.0),
            .leverage = @intCast(envInt("LEVERAGE", 5)),
            .margin_mode = "ISOLATED",
            .timeframe_minutes = @intCast(envInt("TIMEFRAME_MINUTES", 15)),
            .buy_threshold_percent = envFloat("BUY_THRESHOLD_PERCENT", 5.0),
            .sell_threshold_percent = envFloat("SELL_THRESHOLD_PERCENT", -5.0),
            .toggle_percent = envFloat("TOGGLE_PERCENT", 0.001),
            .price_check_interval_ms = envInt("PRICE_CHECK_INTERVAL_MS", 1000),
            .max_global_open_trades = @intCast(envInt("MAX_GLOBAL_OPEN_TRADES", 1)),
            .live_trading = envBool("LIVE_TRADING", false),
            .live_confirmed = envEquals("LIVE_TRADING_CONFIRM", live_confirmation_phrase),
            .api_key_present = hasEnv("BINANCE_FUTURES_API_KEY"),
            .api_secret_present = hasEnv("BINANCE_FUTURES_API_SECRET"),
        };
    }

    pub fn realOrdersEnabled(self: Config) bool {
        return self.live_trading and self.live_confirmed and self.api_key_present and self.api_secret_present;
    }

    pub fn logStartupSafety(self: Config) void {
        if (self.realOrdersEnabled()) {
            std.log.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", .{});
            std.log.warn("LIVE BINANCE FUTURES TRADING ENABLED: real market orders may be placed", .{});
            std.log.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", .{});
        } else {
            std.log.info("DRY-RUN mode: real orders disabled unless LIVE_TRADING=true, confirmation phrase, and API credentials are present", .{});
        }
    }
};

fn hasEnv(name: []const u8) bool {
    const val = std.process.getEnvVarOwned(std.heap.page_allocator, name) catch return false;
    defer std.heap.page_allocator.free(val);
    return val.len > 0;
}
fn envEquals(name: []const u8, expected: []const u8) bool {
    const val = std.process.getEnvVarOwned(std.heap.page_allocator, name) catch return false;
    defer std.heap.page_allocator.free(val);
    return std.mem.eql(u8, val, expected);
}
fn envBool(name: []const u8, default: bool) bool {
    const val = std.process.getEnvVarOwned(std.heap.page_allocator, name) catch return default;
    defer std.heap.page_allocator.free(val);
    return std.ascii.eqlIgnoreCase(val, "true") or std.mem.eql(u8, val, "1");
}
fn envInt(name: []const u8, default: u64) u64 {
    const val = std.process.getEnvVarOwned(std.heap.page_allocator, name) catch return default;
    defer std.heap.page_allocator.free(val);
    return std.fmt.parseInt(u64, val, 10) catch default;
}
fn envFloat(name: []const u8, default: f64) f64 {
    const val = std.process.getEnvVarOwned(std.heap.page_allocator, name) catch return default;
    defer std.heap.page_allocator.free(val);
    return std.fmt.parseFloat(f64, val) catch default;
}
