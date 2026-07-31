const std = @import("std");
const types = @import("types.zig");
const Symbol = types.Symbol;
const GetPriceError = @import("errors.zig").GetPriceError;

pub const SymbolMap = std.StringHashMap(Symbol);

pub const MarketQuote = struct {
    price: f64,
    exchange_event_ms: i64,
};

pub fn dump(self: *const SymbolMap) void {
    var it = self.iterator();
    while (it.next()) |entry| {
        const symbol = entry.value_ptr;
        if (!symbol.orderbook.hasSignificantSpread(0.2)) {
            continue;
        }
        std.log.info("Symbol: {s}", .{entry.key_ptr.*});
        std.log.info(" Ticker queue length: {}", .{symbol.count});
        var idx: usize = 0;
        while (idx < symbol.count) : (idx += 1) {
            const ohlc = symbol.ticker_queue[idx];
            std.log.info(" [{}] O:{d} H:{d} L:{d} C:{d} V:{d}", .{ idx, ohlc.open_price, ohlc.high_price, ohlc.low_price, ohlc.close_price, ohlc.volume });
        }
        std.log.info(" OrderBook Bids: {d} Asks: {d}", .{ symbol.orderbook.bids.len, symbol.orderbook.asks.len });
        symbol.orderbook.dump();
    }
}

pub fn getMarketQuote(self: *const SymbolMap, symbol: []const u8) GetPriceError!MarketQuote {
    if (self.get(symbol)) |sym| {
        if (sym.count == 0 or sym.last_price_update_time <= 0) {
            return GetPriceError.NoPriceDataAvailable;
        }
        const latest_idx = (sym.head + 15 - 1) % 15;
        const price = sym.ticker_queue[latest_idx].close_price;
        if (!std.math.isFinite(price) or price <= 0.0) {
            return GetPriceError.NoPriceDataAvailable;
        }
        return .{ .price = price, .exchange_event_ms = sym.last_price_update_time };
    }
    return GetPriceError.SymbolNotFound;
}

pub fn getLastClosePrice(self: *const SymbolMap, symbol: []const u8) GetPriceError!f64 {
    if (self.get(symbol)) |sym| {
        if (sym.count == 0) {
            return GetPriceError.NoPriceDataAvailable;
        }

        const latest_idx = (sym.head + 15 - 1) % 15;
        const price = sym.ticker_queue[latest_idx].close_price;

        if (!std.math.isFinite(price) or price <= 0.0) {
            return GetPriceError.NoPriceDataAvailable;
        }

        return price;
    }

    return GetPriceError.SymbolNotFound;
}
