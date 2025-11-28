const std = @import("std");

pub const MAX_ORDERBOOK_SIZE = 5;

pub const SignalType = enum { BUY, SELL, HOLD };

pub const TradingSignal = struct {
    symbol_name: []const u8,
    signal_type: SignalType,
    rsi_value: f32,
    orderbook_percentage: f32,
    timestamp: i128,
    signal_strength: f32,
    leverage: f32,
};

pub const Position = struct {
    symbol_name: []const u8,
    is_open: bool,
    entry_rsi: f32,
    timestamp: i128,
    signal_strength: f32,
};

pub const MAX_SYMBOLS_CUDA = 500;
pub const MAX_SYMBOLS = MAX_SYMBOLS_CUDA;
pub const MAX_RSI_VALUES_PER_SYMBOL = 15;

pub const DeviceInfo = extern struct {
    name: [256]u8,
    major: c_int,
    minor: c_int,
    totalGlobalMem: usize,
};

pub const GPUOHLCDataBatch = extern struct {
    close_prices: [MAX_SYMBOLS][15]f32,
    counts: [MAX_SYMBOLS]u32,
};

pub const GPUPercentageChangeDeviceBatch = extern struct {
    percentage_change: [MAX_SYMBOLS]f32,
    current_price: [MAX_SYMBOLS]f32,
    candle_open_price: [MAX_SYMBOLS]f32,
    candle_timestamp: [MAX_SYMBOLS]i64,
};

pub const GPUPercentageChangeResultBatch = struct {
    device: GPUPercentageChangeDeviceBatch,
    symbols: [MAX_SYMBOLS][]const u8,
    count: usize,
};

pub const GPUOrderBookDataBatch = extern struct {
    bid_prices: [MAX_SYMBOLS][MAX_ORDERBOOK_SIZE]f32,
    bid_quantities: [MAX_SYMBOLS][MAX_ORDERBOOK_SIZE]f32,
    ask_prices: [MAX_SYMBOLS][MAX_ORDERBOOK_SIZE]f32,
    ask_quantities: [MAX_SYMBOLS][MAX_ORDERBOOK_SIZE]f32,
    bid_counts: [MAX_SYMBOLS]u32,
    ask_counts: [MAX_SYMBOLS]u32,
};

pub const GPUOrderBookResultBatch = extern struct {
    bid_percentage: [MAX_SYMBOLS]f32,
    ask_percentage: [MAX_SYMBOLS]f32,
    total_bid_volume: [MAX_SYMBOLS]f32,
    total_ask_volume: [MAX_SYMBOLS]f32,
    spread_percentage: [MAX_SYMBOLS]f32,
    best_bid_price: [MAX_SYMBOLS]f32,
    best_ask_price: [MAX_SYMBOLS]f32,
    best_bid_qty: [MAX_SYMBOLS]f32,
    best_ask_qty: [MAX_SYMBOLS]f32,
};

pub const GPUBatchResult = struct {
    percentage_change: GPUPercentageChangeResultBatch,
};

pub const OHLC = struct {
    open_price: f64,
    high_price: f64,
    low_price: f64,
    close_price: f64,
    volume: f64,
};

pub const PriceLevel = struct {
    price: f64,
    quantity: f64,
};

pub const OrderBook = struct {
    bids: [MAX_ORDERBOOK_SIZE]PriceLevel,
    asks: [MAX_ORDERBOOK_SIZE]PriceLevel,
    bid_head: usize,
    ask_head: usize,
    bid_count: usize,
    ask_count: usize,
    last_update_id: i64,

    pub fn init() OrderBook {
        return OrderBook{
            .bids = [_]PriceLevel{PriceLevel{ .price = 0.0, .quantity = 0.0 }} ** MAX_ORDERBOOK_SIZE,
            .asks = [_]PriceLevel{PriceLevel{ .price = 0.0, .quantity = 0.0 }} ** MAX_ORDERBOOK_SIZE,
            .bid_head = 0,
            .ask_head = 0,
            .bid_count = 0,
            .ask_count = 0,
            .last_update_id = 0,
        };
    }

    pub fn updateLevel(self: *OrderBook, price: f64, quantity: f64, is_bid: bool) void {
        if (is_bid) {
            self.updateBidLevel(price, quantity);
        } else {
            self.updateAskLevel(price, quantity);
        }
    }

    fn updateBidLevel(self: *OrderBook, price: f64, quantity: f64) void {
        var existing_idx: ?usize = null;
        var i: usize = 0;
        while (i < self.bid_count) : (i += 1) {
            const actual_idx = (self.bid_head + i) % MAX_ORDERBOOK_SIZE;
            if (self.bids[actual_idx].price == price) {
                existing_idx = actual_idx;
                break;
            }
        }

        if (quantity == 0.0) {
            if (existing_idx) |idx| {
                self.removeBidLevel(idx);
            }
        } else {
            if (existing_idx) |idx| {
                self.bids[idx].quantity = quantity;
            } else {
                self.addBidLevel(price, quantity);
            }
        }
    }

    fn updateAskLevel(self: *OrderBook, price: f64, quantity: f64) void {
        var existing_idx: ?usize = null;
        var i: usize = 0;
        while (i < self.ask_count) : (i += 1) {
            const actual_idx = (self.ask_head + i) % MAX_ORDERBOOK_SIZE;
            if (self.asks[actual_idx].price == price) {
                existing_idx = actual_idx;
                break;
            }
        }

        if (quantity == 0.0) {
            if (existing_idx) |idx| {
                self.removeAskLevel(idx);
            }
        } else {
            if (existing_idx) |idx| {
                self.asks[idx].quantity = quantity;
            } else {
                self.addAskLevel(price, quantity);
            }
        }
    }

    fn addBidLevel(self: *OrderBook, price: f64, quantity: f64) void {
        if (self.bid_count == MAX_ORDERBOOK_SIZE) {
            const worst_idx = (self.bid_head + MAX_ORDERBOOK_SIZE - 1) % MAX_ORDERBOOK_SIZE;
            if (price <= self.bids[worst_idx].price) {
                return;
            }
            self.bid_count -= 1;
        }

        var insert_pos: usize = 0;
        var i: usize = 0;
        while (i < self.bid_count) : (i += 1) {
            const actual_idx = (self.bid_head + i) % MAX_ORDERBOOK_SIZE;
            if (self.bids[actual_idx].price < price) {
                insert_pos = i;
                break;
            }
            insert_pos = i + 1;
        }

        if (insert_pos < self.bid_count) {
            var shift_i = self.bid_count;
            while (shift_i > insert_pos) : (shift_i -= 1) {
                const from_idx = (self.bid_head + shift_i - 1) % MAX_ORDERBOOK_SIZE;
                const to_idx = (self.bid_head + shift_i) % MAX_ORDERBOOK_SIZE;
                self.bids[to_idx] = self.bids[from_idx];
            }
        }

        const new_idx = (self.bid_head + insert_pos) % MAX_ORDERBOOK_SIZE;
        self.bids[new_idx] = PriceLevel{ .price = price, .quantity = quantity };
        self.bid_count += 1;
    }

    fn addAskLevel(self: *OrderBook, price: f64, quantity: f64) void {
        if (self.ask_count == MAX_ORDERBOOK_SIZE) {
            const worst_idx = (self.ask_head + MAX_ORDERBOOK_SIZE - 1) % MAX_ORDERBOOK_SIZE;
            if (price >= self.asks[worst_idx].price) {
                return;
            }
            self.ask_count -= 1;
        }

        var insert_pos: usize = 0;
        var i: usize = 0;
        while (i < self.ask_count) : (i += 1) {
            const actual_idx = (self.ask_head + i) % MAX_ORDERBOOK_SIZE;
            if (self.asks[actual_idx].price > price) {
                insert_pos = i;
                break;
            }
            insert_pos = i + 1;
        }

        if (insert_pos < self.ask_count) {
            var shift_i = self.ask_count;
            while (shift_i > insert_pos) : (shift_i -= 1) {
                const from_idx = (self.ask_head + shift_i - 1) % MAX_ORDERBOOK_SIZE;
                const to_idx = (self.ask_head + shift_i) % MAX_ORDERBOOK_SIZE;
                self.asks[to_idx] = self.asks[from_idx];
            }
        }

        const new_idx = (self.ask_head + insert_pos) % MAX_ORDERBOOK_SIZE;
        self.asks[new_idx] = PriceLevel{ .price = price, .quantity = quantity };
        self.ask_count += 1;
    }

    fn removeBidLevel(self: *OrderBook, target_idx: usize) void {
        var pos: usize = 0;
        var i: usize = 0;
        while (i < self.bid_count) : (i += 1) {
            const actual_idx = (self.bid_head + i) % MAX_ORDERBOOK_SIZE;
            if (actual_idx == target_idx) {
                pos = i;
                break;
            }
        }

        if (pos < self.bid_count - 1) {
            var shift_i = pos;
            while (shift_i < self.bid_count - 1) : (shift_i += 1) {
                const from_idx = (self.bid_head + shift_i + 1) % MAX_ORDERBOOK_SIZE;
                const to_idx = (self.bid_head + shift_i) % MAX_ORDERBOOK_SIZE;
                self.bids[to_idx] = self.bids[from_idx];
            }
        }

        self.bid_count -= 1;
    }

    fn removeAskLevel(self: *OrderBook, target_idx: usize) void {
        var pos: usize = 0;
        var i: usize = 0;
        while (i < self.ask_count) : (i += 1) {
            const actual_idx = (self.ask_head + i) % MAX_ORDERBOOK_SIZE;
            if (actual_idx == target_idx) {
                pos = i;
                break;
            }
        }

        if (pos < self.ask_count - 1) {
            var shift_i = pos;
            while (shift_i < self.ask_count - 1) : (shift_i += 1) {
                const from_idx = (self.ask_head + shift_i + 1) % MAX_ORDERBOOK_SIZE;
                const to_idx = (self.ask_head + shift_i) % MAX_ORDERBOOK_SIZE;
                self.asks[to_idx] = self.asks[from_idx];
            }
        }

        self.ask_count -= 1;
    }

    pub fn getBestBid(self: *const OrderBook) ?PriceLevel {
        if (self.bid_count == 0) return null;
        return self.bids[self.bid_head];
    }

    pub fn getBestAsk(self: *const OrderBook) ?PriceLevel {
        if (self.ask_count == 0) return null;
        return self.asks[self.ask_head];
    }

    pub fn hasSignificantSpread(self: *const OrderBook, threshold_percent: f64) bool {
        const best_bid = self.getBestBid() orelse return false;
        const best_ask = self.getBestAsk() orelse return false;
        if (best_bid.price == 0.0 or best_ask.price == 0.0) return false;
        const spread = ((best_ask.price - best_bid.price) / best_bid.price) * 100.0;
        return spread >= threshold_percent;
    }

    pub fn dump(self: *const OrderBook) void {
        std.log.info("Best Bid: {d:.4} Best Ask: {d:.4}", .{
            self.getBestBid().?.price,
            self.getBestAsk().?.price,
        });
    }

    pub fn validate(self: *const OrderBook) bool {
        if (self.bid_count > 0 and self.ask_count > 0) {
            const best_bid = self.getBestBid() orelse return false;
            const best_ask = self.getBestAsk() orelse return false;
            if (best_bid.price > best_ask.price) {
                std.log.err("Bid/Ask inversion detected: Bid {} > Ask {}", .{ best_bid.price, best_ask.price });
                return false;
            }
        }

        var i: usize = 1;
        while (i < self.bid_count) : (i += 1) {
            const prev_idx = (self.bid_head + i - 1) % MAX_ORDERBOOK_SIZE;
            const curr_idx = (self.bid_head + i) % MAX_ORDERBOOK_SIZE;
            if (self.bids[prev_idx].price < self.bids[curr_idx].price) {
                std.log.err("Bid order violation at index {}: {} < {}", .{ i, self.bids[prev_idx].price, self.bids[curr_idx].price });
                return false;
            }
        }

        i = 1;
        while (i < self.ask_count) : (i += 1) {
            const prev_idx = (self.ask_head + i - 1) % MAX_ORDERBOOK_SIZE;
            const curr_idx = (self.ask_head + i) % MAX_ORDERBOOK_SIZE;
            if (self.asks[prev_idx].price > self.asks[curr_idx].price) {
                std.log.err("Ask order violation at index {}: {} > {}", .{ i, self.asks[prev_idx].price, self.asks[curr_idx].price });
                return false;
            }
        }

        return true;
    }
};

pub const Symbol = struct {
    ticker_queue: [15]OHLC,
    head: usize,
    count: usize,
    orderbook: OrderBook,

    candle_start_time: i64,
    candle_open_price: f64,
    current_price: f64,
    last_update_time: i64,

    pub fn init() Symbol {
        return Symbol{
            .ticker_queue = [_]OHLC{OHLC{ .open_price = 0.0, .high_price = 0.0, .low_price = 0.0, .close_price = 0.0, .volume = 0.0 }} ** 15,
            .head = 0,
            .count = 0,
            .orderbook = OrderBook.init(),
            .candle_start_time = 0,
            .candle_open_price = 0.0,
            .current_price = 0.0,
            .last_update_time = 0,
        };
    }

    pub fn addTicker(self: *Symbol, ohlc: OHLC) void {
        self.ticker_queue[self.head] = ohlc;
        self.head = (self.head + 1) % 15;
        if (self.count < 15) {
            self.count += 1;
        }
    }

    pub fn getTicker(self: *const Symbol, idx: usize) ?OHLC {
        if (idx >= self.count) return null;
        const offset = @as(i64, @intCast(self.head)) - @as(i64, @intCast(idx)) - 1;
        const real_idx = @as(usize, @intCast((offset % @as(i64, 15) + @as(i64, 15)) % @as(i64, 15)));
        return self.ticker_queue[real_idx];
    }

    pub fn initCandleWithPreviousClose(self: *Symbol, prev_close_price: f64, candle_start_ms: i64) void {
        self.candle_start_time = candle_start_ms;
        self.candle_open_price = prev_close_price;
        self.current_price = prev_close_price;
        self.last_update_time = candle_start_ms;
    }

    pub fn updateCurrentPrice(self: *Symbol, new_price: f64, update_time_ms: i64) void {
        self.current_price = new_price;
        self.last_update_time = update_time_ms;
    }

    pub fn getPercentageChange(self: *const Symbol) f64 {
        if (self.candle_open_price == 0.0) return 0.0;
        return ((self.current_price - self.candle_open_price) / self.candle_open_price) * 100.0;
    }

    pub fn startNewCandle(self: *Symbol, new_candle_start_ms: i64) void {
        self.candle_start_time = new_candle_start_ms;
        self.candle_open_price = self.current_price;
    }
};
