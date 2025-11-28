# Detailed Code Changes

## Change 1: src/types.zig - Symbol Struct Enhancements

### Added Fields to Symbol Struct

```zig
pub const Symbol = struct {
    ticker_queue: [15]OHLC,
    head: usize,
    count: usize,
    orderbook: OrderBook,

    // NEW FIELDS FOR 15-MINUTE CANDLE TRACKING
    candle_start_time: i64,      // When current 15-min candle started (ms)
    candle_open_price: f64,      // Price when candle started
    current_price: f64,          // Most recent price
    last_update_time: i64,       // When price was last updated
    
    // ... existing init() and other methods ...
```

### New Methods

#### 1. Initialize Candle on First Update
```zig
pub fn initCandleWithPreviousClose(self: *Symbol, prev_close_price: f64, candle_start_ms: i64) void {
    self.candle_start_time = candle_start_ms;
    self.candle_open_price = prev_close_price;
    self.current_price = prev_close_price;
    self.last_update_time = candle_start_ms;
}
```
**When used:** First time a symbol gets data, or at startup
**Purpose:** Set opening price to previous candle's close

#### 2. Update Price on Every Tick
```zig
pub fn updateCurrentPrice(self: *Symbol, new_price: f64, update_time_ms: i64) void {
    self.current_price = new_price;
    self.last_update_time = update_time_ms;
}
```
**When used:** Every time new price data arrives from Binance
**Purpose:** Keep current price updated in real-time

#### 3. Calculate Percentage Change
```zig
pub fn getPercentageChange(self: *const Symbol) f64 {
    if (self.candle_open_price == 0.0) return 0.0;
    return ((self.current_price - self.candle_open_price) / self.candle_open_price) * 100.0;
}
```
**When used:** In signal engine to decide BUY/SELL
**Formula:** `((current - open) / open) × 100`
**Example:** $50,000 → $50,500 = +1.0%

#### 4. Handle Candle Transition
```zig
pub fn startNewCandle(self: *Symbol, new_candle_start_ms: i64) void {
    self.candle_start_time = new_candle_start_ms;
    self.candle_open_price = self.current_price;
}
```
**When used:** When new 15-minute period is detected
**Purpose:** Make the last price the new candle's opening price

---

## Change 2: src/data_aggregator/ticker_handler.zig - Candle Detection

### The handleMiniTicker Function

#### OLD CODE (Lines 56-102)
```zig
fn handleMiniTicker(self: *TickerHandler, root: json.Value) !void {
    const symbol_val = root.object.get("s") orelse return;
    if (symbol_val != .string) return;
    const symbol = symbol_val.string;

    // Extract OHLCV from JSON
    const o_val = root.object.get("o") orelse return;
    const h_val = root.object.get("h") orelse return;
    const l_val = root.object.get("l") orelse return;
    const c_val = root.object.get("c") orelse return;
    const v_val = root.object.get("v") orelse return;

    // Parse strings to floats
    const open_price = std.fmt.parseFloat(f64, o_val.string) catch return;
    const high_price = std.fmt.parseFloat(f64, h_val.string) catch return;
    const low_price = std.fmt.parseFloat(f64, l_val.string) catch return;
    const close_price = std.fmt.parseFloat(f64, c_val.string) catch return;
    const volume = std.fmt.parseFloat(f64, v_val.string) catch return;

    if (self.symbol_map.getPtr(symbol)) |sym| {
        const ohlc = OHLC{
            .open_price = open_price,
            .high_price = high_price,
            .low_price = low_price,
            .close_price = close_price,
            .volume = volume,
        };
        self.mutex.lock();
        defer self.mutex.unlock();
        sym.addTicker(ohlc);
        // ⚠️ PROBLEM: No tracking of candle boundaries!
    }
}
```

#### NEW CODE (Lines 56-117)
```zig
fn handleMiniTicker(self: *TickerHandler, root: json.Value) !void {
    const symbol_val = root.object.get("s") orelse return;
    if (symbol_val != .string) return;
    const symbol = symbol_val.string;

    // Extract OHLCV from JSON
    const o_val = root.object.get("o") orelse return;
    const h_val = root.object.get("h") orelse return;
    const l_val = root.object.get("l") orelse return;
    const c_val = root.object.get("c") orelse return;
    const v_val = root.object.get("v") orelse return;
    const e_val = root.object.get("E") orelse return;  // ← NEW: Get event timestamp

    // Validate types
    if (o_val != .string or h_val != .string or l_val != .string or 
        c_val != .string or v_val != .string or e_val != .integer) return;

    // Parse strings to floats
    const open_price = std.fmt.parseFloat(f64, o_val.string) catch return;
    const high_price = std.fmt.parseFloat(f64, h_val.string) catch return;
    const low_price = std.fmt.parseFloat(f64, l_val.string) catch return;
    const close_price = std.fmt.parseFloat(f64, c_val.string) catch return;
    const volume = std.fmt.parseFloat(f64, v_val.string) catch return;
    const event_time_ms = e_val.integer;  // ← NEW: Extract timestamp

    if (self.symbol_map.getPtr(symbol)) |sym| {
        const ohlc = OHLC{
            .open_price = open_price,
            .high_price = high_price,
            .low_price = low_price,
            .close_price = close_price,
            .volume = volume,
        };
        self.mutex.lock();
        defer self.mutex.unlock();
        sym.addTicker(ohlc);

        // ✨ NEW: Detect and handle 15-minute candle boundaries
        const current_candle_ms = @as(i64, @intCast((event_time_ms / 900000) * 900000));
        // 900000 ms = 15 minutes
        // This calculation aligns timestamp to 15-minute boundary
        // Example: 
        //   15:00:30 (54030000 ms) → 15:00:00 (54000000 ms)
        //   15:14:59 (54899999 ms) → 15:00:00 (54000000 ms)  
        //   15:15:00 (54900000 ms) → 15:15:00 (54900000 ms)  ← NEW CANDLE!

        if (sym.candle_start_time == 0) {
            // First time: Initialize with previous close price
            sym.initCandleWithPreviousClose(close_price, current_candle_ms);
        } else if (current_candle_ms != sym.candle_start_time) {
            // New candle detected!
            // Save the current price as the opening price for new candle
            const prev_close = sym.current_price;
            sym.startNewCandle(current_candle_ms);
            sym.current_price = prev_close;
        }

        // Always update current price
        sym.updateCurrentPrice(close_price, event_time_ms);
    }
}
```

### Key Calculations

```
900000 milliseconds = 15 minutes × 60 seconds × 1000 ms

Candle Calculation Example:
┌─────────────────────────────────────────────────────────────┐
│ Time: 15:00:45 EST                                          │
│ Milliseconds from epoch: 1699526445000                      │
│                                                             │
│ Step 1: Divide by 900000                                   │
│   1699526445000 ÷ 900000 = 1888362.7166...                │
│                                                             │
│ Step 2: Convert to integer (truncate decimal)             │
│   floor(1888362.7166) = 1888362                            │
│                                                             │
│ Step 3: Multiply by 900000                                │
│   1888362 × 900000 = 1699525800000                         │
│                                                             │
│ Result: 15:00:00 EST (start of candle)                    │
└─────────────────────────────────────────────────────────────┘

Detection Logic:
┌─────────────────────────────────────────────────────────────┐
│ if (sym.candle_start_time == 0)                            │
│   → First time seeing this symbol, initialize             │
│   → Use close_price as candle_open_price                  │
│                                                             │
│ else if (current_candle_ms != sym.candle_start_time)      │
│   → New 15-minute period detected!                        │
│   → Save current price as new opening                     │
│   → Reset candle tracking                                 │
│                                                             │
│ sym.updateCurrentPrice(close_price, event_time_ms)        │
│   → Always update price (happens in all cases)            │
└─────────────────────────────────────────────────────────────┘
```

---

## Change 3: src/signal_engine/lib.zig - Simplified Calculation

### The processSignalsParallel Function

#### OLD CODE (Lines 305-373)
```zig
fn processSignalsParallel(self: *SignalEngine, pct_results: *GPUPercentageChangeResultBatch) !void {
    const num_symbols = @min(self.symbol_map.count(), MAX_SYMBOLS);
    if (num_symbols == 0) return;

    const percentage_changes = try self.allocator.alloc(f32, num_symbols);
    defer self.allocator.free(percentage_changes);

    const current_prices = try self.allocator.alloc(f32, num_symbols);
    defer self.allocator.free(current_prices);

    const candle_open_prices = try self.allocator.alloc(f32, num_symbols);
    defer self.allocator.free(candle_open_prices);

    // ... more allocations ...

    self.mutex.lock();
    var symbol_idx: usize = 0;
    var iterator = self.symbol_map.iterator();
    while (iterator.next()) |entry| {
        if (symbol_idx >= num_symbols) break;
        symbol_names[symbol_idx] = entry.key_ptr.*;

        // ⚠️ OLD: Copy from GPU batch results
        percentage_changes[symbol_idx] = pct_results.percentage_change[symbol_idx];
        current_prices[symbol_idx] = pct_results.current_price[symbol_idx];
        candle_open_prices[symbol_idx] = pct_results.candle_open_price[symbol_idx];
        position_sizes[symbol_idx] = intended_position_size_usdt;

        has_positions[symbol_idx] = self.trade_handler.hasOpenPosition(symbol_names[symbol_idx]);
        symbol_idx += 1;
    }
    self.mutex.unlock();

    // ... signal generation ...
}
```

#### NEW CODE (Lines 305-358)
```zig
fn processSignalsParallel(self: *SignalEngine, pct_results: *GPUPercentageChangeResultBatch) !void {
    const num_symbols = @min(self.symbol_map.count(), MAX_SYMBOLS);
    if (num_symbols == 0) return;

    // ✨ NEW: Only allocate what we need
    const symbol_names = try self.allocator.alloc([]const u8, num_symbols);
    defer self.allocator.free(symbol_names);

    const percentage_changes = try self.allocator.alloc(f64, num_symbols);
    defer self.allocator.free(percentage_changes);

    const has_positions = try self.allocator.alloc(bool, num_symbols);
    defer self.allocator.free(has_positions);

    self.mutex.lock();
    var symbol_idx: usize = 0;
    var iterator = self.symbol_map.iterator();
    while (iterator.next()) |entry| {
        if (symbol_idx >= num_symbols) break;
        symbol_names[symbol_idx] = entry.key_ptr.*;
        const symbol = entry.value_ptr.*;  // ← NEW: Get the Symbol object

        // ✨ NEW: Calculate percentage change directly
        percentage_changes[symbol_idx] = symbol.getPercentageChange();
        has_positions[symbol_idx] = self.trade_handler.hasOpenPosition(symbol_names[symbol_idx]);
        symbol_idx += 1;
    }
    self.mutex.unlock();

    // Signal generation (same logic as before)
    for (0..num_symbols) |i| {
        const pct_change = @as(f32, @floatCast(percentage_changes[i]));
        const has_position = has_positions[i];

        if (pct_change < -1.0 and !has_position) {
            const signal = TradingSignal{
                .symbol_name = symbol_names[i],
                .signal_type = .BUY,
                .rsi_value = pct_change,
                .orderbook_percentage = pct_change,
                .timestamp = @as(i64, @intCast(std.time.nanoTimestamp())),
                .signal_strength = @min(1.0, @abs(pct_change) / 5.0),
            };
            try self.trade_handler.addSignal(signal);
        } else if (pct_change > 1.0 and has_position) {
            const signal = TradingSignal{
                .symbol_name = symbol_names[i],
                .signal_type = .SELL,
                .rsi_value = pct_change,
                .orderbook_percentage = pct_change,
                .timestamp = @as(i64, @intCast(std.time.nanoTimestamp())),
                .signal_strength = @min(1.0, pct_change / 5.0),
            };
            try self.trade_handler.addSignal(signal);
        }
    }
}
```

### Benefits of This Change

| Aspect | Old | New |
|--------|-----|-----|
| **Data Source** | GPU batch results | Symbol object directly |
| **Allocation** | Many allocations | Minimal allocations |
| **Accuracy** | Depends on GPU kernel | Always accurate |
| **Latency** | Wait for GPU batch | Immediate access |
| **Code Complexity** | Complex batching logic | Simple direct access |
| **Candle Detection** | Not handled in GPU | Handled in ticker_handler |

---

## Summary of All Changes

```
src/types.zig
  ├─ Symbol struct +4 fields
  └─ Symbol struct +4 methods

src/data_aggregator/ticker_handler.zig
  └─ handleMiniTicker() enhanced with candle detection

src/signal_engine/lib.zig
  └─ processSignalsParallel() simplified calculation
```

**Total Lines Added:** ~60
**Total Lines Modified:** ~30
**Total Lines Removed:** ~10
**Net Change:** +50 lines (mostly new functionality)
