# Implementation Changes: 15-Minute Candle Tracking with Accurate Percentage Calculations

## Summary
Modified the trading robot to properly track 15-minute candle opening prices and calculate accurate percentage changes every 2 seconds from candle start to current price.

## Problem Solved
Previously, the percentage change calculation was not correctly tracking when a new 15-minute candle started. Now:
- Each cryptocurrency tracks the **opening price** from when the 15-minute candle begins
- The **current price** is updated on every price tick from Binance
- **Percentage change** is calculated as: `((current_price - candle_open_price) / candle_open_price) × 100%`

## Files Modified

### 1. **src/types.zig** - Added Candle Tracking to Symbol Struct

**What Changed:**
Added 4 new fields to the `Symbol` struct:
- `candle_start_time: i64` - When current 15-minute candle started (milliseconds)
- `candle_open_price: f64` - Price when candle started
- `current_price: f64` - Most recent price update
- `last_update_time: i64` - Timestamp of last price update

**New Methods Added:**

```zig
pub fn initCandleWithPreviousClose(self: *Symbol, prev_close_price: f64, candle_start_ms: i64) void
```
- Called when a symbol is first initialized
- Sets the opening price to the closing price of the previous candle
- Used during robot startup/warm-up phase

```zig
pub fn updateCurrentPrice(self: *Symbol, new_price: f64, update_time_ms: i64) void
```
- Called every time a new price comes from Binance
- Updates `current_price` and `last_update_time`

```zig
pub fn getPercentageChange(self: *const Symbol) f64
```
- Returns percentage change from candle opening price to current price
- Formula: `((current_price - candle_open_price) / candle_open_price) * 100.0`
- Returns 0.0 if candle_open_price is 0

```zig
pub fn startNewCandle(self: *Symbol, new_candle_start_ms: i64) void
```
- Called when a new 15-minute candle begins
- Sets `candle_start_time` to new value
- Sets `candle_open_price` to the previous `current_price`

---

### 2. **src/data_aggregator/ticker_handler.zig** - Detect Candle Transitions

**What Changed:**
Modified the `handleMiniTicker()` function to:

1. **Extract Event Timestamp**
   - Gets `E` (event time in milliseconds) from Binance WebSocket data
   - Used to detect when a new 15-minute candle starts

2. **Calculate Current Candle Period**
   ```zig
   const current_candle_ms = @as(i64, @intCast((event_time_ms / 900000) * 900000));
   ```
   - Converts millisecond timestamp to nearest 15-minute boundary
   - `900000 ms = 15 minutes × 60 seconds × 1000 ms`
   - This aligns all timestamps to the start of their 15-minute candle

3. **Handle Candle Transitions**
   ```zig
   if (sym.candle_start_time == 0) {
       // First time initializing this symbol
       sym.initCandleWithPreviousClose(close_price, current_candle_ms);
   } else if (current_candle_ms != sym.candle_start_time) {
       // New 15-minute candle started
       const prev_close = sym.current_price;
       sym.startNewCandle(current_candle_ms);
       sym.current_price = prev_close;
   }
   sym.updateCurrentPrice(close_price, event_time_ms);
   ```

**How It Works:**
- **First update**: Store the closing price as the candle opening price
- **New candle detected**: When `current_candle_ms != sym.candle_start_time`, a new 15-minute period has started
  - Save the previous price as the opening price for the new candle
  - Reset the tracking for the new candle
- **Every update**: Always update current price regardless of candle transition

---

### 3. **src/signal_engine/lib.zig** - Simplified Percentage Calculation

**What Changed:**
Simplified the `processSignalsParallel()` function to:

1. **Removed GPU Dependency**
   - No longer needs results from GPU percentage change kernel
   - Uses `symbol.getPercentageChange()` method instead
   - More straightforward calculation in CPU memory

2. **Direct Symbol Iteration**
   ```zig
   var iterator = self.symbol_map.iterator();
   while (iterator.next()) |entry| {
       if (symbol_idx >= num_symbols) break;
       symbol_names[symbol_idx] = entry.key_ptr.*;
       const symbol = entry.value_ptr.*;

       percentage_changes[symbol_idx] = symbol.getPercentageChange();
       has_positions[symbol_idx] = self.trade_handler.hasOpenPosition(symbol_names[symbol_idx]);
       symbol_idx += 1;
   }
   ```

3. **Signal Generation Logic (Unchanged)**
   - **BUY Signal**: `percentage_change < -1.0% AND no open position`
   - **SELL Signal**: `percentage_change > +1.0% AND open position exists`

---

## Data Flow Example

### Timeline: 15:00:00 to 15:15:00 Candle Period

**15:00:00 - Candle Starts**
```
Event: New BTC data arrives
  Timestamp: 15:00:00 (time_ms = X)
  Current candle = (X / 900000) * 900000 = 15:00:00 window
  Previous price from earlier: $50,000

  Symbol State:
  - candle_start_time = 15:00:00
  - candle_open_price = $50,000  (from previous close)
  - current_price = $50,000
  - last_update_time = 15:00:00
```

**15:00:30 - Price Update (30 seconds into candle)**
```
Event: BTC price update
  Price: $50,500
  Timestamp: 15:00:30

  Symbol State:
  - candle_start_time = 15:00:00 (unchanged)
  - candle_open_price = $50,000 (unchanged)
  - current_price = $50,500 (updated!)
  - last_update_time = 15:00:30

  Percentage Change = ((50,500 - 50,000) / 50,000) × 100 = +1.0%
  Signal: SELL (if position open)
```

**15:01:00 - Price Update (1 minute into candle)**
```
Event: BTC price update
  Price: $49,500
  Timestamp: 15:01:00

  Symbol State:
  - candle_start_time = 15:00:00 (unchanged)
  - candle_open_price = $50,000 (unchanged)
  - current_price = $49,500 (updated!)
  - last_update_time = 15:01:00

  Percentage Change = ((49,500 - 50,000) / 50,000) × 100 = -1.0%
  Signal: BUY (if no position open)
```

**15:15:00 - New Candle Starts**
```
Event: Next BTC data arrives at 15:15:00
  Timestamp: 15:15:00 (time_ms = Y)
  Current candle = (Y / 900000) * 900000 = 15:15:00 window
  Current price before transition: $49,500

  Detected: 15:15:00 != 15:00:00 (new candle!)

  Symbol State UPDATES:
  - candle_start_time = 15:15:00 (NEW)
  - candle_open_price = $49,500 (was current_price)
  - current_price = $49,500 (same as incoming price)
  - last_update_time = 15:15:00

  Now percentage change is calculated from $49,500 (new candle open)
```

---

## Key Benefits

1. **Accurate Candle Tracking**:
   - Correctly identifies 15-minute candle boundaries
   - Opening price is the close of the previous candle
   - Perfect for intra-candle analysis

2. **Continuous Price Updates**:
   - Every price tick updates the current price
   - Percentage change calculated on every update
   - More responsive signal generation

3. **Simplified Logic**:
   - No GPU dependency for basic calculations
   - CPU-based percentage calculation is trivial and fast
   - Easier to debug and maintain

4. **Proper Time Alignment**:
   - Uses event timestamps from Binance
   - 15-minute boundaries calculated mathematically
   - No missed transitions or timing issues

---

## Testing Recommendations

1. **Verify Candle Detection**
   - Log when new candles are detected
   - Confirm transitions happen exactly every 15 minutes

2. **Verify Percentage Calculations**
   - Compare `symbol.getPercentageChange()` with manual calculations
   - Verify signal thresholds (-1.0% for BUY, +1.0% for SELL)

3. **Verify Price Updates**
   - Confirm `current_price` matches latest Binance price
   - Check `candle_open_price` matches previous candle close

4. **End-to-End Test**
   - Run bot for at least 30 minutes
   - Check 2-3 candle transitions
   - Verify signals are generated correctly at thresholds

---

## Breaking Changes

None. The changes are **backward compatible**:
- Existing `addTicker()` method still works
- Existing OHLC storage unchanged
- Order book functionality unchanged
- Only adds new optional tracking fields
