# Quick Reference: 15-Minute Candle Implementation

## What Was Changed?

Three files were modified to properly track 15-minute candle opening prices and calculate accurate percentage changes.

## The Problem

Before: The system couldn't tell when a new 15-minute candle started. It was calculating percentage change from the wrong reference point.

After: The system correctly identifies 15-minute boundaries and calculates percentage change from the candle opening price.

## The Solution

### File 1: types.zig
```
Added to Symbol struct:
  Fields: candle_start_time, candle_open_price, current_price, last_update_time
  Methods: initCandleWithPreviousClose(), updateCurrentPrice(), 
           getPercentageChange(), startNewCandle()
```

### File 2: ticker_handler.zig
```
Enhanced to:
  Extract event timestamp from Binance
  Calculate which 15-minute candle the price belongs to
  Detect when a new candle starts
  Call new Symbol methods to track prices correctly
```

### File 3: signal_engine/lib.zig
```
Simplified to:
  Call symbol.getPercentageChange() directly
  No longer needs to wait for GPU batch calculations
  Same signal generation logic (BUY/SELL thresholds unchanged)
```

## How It Works in 3 Steps

### Step 1: Calculate Candle Boundary (in ticker_handler)
```
When price arrives with timestamp = 15:00:45
  → candle_ms = (1699526445000 / 900000) * 900000
  → candle_ms = 15:00:00 ✓ Correct period identified
```

### Step 2: Detect Transition (in ticker_handler)
```
First time:
  → candle_start_time was 0
  → Initialize with this price as opening

New period:
  → candle_start_time was 15:00:00
  → Current calculated candle is 15:15:00
  → NEW CANDLE DETECTED!
  → Save previous price as opening for new candle
```

### Step 3: Calculate Change (in signal_engine)
```
Symbol.getPercentageChange()
  = ((current_price - candle_open_price) / candle_open_price) * 100
  = ((50500 - 50000) / 50000) * 100
  = +1.0%
```

## Visual Timeline

```
15:00:00 - CANDLE START
│ opening = $50,000
│ current = $50,000
│ % change = 0.0%

15:00:30
│ current = $50,500
│ % change = +1.0% → SELL SIGNAL (if position open)

15:01:00
│ current = $49,500
│ % change = -1.0% → BUY SIGNAL (if no position)

15:15:00 - NEW CANDLE DETECTED!
│ old opening = $50,000
│ new opening = $49,500 (was previous current)
│ current = $49,500
│ % change = 0.0% (resets from new opening)

15:15:30
│ current = $50,000
│ % change = ((50,000 - 49,500) / 49,500) * 100 = +1.01%
```

## Key Constants

```
Candle Duration: 15 minutes
  = 15 × 60 × 1000 milliseconds
  = 900,000 milliseconds

Threshold for BUY:  percentage_change < -1.0%
Threshold for SELL: percentage_change > +1.0%

Signal Strength Calculation:
  strength = min(1.0, |percentage_change| / 5.0)
  0.2 to 1.0 range depending on price movement
```

## Methods You Now Have

```zig
symbol.initCandleWithPreviousClose(price, time)
  → Initialize symbol on first data

symbol.updateCurrentPrice(price, time)
  → Update price on every tick

symbol.getPercentageChange()
  → Get % change from candle open to now

symbol.startNewCandle(time)
  → Handle transition to new 15-minute period
```

## Testing Checklist

- [ ] Run robot for 30+ minutes
- [ ] Verify at least 2-3 candle transitions
- [ ] Check logs for correct candle_start_time changes
- [ ] Verify percentage_change resets when new candle starts
- [ ] Confirm BUY signals when price drops > 1%
- [ ] Confirm SELL signals when price rises > 1%
- [ ] Compare calculations with manual math
- [ ] Check for any timing issues or missed updates

## Files Changed Summary

| File | Lines | Changes |
|------|-------|---------|
| types.zig | 356-416 | +4 fields, +4 methods in Symbol |
| ticker_handler.zig | 56-117 | +30 lines for candle detection |
| signal_engine/lib.zig | 305-358 | -15 lines (simplified), +8 lines (new logic) |
| **TOTAL** | | **+50 net lines** |

## No Breaking Changes

✓ All existing functionality preserved
✓ addTicker() method still works
✓ OHLC storage unchanged
✓ Order book functionality untouched
✓ Signal thresholds unchanged
✓ Trade execution logic unchanged
