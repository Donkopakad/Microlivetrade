# Signal Generation Pipeline Modifications

## Summary of Changes

Removed RSI and orderbook metrics from the stat calc pipeline and replaced with **percentage change calculations every 2 seconds from the 15-minute candle open price**.

### Modified Files

#### 1. **types.zig**
- **Removed**: `GPURSIResultBatch` structure
- **Removed**: `GPUOrderBookResultBatch` structure  
- **Removed**: `GPUOrderBookDataBatch` structure
- **Added**: `GPUPercentageChangeResultBatch` struct with:
  - `percentage_change[MAX_SYMBOLS]` - % change from candle open
  - `current_price[MAX_SYMBOLS]` - latest close price
  - `candle_open_price[MAX_SYMBOLS]` - opening price of 15m candle
  - `candle_timestamp[MAX_SYMBOLS]` - timestamp when candle started

- **Updated**: `GPUBatchResult` to only contain `percentage_change` field

#### 2. **stat_calc/lib.zig**
- **Removed**: `calculateRSIBatch()` function
- **Removed**: `calculateOrderBookPercentageBatch()` function
- **Added**: `calculatePercentageChangeBatch()` function
- **Updated**: `calculateSymbolMapBatch()` to call new percentage change function
- **Updated**: GPU memory structures to only allocate for OHLC and percentage change results
- **Updated**: `warmUp()` function to test percentage change kernel

#### 3. **stat_calc/kernel.h**
- **Removed**: `GPURSIResultBatch_C` struct
- **Removed**: `GPUOrderBookDataBatch_C` struct
- **Removed**: `GPUOrderBookResultBatch_C` struct
- **Added**: `GPUPercentageChangeResultBatch_C` struct
- **Updated**: All CUDA wrapper function signatures to use new data structures

#### 4. **stat_calc/kernel.cu**
- **Removed**: `rsi_kernel_batch()` CUDA kernel
- **Removed**: `stoch_rsi_kernel_batch()` CUDA kernel (was commented out)
- **Removed**: `orderbook_kernel_batch()` CUDA kernel
- **Added**: `percentage_change_kernel_batch()` - calculates % change from first to last price
- **Updated**: Memory allocation/deallocation functions
- **Updated**: All CUDA wrapper implementations

#### 5. **signal_engine/lib.zig**
- **Updated**: Imports to use `GPUPercentageChangeResultBatch`
- **Replaced**: `processSignalsParallel()` function
  - **Old**: Used SIMD analysis on RSI + orderbook metrics with parallel task chunks
  - **New**: Simple signal generation based on percentage change:
    - **BUY**: When percentage_change < -1.0% AND no open position
    - **SELL**: When percentage_change > +1.0% AND open position exists
    - **Signal strength**: Scaled from percentage change magnitude

### Signal Generation Logic

**New Buy Signal:**
```
IF (percentage_change < -1.0%) AND (no open position)
  → Generate BUY signal
  → signal_strength = min(1.0, abs(pct_change) / 5.0)
```

**New Sell Signal:**
```
IF (percentage_change > +1.0%) AND (open position exists)
  → Generate SELL signal  
  → signal_strength = min(1.0, pct_change / 5.0)
```

### How It Works

1. **Data Collection**: Last 15 close prices stored in circular buffer per symbol
2. **GPU Calculation**: Percentage change kernel computes:
   - First price in buffer = candle open (or previous close)
   - Last price = current market price
   - % change = ((last - first) / first) * 100
3. **Signal Engine**: Evaluates percentage change thresholds
4. **Trading**: Portfolio manager executes buy/sell based on signals

### Next Steps to Complete

1. Build with CUDA compiler to verify kernel syntax
2. Test with live Binance data
3. Adjust percentage thresholds based on backtest results
4. Consider adding momentum or trend filters

