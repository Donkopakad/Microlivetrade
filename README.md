**MicroRush** is a high-performance, real-time high-frequency trading (HFT) engine written in [Zig](https://ziglang.org/). It is designed for ultra-low-latency signal processing and execution using cutting-edge system-level features.

## Features

- **Ultra-Fast Signal Engine**  
  SIMD-accelerated signal generation using AVX2 across multiple CPU cores and thread pools.

- **GPU-Accelerated Analytics**  
  Real-time RSI, StochRSI, and order book metrics computed using CUDA kernels.

- **Lock-Free Queues**  
  Inter-thread communication built with lock-free data structures to avoid contention.

- **Multithreaded Execution Engine**  
  Uses Zig’s thread pools and affinity pinning for high throughput and core-level balancing.

- **Atomic Portfolio Management**  
  All trade operations are lock-protected and atomic for safe concurrent access.

- **Live Exchange Integration**  
  Fetches real-time market data from **Binance** using WebSocket streams and REST APIs.

- **Balanced Load Metrics**  
  `metrics.zig` implements core-optimized, lock-free load reporting for adaptive tuning and performance analysis.

- **Built-in Benchmarking Tools**
  Track system performance using built-in metrics collectors and CLI flags.

## Binance futures percentage strategy

- The data aggregator continuously dumps the 15-minute opening price, latest close, and computed percentage change for every subscribed symbol into `percent_changes_15m.csv`. Each row represents the "closing percentage" referenced by the trading rules.
- The signal engine now tails that CSV feed directly. Every time a new row shows a symbol rallying **+5% or more**, the engine emits a `BUY` signal that opens/maintains a **1× leverage long** on Binance futures. When the CSV reports a symbol selling off **−5% or worse**, the engine emits a `SELL` signal that either opens or maintains a **1× leverage short**.
- The portfolio manager tracks whether a symbol is long, short, or flat so opposite signals automatically close the current exposure before flipping direction.
- Once a position opens, it is forcibly closed before the 15-minute candle finishes. The portfolio manager enforces a 15-minute hold limit so every trade is flattened within the same timeframe that generated it.
- For a step-by-step explanation—with concrete CSV examples—see [`docs/trade_lifecycle.md`](docs/trade_lifecycle.md). That document walks through how `SignalEngine.generateSignalsFromCsv()` decides when to emit `TradingSignal`s, how `TradeHandler` prioritises them, and how `PortfolioManager` opens, sizes, and closes every position inside the current 15-minute window.

### Quick example

```
timestamp_ms,symbol,open_price,last_price,percent_change
1729462955000,BTCUSDT,68000.00,71400.00,5.00
```

1. `handleCsvLine()` parses the row and sees `percent_change = +5.00`.
2. `evaluateCsvSignal()` creates a 1× leverage `.BUY` signal unless BTCUSDT is already long.
3. `PortfolioManager.processSignal()` opens the futures long at the latest Binance price.
4. If no opposite signal arrives, `checkStopLossConditions()` will auto-close the trade at the 15-minute mark so the position never leaks into the next candle.


---

## Key Components

- **`core/`** – Core HFT runtime loop, lock-free logic, signal dispatch.
- **`signal_engine.zig`** – SIMD-powered, batched signal calculations.
- **`statcalc/`** – GPU-based technical indicator computation.
- **`trade_handler.zig`** – Portfolio and execution manager, thread-safe.
- **`metrics.zig`** – Real-time metrics collection, lock-free load tracker.

---

## Requirements

- Zig (latest [master build](https://ziglang.org/download/))
- CUDA Toolkit nvcc (>=release 12.8, V12.8.93)
- AVX2-capable CPU (modern Intel or AMD)
- Linux (tested on Gentoo Amd64)

---

## Building

Use the provided Makefile: