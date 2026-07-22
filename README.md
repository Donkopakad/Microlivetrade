# MicroRush Zig Binance USDT-M Futures Bot

This branch implements the requested single-global-trade 15-minute momentum/toggle strategy in the Zig entrypoint `src/main.zig`.

## Strategy

- Universe: active Binance USDT-M futures symbols where `quoteAsset=USDT`, `contractType=PERPETUAL`, and `status=TRADING`.
- Candle: fixed Binance 15-minute wall-clock candles (`@kline_15m`) with official kline open `k.o`; miniTicker provides current prices.
- Signal: every batch, percentage is `((current_price - official_15m_open) / official_15m_open) * 100`.
- Entry: +5% opens long; -5% opens short immediately.
- Global trade rule: the signal handler and portfolio manager allow only one open position globally; deterministic batch ordering is timestamp then symbol.
- Position: 1000 USDT notional, 5x isolated leverage. Live orders are disabled unless explicitly enabled.
- Toggle: first actual fill (or dry-run fill) is fixed reference. Upper=`entry*1.001`; lower=`entry*0.999`. Price at/above upper must be long; price at/below lower must be short.
- Exit: the active position is closed at the original 15-minute candle end.

## Streams

- Official candle open: Binance Futures WebSocket `<symbol>@kline_15m`.
- Current price for universe scans: Binance Futures WebSocket `<symbol>@miniTicker`.
- Active trade monitoring: the same miniTicker-updated symbol map is checked approximately once per second, with REST fallback available through the futures client for live order pricing.

## Safety

Dry-run is the default. Do not put secrets in source control. To enable live trading, set credentials and:

```bash
export LIVE_TRADING=true
export LIVE_TRADING_CONFIRM=I_UNDERSTAND_THIS_IS_LIVE
```

## Build

```bash
zig build
zig build test
zig build run
```
