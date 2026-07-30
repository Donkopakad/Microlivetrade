# Closed-candle 5% reversal strategy

Dry-run strategy rules:

1. Evaluate only a fully closed Binance USD-M 15-minute candle.
2. Candle change = `(close - open) / open`.
3. If the closed candle is +5.00% or more, enter SHORT at the beginning of the next 15-minute candle.
4. If the closed candle is -5.00% or less, enter LONG at the beginning of the next 15-minute candle.
5. The closed candle's close is the fixed midpoint/pivot for the whole trade candle.
6. Toggle levels are fixed at pivot +/-0.20%:
   - upper = previous close * 1.002
   - lower = previous close * 0.998
7. While LONG, a true crossing down through the lower level reverses to SHORT.
8. While SHORT, a true crossing up through the upper level reverses to LONG.
9. Large-gap protection remains active: close the wrong-side position but do not immediately reverse if jump/slippage exceeds configured limits.
10. Close any remaining position when the next 15-minute candle ends.
11. One symbol/position at a time; 500 USDT notional; 1x isolated; dry-run only until audited.

Important timing rule: the signal must be generated once per unique closed candle. A candle that is still forming must never create an entry.
