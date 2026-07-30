# Millisecond conditional toggle protection

Status: implementation branch for dry-run and testnet validation. Live trading remains disabled until the complete order lifecycle is audited.

## Required market-data architecture

- Keep the existing all-symbol bulk scan for signal discovery.
- After a position opens, subscribe to the active symbol's Binance Futures `bookTicker` or aggregate-trade WebSocket.
- Every active update must carry exchange event time and local receive time in milliseconds.
- Client-side decisions are blocked when data age exceeds 1,000 ms.

## Toggle levels

The original filled entry remains the fixed pivot for the candle.

- LONG active: lower trigger = pivot × 0.998. Arm a SELL close condition.
- SHORT active: upper trigger = pivot × 1.002. Arm a BUY close condition.

A threshold is considered crossed only when the previous and current observations straddle the trigger. A current price already beyond a trigger without a prior-side observation is not treated as a fresh crossing.

## Gap rule

The protective close must not be blocked. The opposite position must not be opened automatically when either:

- single-update price jump exceeds 0.50%; or
- first observed price is more than 0.30% beyond the intended trigger.

Result: close current position, record `GAP_EXIT`, remain flat, and wait for stabilization or a new valid crossing.

## Exchange-side conditional lifecycle

1. Initial market entry becomes FILLED.
2. Read the actual average fill and quantity.
3. Compute and tick-round the fixed trigger.
4. Submit a reduce-only/close-position conditional MARKET order using CONTRACT_PRICE.
5. Persist client order ID, exchange order ID, side, quantity, trigger, and candle end.
6. Consume Binance user-data order updates.
7. Only after the protective close reports FILLED, inspect the account position.
8. Apply the gap rule using the trigger and actual fill.
9. When reversal is permitted, open the opposite position.
10. After the opposite entry is FILLED, arm the opposite protective condition.
11. At the candle boundary, cancel every outstanding conditional order and close any remaining position.

A conditional order may trigger on a brief touch. The stop price is only the activation level; a market fill can be worse during a gap.

## Restart and duplicate protection

On startup:

- query open positions;
- query open conditional orders;
- reconcile persisted client order IDs;
- never submit a second protective order when an equivalent active order exists;
- never reverse from an ACK alone—require FILLED and verify the resulting position quantity.

## Required logs

- exchange_event_ms
- local_receive_ms
- data_age_ms
- previous_price
- current_price
- best_bid
- best_ask
- spread_percent
- fixed_pivot
- raw_trigger
- rounded_trigger
- crossing_time_ms
- conditional_submit_ms
- exchange_ack_ms
- fill_time_ms
- actual_fill_price
- jump_percent
- trigger_slippage_percent
- action: NORMAL_TOGGLE, GAP_EXIT, STALE_PAUSE, BOUNDARY_CLOSE
- close order ID and reverse-entry order ID

## Validation sequence

1. Unit-test pure toggle decisions.
2. Dry-run with active-symbol WebSocket while no orders are sent.
3. Compare WebSocket crossings against the old one-second snapshots.
4. Binance testnet conditional-order lifecycle.
5. Restart recovery and duplicate-order tests.
6. Small controlled live canary only after audit approval.
