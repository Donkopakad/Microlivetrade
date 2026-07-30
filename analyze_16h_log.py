#!/usr/bin/env python3
"""
Parse the timestamped Microlivetrade dry-run log and create a detailed Excel report.

Usage:
    python3 analyze_16h_log.py timestamped_16h_dry_run.log 16h_trade_analysis.xlsx

Optional environment variables:
    REPORT_NOTIONAL_USDT=250
    OPEN_FEE_USDT=0.50
    CLOSE_FEE_USDT=0.50
"""

from __future__ import annotations

import json
import math
import os
import re
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

IST = timezone(timedelta(hours=5, minutes=30))
REPORT_NOTIONAL = float(os.getenv("REPORT_NOTIONAL_USDT", "500"))
OPEN_FEE = float(os.getenv("OPEN_FEE_USDT", "0.25"))
CLOSE_FEE = float(os.getenv("CLOSE_FEE_USDT", "0.25"))

TS_RE = re.compile(r"^\[(?P<ts>[^\]]+)\]\s*(?P<msg>.*)$")
THRESHOLD_RE = re.compile(
    r"Threshold crossed (?P<symbol>\S+): pct=(?P<pct>-?\d+(?:\.\d+)?)% "
    r"official_open=(?P<open>\d+(?:\.\d+)?) current=(?P<current>\d+(?:\.\d+)?) "
    r"candle_start_ms=(?P<start>\d+)"
)
ACTIVE_RE = re.compile(
    r"Active price check (?P<symbol>\S+): price=(?P<price>\d+(?:\.\d+)?) "
    r"upper=(?P<upper>\d+(?:\.\d+)?) lower=(?P<lower>\d+(?:\.\d+)?)"
)
OPEN_RE = re.compile(
    r"Opened simulated (?P<side>LONG|SHORT) (?P<symbol>\S+) "
    r"qty=(?P<qty>\d+(?:\.\d+)?) price=\$(?P<price>\d+(?:\.\d+)?)"
)
CLOSE_RE = re.compile(
    r"Closed simulated (?P<side>LONG|SHORT) (?P<symbol>\S+) "
    r"qty=(?P<qty>\d+(?:\.\d+)?) price=\$(?P<price>\d+(?:\.\d+)?) "
    r"PnL=\$(?P<pnl>-?\d+(?:\.\d+)?)"
)
TOGGLE_RE = re.compile(
    r"Toggled simulated (?P<side>LONG|SHORT) (?P<symbol>\S+) "
    r"qty=(?P<qty>\d+(?:\.\d+)?) price=\$(?P<price>\d+(?:\.\d+)?)"
)
NEW_CANDLE_RE = re.compile(
    r"New 15m candle detected; entries paused until \+5 seconds "
    r"\(target_start_ms=(?P<start>\d+)\)"
)
FRESH_RE = re.compile(
    r"Fresh 15m cycle ready: activated (?P<count>\d+) symbols after boundary \+5 seconds"
)
SKIP_RE = re.compile(r"Skipping signal for (?P<symbol>\S+); (?P<reason>.+)")
HEARTBEAT_RE = re.compile(
    r"REST market-data heartbeat: matched_symbols=(?P<matched>\d+) "
    r"candle_ready_symbols=(?P<ready>\d+) last_price_poll_age_ms=(?P<age>\d+) "
    r"healthy=(?P<healthy>true|false) total_failures=(?P<failures>\d+)"
)

@dataclass
class Position:
    trade_id: int
    candle_id: int
    symbol: str
    side: str
    entry_time: datetime
    entry_price: float
    logged_qty_10usdt: float
    candle_start_ms: int
    official_open: float
    threshold_pct_at_initial_signal: Optional[float]
    pivot_price: float
    upper_toggle: Optional[float]
    lower_toggle: Optional[float]
    open_reason: str
    toggle_number: int

def parse_ts(text: str) -> datetime:
    # Expected from runner: YYYY-MM-DD HH:MM:SS IST
    text = text.strip().replace(" IST", "")
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)

def pct(a: float, b: float) -> Optional[float]:
    if not a:
        return None
    return ((b - a) / a) * 100.0

def fetch_candle(symbol: str, start_ms: int) -> dict:
    query = urllib.parse.urlencode({
        "symbol": symbol,
        "interval": "15m",
        "startTime": str(start_ms),
        "limit": "1",
    })
    url = f"https://fapi.binance.com/fapi/v1/klines?{query}"
    try:
        with urllib.request.urlopen(url, timeout=20) as response:
            data = json.load(response)
        if not data:
            return {}
        k = data[0]
        if int(k[0]) != start_ms:
            return {}
        return {
            "official_open": float(k[1]),
            "official_high": float(k[2]),
            "official_low": float(k[3]),
            "official_close": float(k[4]),
            "volume": float(k[5]),
            "close_time_ms": int(k[6]),
            "trades": int(k[8]),
        }
    except Exception as exc:
        return {"fetch_error": str(exc)}

def money_pnl(side: str, entry: float, exit_price: float, notional: float) -> float:
    quantity = notional / entry
    if side == "LONG":
        return (exit_price - entry) * quantity
    return (entry - exit_price) * quantity

def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python3 analyze_16h_log.py <timestamped_log> [output.xlsx]")
        return 2

    log_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2]) if len(sys.argv) >= 3 else Path("16h_trade_analysis.xlsx")
    if not log_path.exists():
        print(f"Log not found: {log_path}")
        return 2

    trades: list[dict] = []
    events: list[dict] = []
    skipped: list[dict] = []
    heartbeats: list[dict] = []
    system_events: list[dict] = []

    latest_threshold: dict[str, dict] = {}
    latest_active: dict[str, dict] = {}
    open_position: Optional[Position] = None
    pending_close: Optional[dict] = None
    trade_id = 0
    candle_id = 0
    current_cycle_start_ms: Optional[int] = None
    current_cycle_activated_at: Optional[datetime] = None
    current_cycle_ready_count: Optional[int] = None
    candle_meta: dict[tuple[str, int], dict] = {}
    toggle_counts: dict[tuple[str, int], int] = {}

    with log_path.open("r", encoding="utf-8", errors="replace") as fh:
        for line_no, raw in enumerate(fh, start=1):
            raw = raw.rstrip("\n")
            m = TS_RE.match(raw)
            if not m:
                continue
            ts = parse_ts(m.group("ts"))
            msg = m.group("msg")

            if nm := NEW_CANDLE_RE.search(msg):
                current_cycle_start_ms = int(nm.group("start"))
                current_cycle_activated_at = None
                current_cycle_ready_count = None
                system_events.append({
                    "time_ist": ts, "event": "NEW_CANDLE_GATE",
                    "details": msg, "line_no": line_no
                })
                continue

            if fm := FRESH_RE.search(msg):
                current_cycle_activated_at = ts
                current_cycle_ready_count = int(fm.group("count"))
                if current_cycle_start_ms is None:
                    # Startup log does not include New candle line. Infer from most recent threshold later.
                    pass
                system_events.append({
                    "time_ist": ts, "event": "FRESH_CYCLE_READY",
                    "details": msg, "line_no": line_no
                })
                continue

            if hm := HEARTBEAT_RE.search(msg):
                heartbeats.append({
                    "time_ist": ts,
                    "matched_symbols": int(hm.group("matched")),
                    "candle_ready_symbols": int(hm.group("ready")),
                    "poll_age_ms": int(hm.group("age")),
                    "healthy": hm.group("healthy") == "true",
                    "total_failures": int(hm.group("failures")),
                    "line_no": line_no,
                })
                continue

            if tm := THRESHOLD_RE.search(msg):
                symbol = tm.group("symbol")
                start_ms = int(tm.group("start"))
                current_cycle_start_ms = start_ms
                latest_threshold[symbol] = {
                    "time": ts,
                    "pct": float(tm.group("pct")),
                    "official_open": float(tm.group("open")),
                    "current": float(tm.group("current")),
                    "start_ms": start_ms,
                    "line_no": line_no,
                }
                candle_meta.setdefault((symbol, start_ms), {
                    "symbol": symbol,
                    "candle_start_ms": start_ms,
                    "official_open_from_log": float(tm.group("open")),
                    "first_threshold_time_ist": ts,
                    "first_threshold_pct": float(tm.group("pct")),
                    "first_threshold_price": float(tm.group("current")),
                })
                events.append({
                    "time_ist": ts, "event_type": "THRESHOLD",
                    "symbol": symbol, "side": "",
                    "price": float(tm.group("current")),
                    "upper_toggle": None, "lower_toggle": None,
                    "reason": f"Price reached {float(tm.group('pct')):.4f}% from official candle open",
                    "candle_start_ms": start_ms, "line_no": line_no
                })
                continue

            if am := ACTIVE_RE.search(msg):
                latest_active[am.group("symbol")] = {
                    "time": ts,
                    "price": float(am.group("price")),
                    "upper": float(am.group("upper")),
                    "lower": float(am.group("lower")),
                    "line_no": line_no,
                }
                continue

            if sm := SKIP_RE.search(msg):
                skipped.append({
                    "time_ist": ts,
                    "symbol": sm.group("symbol"),
                    "reason": sm.group("reason"),
                    "candle_start_ms": current_cycle_start_ms,
                    "line_no": line_no,
                })
                continue

            if om := OPEN_RE.search(msg):
                symbol = om.group("symbol")
                side = om.group("side")
                price = float(om.group("price"))
                qty = float(om.group("qty"))
                th = latest_threshold.get(symbol, {})
                start_ms = int(th.get("start_ms") or current_cycle_start_ms or 0)
                official_open = float(th.get("official_open") or 0.0)
                active = latest_active.get(symbol, {})
                pivot = price
                upper = float(active["upper"]) if active else None
                lower = float(active["lower"]) if active else None
                trade_id += 1
                key = (symbol, start_ms)
                toggle_counts.setdefault(key, 0)
                candle_id += 1
                open_position = Position(
                    trade_id=trade_id,
                    candle_id=candle_id,
                    symbol=symbol,
                    side=side,
                    entry_time=ts,
                    entry_price=price,
                    logged_qty_10usdt=qty,
                    candle_start_ms=start_ms,
                    official_open=official_open,
                    threshold_pct_at_initial_signal=th.get("pct"),
                    pivot_price=pivot,
                    upper_toggle=upper,
                    lower_toggle=lower,
                    open_reason=(
                        f"Initial {side} entry after {th.get('pct')}% threshold signal"
                        if th else f"Initial {side} entry"
                    ),
                    toggle_number=0,
                )
                events.append({
                    "time_ist": ts, "event_type": "OPEN",
                    "symbol": symbol, "side": side, "price": price,
                    "upper_toggle": upper, "lower_toggle": lower,
                    "reason": open_position.open_reason,
                    "candle_start_ms": start_ms, "line_no": line_no
                })
                continue

            if cm := CLOSE_RE.search(msg):
                if open_position is None:
                    system_events.append({
                        "time_ist": ts, "event": "UNMATCHED_CLOSE",
                        "details": msg, "line_no": line_no
                    })
                    continue
                exit_price = float(cm.group("price"))
                pending_close = {
                    "time": ts,
                    "line_no": line_no,
                    "exit_price": exit_price,
                    "logged_pnl_10usdt": float(cm.group("pnl")),
                    "logged_qty_10usdt": float(cm.group("qty")),
                    "position": open_position,
                }
                # Delay finalizing until we see whether the next line is a toggle.
                continue

            if tog := TOGGLE_RE.search(msg):
                symbol = tog.group("symbol")
                new_side = tog.group("side")
                toggle_price = float(tog.group("price"))
                qty = float(tog.group("qty"))
                if pending_close is None:
                    system_events.append({
                        "time_ist": ts, "event": "UNMATCHED_TOGGLE",
                        "details": msg, "line_no": line_no
                    })
                    continue

                pos: Position = pending_close["position"]
                active = latest_active.get(symbol, {})
                if new_side == "LONG":
                    reason = (
                        f"Toggle to LONG because checked price {toggle_price:.10g} "
                        f"was at/above upper level {active.get('upper', pos.upper_toggle)}"
                    )
                else:
                    reason = (
                        f"Toggle to SHORT because checked price {toggle_price:.10g} "
                        f"was at/below lower level {active.get('lower', pos.lower_toggle)}"
                    )
                exit_price = pending_close["exit_price"]
                gross_250 = money_pnl(pos.side, pos.entry_price, exit_price, REPORT_NOTIONAL)
                trades.append({
                    "trade_id": pos.trade_id,
                    "status": "CLOSED",
                    "candle_id": pos.candle_id,
                    "symbol": pos.symbol,
                    "side": pos.side,
                    "entry_time_ist": pos.entry_time,
                    "exit_time_ist": pending_close["time"],
                    "holding_seconds": (pending_close["time"] - pos.entry_time).total_seconds(),
                    "entry_reason": pos.open_reason,
                    "exit_reason": reason,
                    "toggle_number": pos.toggle_number,
                    "entry_price": pos.entry_price,
                    "exit_price": exit_price,
                    "trade_return_pct": pct(pos.entry_price, exit_price) if pos.side == "LONG"
                        else pct(exit_price, pos.entry_price),
                    "official_candle_open": pos.official_open,
                    "entry_pct_from_candle_open": pct(pos.official_open, pos.entry_price),
                    "exit_pct_from_candle_open": pct(pos.official_open, exit_price),
                    "pivot_price": pos.pivot_price,
                    "upper_toggle": pos.upper_toggle,
                    "lower_toggle": pos.lower_toggle,
                    "logged_qty_10usdt": pos.logged_qty_10usdt,
                    "report_notional_usdt": REPORT_NOTIONAL,
                    "report_quantity": REPORT_NOTIONAL / pos.entry_price,
                    "logged_gross_pnl_10usdt": pending_close["logged_pnl_10usdt"],
                    "gross_pnl_usdt": gross_250,
                    "open_fee_usdt": OPEN_FEE,
                    "close_fee_usdt": CLOSE_FEE,
                    "total_fee_usdt": OPEN_FEE + CLOSE_FEE,
                    "net_pnl_usdt": gross_250 - OPEN_FEE - CLOSE_FEE,
                    "candle_start_ms": pos.candle_start_ms,
                    "candle_start_ist": datetime.fromtimestamp(pos.candle_start_ms / 1000, IST),
                    "candle_end_ist": datetime.fromtimestamp((pos.candle_start_ms + 900000) / 1000, IST),
                    "official_candle_close": None,
                    "candle_close_pct_from_open": None,
                    "line_open": None,
                    "line_close": pending_close["line_no"],
                })
                events.append({
                    "time_ist": pending_close["time"], "event_type": "CLOSE_FOR_TOGGLE",
                    "symbol": pos.symbol, "side": pos.side, "price": exit_price,
                    "upper_toggle": pos.upper_toggle, "lower_toggle": pos.lower_toggle,
                    "reason": reason, "candle_start_ms": pos.candle_start_ms,
                    "line_no": pending_close["line_no"]
                })
                events.append({
                    "time_ist": ts, "event_type": "TOGGLE_OPEN",
                    "symbol": symbol, "side": new_side, "price": toggle_price,
                    "upper_toggle": pos.upper_toggle, "lower_toggle": pos.lower_toggle,
                    "reason": reason, "candle_start_ms": pos.candle_start_ms,
                    "line_no": line_no
                })

                key = (symbol, pos.candle_start_ms)
                toggle_counts[key] = toggle_counts.get(key, 0) + 1
                trade_id += 1
                open_position = Position(
                    trade_id=trade_id,
                    candle_id=pos.candle_id,
                    symbol=symbol,
                    side=new_side,
                    entry_time=ts,
                    entry_price=toggle_price,
                    logged_qty_10usdt=qty,
                    candle_start_ms=pos.candle_start_ms,
                    official_open=pos.official_open,
                    threshold_pct_at_initial_signal=pos.threshold_pct_at_initial_signal,
                    pivot_price=pos.pivot_price,
                    upper_toggle=pos.upper_toggle,
                    lower_toggle=pos.lower_toggle,
                    open_reason=reason,
                    toggle_number=toggle_counts[key],
                )
                pending_close = None
                continue

            # Any non-toggle line after a pending close means it was a boundary/final close.
            if pending_close is not None:
                pos = pending_close["position"]
                exit_price = pending_close["exit_price"]
                gross_250 = money_pnl(pos.side, pos.entry_price, exit_price, REPORT_NOTIONAL)
                trades.append({
                    "trade_id": pos.trade_id,
                    "status": "CLOSED",
                    "candle_id": pos.candle_id,
                    "symbol": pos.symbol,
                    "side": pos.side,
                    "entry_time_ist": pos.entry_time,
                    "exit_time_ist": pending_close["time"],
                    "holding_seconds": (pending_close["time"] - pos.entry_time).total_seconds(),
                    "entry_reason": pos.open_reason,
                    "exit_reason": "Candle boundary / strategy candle-end close",
                    "toggle_number": pos.toggle_number,
                    "entry_price": pos.entry_price,
                    "exit_price": exit_price,
                    "trade_return_pct": pct(pos.entry_price, exit_price) if pos.side == "LONG"
                        else pct(exit_price, pos.entry_price),
                    "official_candle_open": pos.official_open,
                    "entry_pct_from_candle_open": pct(pos.official_open, pos.entry_price),
                    "exit_pct_from_candle_open": pct(pos.official_open, exit_price),
                    "pivot_price": pos.pivot_price,
                    "upper_toggle": pos.upper_toggle,
                    "lower_toggle": pos.lower_toggle,
                    "logged_qty_10usdt": pos.logged_qty_10usdt,
                    "report_notional_usdt": REPORT_NOTIONAL,
                    "report_quantity": REPORT_NOTIONAL / pos.entry_price,
                    "logged_gross_pnl_10usdt": pending_close["logged_pnl_10usdt"],
                    "gross_pnl_usdt": gross_250,
                    "open_fee_usdt": OPEN_FEE,
                    "close_fee_usdt": CLOSE_FEE,
                    "total_fee_usdt": OPEN_FEE + CLOSE_FEE,
                    "net_pnl_usdt": gross_250 - OPEN_FEE - CLOSE_FEE,
                    "candle_start_ms": pos.candle_start_ms,
                    "candle_start_ist": datetime.fromtimestamp(pos.candle_start_ms / 1000, IST),
                    "candle_end_ist": datetime.fromtimestamp((pos.candle_start_ms + 900000) / 1000, IST),
                    "official_candle_close": None,
                    "candle_close_pct_from_open": None,
                    "line_open": None,
                    "line_close": pending_close["line_no"],
                })
                events.append({
                    "time_ist": pending_close["time"], "event_type": "CANDLE_END_CLOSE",
                    "symbol": pos.symbol, "side": pos.side, "price": exit_price,
                    "upper_toggle": pos.upper_toggle, "lower_toggle": pos.lower_toggle,
                    "reason": "Candle boundary / strategy candle-end close",
                    "candle_start_ms": pos.candle_start_ms,
                    "line_no": pending_close["line_no"]
                })
                open_position = None
                pending_close = None

    # Finalize a pending close at EOF.
    if pending_close is not None:
        pos = pending_close["position"]
        exit_price = pending_close["exit_price"]
        gross_250 = money_pnl(pos.side, pos.entry_price, exit_price, REPORT_NOTIONAL)
        trades.append({
            "trade_id": pos.trade_id, "status": "CLOSED", "candle_id": pos.candle_id,
            "symbol": pos.symbol, "side": pos.side,
            "entry_time_ist": pos.entry_time, "exit_time_ist": pending_close["time"],
            "holding_seconds": (pending_close["time"] - pos.entry_time).total_seconds(),
            "entry_reason": pos.open_reason, "exit_reason": "Close at end of log",
            "toggle_number": pos.toggle_number, "entry_price": pos.entry_price,
            "exit_price": exit_price,
            "trade_return_pct": pct(pos.entry_price, exit_price) if pos.side == "LONG"
                else pct(exit_price, pos.entry_price),
            "official_candle_open": pos.official_open,
            "entry_pct_from_candle_open": pct(pos.official_open, pos.entry_price),
            "exit_pct_from_candle_open": pct(pos.official_open, exit_price),
            "pivot_price": pos.pivot_price, "upper_toggle": pos.upper_toggle,
            "lower_toggle": pos.lower_toggle,
            "logged_qty_10usdt": pos.logged_qty_10usdt,
            "report_notional_usdt": REPORT_NOTIONAL,
            "report_quantity": REPORT_NOTIONAL / pos.entry_price,
            "logged_gross_pnl_10usdt": pending_close["logged_pnl_10usdt"],
            "gross_pnl_usdt": gross_250, "open_fee_usdt": OPEN_FEE,
            "close_fee_usdt": CLOSE_FEE,
            "total_fee_usdt": OPEN_FEE + CLOSE_FEE,
            "net_pnl_usdt": gross_250 - OPEN_FEE - CLOSE_FEE,
            "candle_start_ms": pos.candle_start_ms,
            "candle_start_ist": datetime.fromtimestamp(pos.candle_start_ms / 1000, IST),
            "candle_end_ist": datetime.fromtimestamp((pos.candle_start_ms + 900000) / 1000, IST),
            "official_candle_close": None, "candle_close_pct_from_open": None,
            "line_open": None, "line_close": pending_close["line_no"],
        })
        open_position = None

    if open_position is not None:
        trades.append({
            "trade_id": open_position.trade_id, "status": "OPEN_AT_LOG_END",
            "candle_id": open_position.candle_id, "symbol": open_position.symbol,
            "side": open_position.side, "entry_time_ist": open_position.entry_time,
            "exit_time_ist": None, "holding_seconds": None,
            "entry_reason": open_position.open_reason,
            "exit_reason": "Not closed before process ended",
            "toggle_number": open_position.toggle_number,
            "entry_price": open_position.entry_price, "exit_price": None,
            "trade_return_pct": None,
            "official_candle_open": open_position.official_open,
            "entry_pct_from_candle_open": pct(open_position.official_open, open_position.entry_price),
            "exit_pct_from_candle_open": None,
            "pivot_price": open_position.pivot_price,
            "upper_toggle": open_position.upper_toggle,
            "lower_toggle": open_position.lower_toggle,
            "logged_qty_10usdt": open_position.logged_qty_10usdt,
            "report_notional_usdt": REPORT_NOTIONAL,
            "report_quantity": REPORT_NOTIONAL / open_position.entry_price,
            "logged_gross_pnl_10usdt": None, "gross_pnl_usdt": None,
            "open_fee_usdt": OPEN_FEE, "close_fee_usdt": 0.0,
            "total_fee_usdt": OPEN_FEE, "net_pnl_usdt": -OPEN_FEE,
            "candle_start_ms": open_position.candle_start_ms,
            "candle_start_ist": datetime.fromtimestamp(open_position.candle_start_ms / 1000, IST),
            "candle_end_ist": datetime.fromtimestamp((open_position.candle_start_ms + 900000) / 1000, IST),
            "official_candle_close": None, "candle_close_pct_from_open": None,
            "line_open": None, "line_close": None,
        })

    # Fetch one official Binance candle per traded symbol/candle.
    unique_keys = sorted({
        (t["symbol"], int(t["candle_start_ms"]))
        for t in trades if int(t.get("candle_start_ms") or 0) > 0
    })
    fetched: dict[tuple[str, int], dict] = {}
    for symbol, start_ms in unique_keys:
        print(f"Fetching official candle: {symbol} {start_ms}")
        fetched[(symbol, start_ms)] = fetch_candle(symbol, start_ms)

    for t in trades:
        key = (t["symbol"], int(t["candle_start_ms"]))
        c = fetched.get(key, {})
        if c.get("official_open"):
            t["official_candle_open"] = c["official_open"]
            t["official_candle_close"] = c["official_close"]
            t["candle_close_pct_from_open"] = pct(c["official_open"], c["official_close"])
            t["entry_pct_from_candle_open"] = pct(c["official_open"], t["entry_price"])
            if t["exit_price"] is not None:
                t["exit_pct_from_candle_open"] = pct(c["official_open"], t["exit_price"])

    trades_df = pd.DataFrame(trades)
    events_df = pd.DataFrame(events)
    skipped_df = pd.DataFrame(skipped)
    heartbeats_df = pd.DataFrame(heartbeats)
    system_df = pd.DataFrame(system_events)

    candle_rows = []
    if not trades_df.empty:
        for (symbol, start_ms), grp in trades_df.groupby(["symbol", "candle_start_ms"], dropna=False):
            closed = grp[grp["status"] == "CLOSED"]
            fetched_c = fetched.get((symbol, int(start_ms)), {})
            candle_rows.append({
                "symbol": symbol,
                "candle_start_ms": start_ms,
                "candle_start_ist": datetime.fromtimestamp(int(start_ms) / 1000, IST),
                "candle_end_ist": datetime.fromtimestamp((int(start_ms) + 900000) / 1000, IST),
                "official_open": fetched_c.get("official_open", grp["official_candle_open"].iloc[0]),
                "official_high": fetched_c.get("official_high"),
                "official_low": fetched_c.get("official_low"),
                "official_close": fetched_c.get("official_close"),
                "candle_close_pct_from_open": (
                    pct(fetched_c["official_open"], fetched_c["official_close"])
                    if fetched_c.get("official_open") else None
                ),
                "first_entry_side": grp.iloc[0]["side"],
                "first_entry_price": grp.iloc[0]["entry_price"],
                "entry_threshold_pct": candle_meta.get((symbol, int(start_ms)), {}).get("first_threshold_pct"),
                "completed_positions": len(closed),
                "toggle_count": max(len(closed) - 1, 0),
                "gross_pnl_usdt": closed["gross_pnl_usdt"].sum() if len(closed) else 0.0,
                "fees_usdt": closed["total_fee_usdt"].sum() if len(closed) else 0.0,
                "net_pnl_usdt": closed["net_pnl_usdt"].sum() if len(closed) else 0.0,
                "winning_positions": int((closed["net_pnl_usdt"] > 0).sum()) if len(closed) else 0,
                "losing_positions": int((closed["net_pnl_usdt"] < 0).sum()) if len(closed) else 0,
                "fetch_error": fetched_c.get("fetch_error"),
            })
    candles_df = pd.DataFrame(candle_rows)

    summary_rows = [
        {"metric": "Report notional per position", "value": REPORT_NOTIONAL, "unit": "USDT"},
        {"metric": "Open fee per position", "value": OPEN_FEE, "unit": "USDT"},
        {"metric": "Close fee per completed position", "value": CLOSE_FEE, "unit": "USDT"},
        {"metric": "Completed positions", "value": int((trades_df["status"] == "CLOSED").sum()) if not trades_df.empty else 0, "unit": "count"},
        {"metric": "Positions open at log end", "value": int((trades_df["status"] != "CLOSED").sum()) if not trades_df.empty else 0, "unit": "count"},
        {"metric": "Gross PnL", "value": trades_df.loc[trades_df["status"] == "CLOSED", "gross_pnl_usdt"].sum() if not trades_df.empty else 0, "unit": "USDT"},
        {"metric": "Total fees", "value": trades_df["total_fee_usdt"].sum() if not trades_df.empty else 0, "unit": "USDT"},
        {"metric": "Net PnL", "value": trades_df["net_pnl_usdt"].sum() if not trades_df.empty else 0, "unit": "USDT"},
        {"metric": "Unique traded candles", "value": len(candles_df), "unit": "count"},
        {"metric": "Total toggles", "value": int((events_df["event_type"] == "TOGGLE_OPEN").sum()) if not events_df.empty else 0, "unit": "count"},
        {"metric": "Skipped signals", "value": len(skipped_df), "unit": "count"},
        {"metric": "Unhealthy heartbeats", "value": int((~heartbeats_df["healthy"]).sum()) if not heartbeats_df.empty else 0, "unit": "count"},
    ]
    summary_df = pd.DataFrame(summary_rows)

    # Excel cannot store timezone-aware datetimes.
    # Preserve the IST clock time while removing the timezone object.
    def make_excel_datetime_safe(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        for column in df.columns:
            series = df[column]

            if isinstance(series.dtype, pd.DatetimeTZDtype):
                df[column] = series.dt.tz_localize(None)

            elif series.dtype == "object":
                df[column] = series.map(
                    lambda value: value.replace(tzinfo=None)
                    if isinstance(value, datetime) and value.tzinfo is not None
                    else value
                )

        return df

    summary_df = make_excel_datetime_safe(summary_df)
    trades_df = make_excel_datetime_safe(trades_df)
    candles_df = make_excel_datetime_safe(candles_df)
    events_df = make_excel_datetime_safe(events_df)
    skipped_df = make_excel_datetime_safe(skipped_df)
    heartbeats_df = make_excel_datetime_safe(heartbeats_df)
    system_df = make_excel_datetime_safe(system_df)

    # Excel output
    with pd.ExcelWriter(out_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd hh:mm:ss") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        trades_df.to_excel(writer, sheet_name="Trades", index=False)
        candles_df.to_excel(writer, sheet_name="Candles", index=False)
        events_df.to_excel(writer, sheet_name="Timeline", index=False)
        skipped_df.to_excel(writer, sheet_name="Skipped Signals", index=False)
        heartbeats_df.to_excel(writer, sheet_name="Health", index=False)
        system_df.to_excel(writer, sheet_name="System Events", index=False)

        wb = writer.book
        header_fmt = wb.add_format({
            "bold": True, "bg_color": "#1F4E78", "font_color": "white",
            "border": 1, "align": "center", "valign": "vcenter"
        })
        money_fmt = wb.add_format({"num_format": "0.0000"})
        pct_fmt = wb.add_format({"num_format": "0.0000%"})
        decimal_fmt = wb.add_format({"num_format": "0.00000000"})
        good_fmt = wb.add_format({"bg_color": "#C6EFCE", "font_color": "#006100"})
        bad_fmt = wb.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006"})
        note_fmt = wb.add_format({"text_wrap": True, "valign": "top"})

        for sheet_name, df in [
            ("Summary", summary_df), ("Trades", trades_df), ("Candles", candles_df),
            ("Timeline", events_df), ("Skipped Signals", skipped_df),
            ("Health", heartbeats_df), ("System Events", system_df)
        ]:
            ws = writer.sheets[sheet_name]
            ws.freeze_panes(1, 0)
            ws.autofilter(0, 0, max(len(df), 1), max(len(df.columns) - 1, 0))
            ws.set_row(0, 24, header_fmt)
            for col_idx, col in enumerate(df.columns):
                width = min(max(len(str(col)) + 2, 12), 38)
                if col in {"entry_reason", "exit_reason", "reason", "details"}:
                    width = 48
                    ws.set_column(col_idx, col_idx, width, note_fmt)
                else:
                    if len(df):
                        sample_len = max((len(str(v)) for v in df[col].head(200).fillna("")), default=0)
                        width = min(max(width, sample_len + 2), 24)
                    ws.set_column(col_idx, col_idx, width)

            if sheet_name == "Trades" and not df.empty:
                for col in ["entry_price", "exit_price", "official_candle_open",
                            "official_candle_close", "pivot_price", "upper_toggle",
                            "lower_toggle", "report_quantity"]:
                    if col in df.columns:
                        idx = df.columns.get_loc(col)
                        ws.set_column(idx, idx, 16, decimal_fmt)
                for col in ["gross_pnl_usdt", "open_fee_usdt", "close_fee_usdt",
                            "total_fee_usdt", "net_pnl_usdt",
                            "logged_gross_pnl_10usdt"]:
                    if col in df.columns:
                        idx = df.columns.get_loc(col)
                        ws.set_column(idx, idx, 14, money_fmt)
                for col in ["trade_return_pct", "entry_pct_from_candle_open",
                            "exit_pct_from_candle_open", "candle_close_pct_from_open"]:
                    if col in df.columns:
                        idx = df.columns.get_loc(col)
                        # Values are percentage points, not fractions.
                        ws.set_column(idx, idx, 16, wb.add_format({"num_format": "0.0000"}))
                net_col = df.columns.get_loc("net_pnl_usdt")
                ws.conditional_format(1, net_col, len(df), net_col, {
                    "type": "cell", "criteria": ">", "value": 0, "format": good_fmt
                })
                ws.conditional_format(1, net_col, len(df), net_col, {
                    "type": "cell", "criteria": "<", "value": 0, "format": bad_fmt
                })

            if sheet_name == "Candles" and not df.empty:
                net_col = df.columns.get_loc("net_pnl_usdt")
                ws.conditional_format(1, net_col, len(df), net_col, {
                    "type": "cell", "criteria": ">", "value": 0, "format": good_fmt
                })
                ws.conditional_format(1, net_col, len(df), net_col, {
                    "type": "cell", "criteria": "<", "value": 0, "format": bad_fmt
                })
                chart = wb.add_chart({"type": "column"})
                chart.add_series({
                    "name": "Net PnL",
                    "categories": ["Candles", 1, df.columns.get_loc("candle_start_ist"),
                                   len(df), df.columns.get_loc("candle_start_ist")],
                    "values": ["Candles", 1, net_col, len(df), net_col],
                })
                chart.set_title({"name": "Net PnL by traded 15-minute candle"})
                chart.set_x_axis({"name": "Candle start (IST)"})
                chart.set_y_axis({"name": "USDT"})
                chart.set_legend({"none": True})
                ws.insert_chart("W2", chart, {"x_scale": 1.4, "y_scale": 1.2})

    print(f"Created Excel report: {out_path.resolve()}")
    print(f"Completed trades: {(trades_df['status'] == 'CLOSED').sum() if not trades_df.empty else 0}")
    print(f"Net PnL: {trades_df['net_pnl_usdt'].sum() if not trades_df.empty else 0:.4f} USDT")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
