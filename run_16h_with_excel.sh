#!/usr/bin/env bash
set -euo pipefail

cd "$HOME/Microlivetrade"

LOG_FILE="timestamped_16h_dry_run.log"
RAW_LOG_FILE="raw_16h_dry_run.log"
EXCEL_FILE="16h_trade_analysis.xlsx"
RUN_SECONDS=58600   # 16 full hours plus enough time to reach the first 15m boundary

echo "Starting extended DRY-RUN."
echo "Full timestamped log: $PWD/$LOG_FILE"
echo "Raw log:              $PWD/$RAW_LOG_FILE"
echo "Excel after run:       $PWD/$EXCEL_FILE"
echo

# Timestamp every console line in IST and preserve both raw and timestamped logs.
set +e
env -u BINANCE_FUTURES_API_KEY \
    -u BINANCE_FUTURES_API_SECRET \
    LIVE_TRADING=false \
    MARKET_DATA_POLL_MS=1000 \
    timeout --signal=INT --kill-after=30s "${RUN_SECONDS}s" \
    stdbuf -oL -eL /opt/zig-0.14.1/zig build run \
    2>&1 \
    | tee "$RAW_LOG_FILE" \
    | TZ=Asia/Kolkata awk '{
          cmd="date +\"%Y-%m-%d %H:%M:%S IST\"";
          cmd | getline ts;
          close(cmd);
          print "[" ts "] " $0;
          fflush();
      }' \
    | tee "$LOG_FILE"
BOT_STATUS=${PIPESTATUS[0]}
set -e

echo
echo "Bot finished with status: $BOT_STATUS"
echo "Building Excel report..."

source "$HOME/avax-analysis/venv/bin/activate" 2>/dev/null || \
source "$HOME/avax-hedge-bot/venv/bin/activate" 2>/dev/null || true

python3 "$HOME/Microlivetrade/analyze_16h_log.py" \
    "$HOME/Microlivetrade/$LOG_FILE" \
    "$HOME/Microlivetrade/$EXCEL_FILE"

echo
echo "Finished."
echo "Log:   $HOME/Microlivetrade/$LOG_FILE"
echo "Excel: $HOME/Microlivetrade/$EXCEL_FILE"
