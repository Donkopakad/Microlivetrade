#!/usr/bin/env python3
from __future__ import annotations

import sys
from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

for line in sys.stdin:
    timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    sys.stdout.write(f"[{timestamp}] {line}")
    sys.stdout.flush()
