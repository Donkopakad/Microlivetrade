#!/usr/bin/env python3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PM = ROOT / "src/trade_handler/portfolio_manager.zig"
TH = ROOT / "src/trade_handler/lib.zig"


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if new in text:
        print(f"[skip] {label} already applied")
        return text
    if old not in text:
        raise SystemExit(f"[error] could not find patch target: {label}")
    print(f"[apply] {label}")
    return text.replace(old, new, 1)


pm = PM.read_text()

pm = replace_once(
    pm,
    'const binance = @import("binance_futures_client.zig");\n',
    'const binance = @import("binance_futures_client.zig");\nconst toggle_protection = @import("toggle_protection.zig");\n',
    "import toggle protection",
)

pm = replace_once(
    pm,
    '    order_id: ?i64,\n};\n',
    '    order_id: ?i64,\n    last_observed_price: f64,\n    last_observed_ms: i64,\n};\n',
    "position observation fields",
)

old_block = '''        if (self.getOpenPositionSymbol()) |sym_name| {
            if (self.positions.getPtr(sym_name)) |pos| {
                if (pos.is_open and pos.pivot_entry_price > 0.0 and now_ns < pos.candle_end_timestamp) {
                    const current_price = symbol_map.getLastClosePrice(self.symbol_map, sym_name) catch {
                        return;
                    };

                    const upper = pos.pivot_entry_price * 1.002;
                    const lower = pos.pivot_entry_price * 0.998;
                    std.log.info("Active price check {s}: price={d:.8} upper={d:.8} lower={d:.8}", .{ sym_name, current_price, upper, lower });
                    if (current_price >= upper and pos.side != .long) {
                        self.flipPosition(pos, .long, current_price);
                    } else if (current_price <= lower and pos.side != .short) {
                        self.flipPosition(pos, .short, current_price);
                    }
                }
            }
        }
'''

new_block = '''        if (self.getOpenPositionSymbol()) |sym_name| {
            if (self.positions.getPtr(sym_name)) |pos| {
                if (pos.is_open and pos.pivot_entry_price > 0.0 and now_ns < pos.candle_end_timestamp) {
                    const current_price = symbol_map.getLastClosePrice(self.symbol_map, sym_name) catch {
                        return;
                    };
                    const now_ms: i64 = std.time.milliTimestamp();

                    if (!std.math.isFinite(pos.last_observed_price) or pos.last_observed_price <= 0.0) {
                        pos.last_observed_price = current_price;
                        pos.last_observed_ms = now_ms;
                        return;
                    }

                    const protection_side: toggle_protection.PositionSide = switch (pos.side) {
                        .long => .long,
                        .short => .short,
                        .none => return,
                    };

                    const decision = toggle_protection.decide(
                        protection_side,
                        pos.pivot_entry_price,
                        .{
                            .previous_price = pos.last_observed_price,
                            .current_price = current_price,
                            // Until the active-symbol WebSocket lands, the bulk-feed read time
                            // is used as the event time. Gap and true-crossing protection are
                            // active; upstream stale-feed detection requires exchange timestamps.
                            .exchange_event_ms = now_ms,
                            .local_receive_ms = now_ms,
                        },
                        .{},
                    );

                    std.log.info(
                        "[TOGGLE_DECISION] symbol={s} side={s} previous={d:.8} current={d:.8} trigger={d:.8} crossed={} action={s} jump_pct={d:.4} slippage_pct={d:.4} data_age_ms={d}",
                        .{
                            sym_name,
                            if (pos.side == .long) "LONG" else "SHORT",
                            pos.last_observed_price,
                            current_price,
                            decision.trigger_price,
                            decision.crossed,
                            @tagName(decision.action),
                            decision.jump_fraction * 100.0,
                            decision.trigger_slippage_fraction * 100.0,
                            decision.data_age_ms,
                        },
                    );

                    switch (decision.action) {
                        .none, .pause_stale => {
                            pos.last_observed_price = current_price;
                            pos.last_observed_ms = now_ms;
                        },
                        .close_only_gap => {
                            std.log.warn(
                                "[GAP_EXIT] {s}: closing {s} only; no immediate reversal. trigger={d:.8} observed={d:.8} jump_pct={d:.4} slippage_pct={d:.4}",
                                .{
                                    sym_name,
                                    if (pos.side == .long) "LONG" else "SHORT",
                                    decision.trigger_price,
                                    current_price,
                                    decision.jump_fraction * 100.0,
                                    decision.trigger_slippage_fraction * 100.0,
                                },
                            );
                            _ = switch (pos.side) {
                                .long => self.closeLong(pos, current_price),
                                .short => self.closeShort(pos, current_price),
                                .none => false,
                            };
                        },
                        .reverse_to_long => self.flipPosition(pos, .long, current_price),
                        .reverse_to_short => self.flipPosition(pos, .short, current_price),
                    }
                }
            }
        }
'''
pm = replace_once(pm, old_block, new_block, "wire protected toggle decisions")

pm = replace_once(
    pm,
    '''                .leverage = 1.0,
                .order_id = null,
            }) catch unreachable;
''',
    '''                .leverage = 1.0,
                .order_id = null,
                .last_observed_price = 0.0,
                .last_observed_ms = 0,
            }) catch unreachable;
''',
    "initialize observation fields",
)

pm = replace_once(
    pm,
    '''        pos.leverage = @floatCast(signal.leverage); // kept as-is; just for record
        pos.order_id = order_id;
''',
    '''        pos.leverage = @floatCast(signal.leverage); // kept as-is; just for record
        pos.order_id = order_id;
        pos.last_observed_price = entry_price;
        pos.last_observed_ms = std.time.milliTimestamp();
''',
    "seed observation state at each fill",
)

PM.write_text(pm)

th = TH.read_text()
th = replace_once(
    th,
    'const EXIT_INTERVAL_NS: u64 = 500_000_000; // 500ms\n',
    'const EXIT_INTERVAL_NS: u64 = 50_000_000; // 50ms decision loop; feed freshness still depends on market-data source\n',
    "50ms protected decision loop",
)
TH.write_text(th)

print("\nPatch applied successfully.")
print("Run:")
print("  /opt/zig-0.14.1/zig test src/trade_handler/toggle_protection.zig")
print("  /opt/zig-0.14.1/zig build")
print("  /opt/zig-0.14.1/zig build test")
