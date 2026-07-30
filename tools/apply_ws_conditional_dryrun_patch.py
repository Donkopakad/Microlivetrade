#!/usr/bin/env python3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def replace(path, old, new, label):
    p = ROOT / path
    text = p.read_text()
    if new in text:
        print(f"[skip] {label}")
        return
    if old not in text:
        raise SystemExit(f"Missing expected text for: {label} in {path}")
    p.write_text(text.replace(old, new, 1))
    print(f"[apply] {label}")

# 1) Expose price plus Binance event timestamp.
replace(
    "src/symbol-map.zig",
    "pub const SymbolMap = std.StringHashMap(Symbol);\n",
    "pub const SymbolMap = std.StringHashMap(Symbol);\n\npub const MarketQuote = struct {\n    price: f64,\n    exchange_event_ms: i64,\n};\n",
    "market quote type",
)
replace(
    "src/symbol-map.zig",
    "pub fn getLastClosePrice(self: *const SymbolMap, symbol: []const u8) GetPriceError!f64 {\n",
    "pub fn getMarketQuote(self: *const SymbolMap, symbol: []const u8) GetPriceError!MarketQuote {\n    if (self.get(symbol)) |sym| {\n        if (sym.count == 0 or sym.last_price_update_time <= 0) {\n            return GetPriceError.NoPriceDataAvailable;\n        }\n        const latest_idx = (sym.head + 15 - 1) % 15;\n        const price = sym.ticker_queue[latest_idx].close_price;\n        if (!std.math.isFinite(price) or price <= 0.0) {\n            return GetPriceError.NoPriceDataAvailable;\n        }\n        return .{ .price = price, .exchange_event_ms = sym.last_price_update_time };\n    }\n    return GetPriceError.SymbolNotFound;\n}\n\npub fn getLastClosePrice(self: *const SymbolMap, symbol: []const u8) GetPriceError!f64 {\n",
    "market quote getter",
)

# 2) Keep WS miniTicker focused on event prices. REST owns official candle state.
replace(
    "src/data_aggregator/ticker_handler.zig",
    "            const candle_ms = @divFloor(event_time_ms, 900000) * 900000;\n            if (sym.candle_start_time == 0) {\n                // Temporary non-trading placeholder until the kline stream supplies the official open.\n                sym.candle_start_time = candle_ms;\n            } else if (candle_ms != sym.candle_start_time) {\n                sym.startNewCandle(candle_ms);\n            }\n            sym.updateCurrentPrice(close_price, event_time_ms);\n",
    "            // REST remains authoritative for 15m candle boundaries and official opens.\n            // miniTicker supplies event-driven prices and Binance event timestamps only.\n            sym.updateCurrentPrice(close_price, event_time_ms);\n",
    "separate WS prices from REST candle state",
)

# 3) Subscribe only to miniTicker streams; REST already supplies klines.
replace(
    "src/data_aggregator/binance_ws.zig",
    "            const kline = try std.fmt.allocPrint(self.allocator, \"{s}@kline_15m\", .{sym_lower});\n            try self.kline_streams.append(kline);\n",
    "            // Klines are intentionally not subscribed here. REST is authoritative\n            // for official 15m candle opens and fresh-candle readiness.\n",
    "miniTicker-only WS subscription",
)
replace(
    "src/data_aggregator/binance_ws.zig",
    "        var ticker_and_kline = std.ArrayList([]const u8).init(self.allocator);\n        defer ticker_and_kline.deinit();\n        try ticker_and_kline.appendSlice(self.ticker_streams.items);\n        try ticker_and_kline.appendSlice(self.kline_streams.items);\n        const tmsg = .{\n            .method = \"SUBSCRIBE\",\n            .params = ticker_and_kline.items,\n",
    "        const tmsg = .{\n            .method = \"SUBSCRIBE\",\n            .params = self.ticker_streams.items,\n",
    "subscribe miniTicker streams only",
)

# 4) Run WS prices alongside the existing REST scanner.
replace(
    "src/data_aggregator/lib.zig",
    "const rest_market_data = @import(\"rest_market_data.zig\");\n",
    "const rest_market_data = @import(\"rest_market_data.zig\");\nconst binance_ws = @import(\"binance_ws.zig\");\n",
    "import WS client",
)
replace(
    "src/data_aggregator/lib.zig",
    "    rest_market_data: ?*rest_market_data.RestMarketData,\n",
    "    rest_market_data: ?*rest_market_data.RestMarketData,\n    ws_client: ?*binance_ws.WSClient,\n",
    "WS client field",
)
replace(
    "src/data_aggregator/lib.zig",
    "            .rest_market_data = null,\n",
    "            .rest_market_data = null,\n            .ws_client = null,\n",
    "initialize WS client",
)
replace(
    "src/data_aggregator/lib.zig",
    "    pub fn deinit(self: *DataAggregator) void {\n        if (self.rest_market_data) |rest| {\n",
    "    pub fn deinit(self: *DataAggregator) void {\n        if (self.ws_client) |ws| {\n            ws.stopListener() catch |err| std.log.warn(\"Failed to stop Binance WS listener: {}\", .{err});\n            ws.deinit();\n            self.allocator.destroy(ws);\n            self.ws_client = null;\n        }\n        if (self.rest_market_data) |rest| {\n",
    "deinitialize WS client",
)
replace(
    "src/data_aggregator/lib.zig",
    "        try rest.start();\n        self.rest_market_data = rest;\n",
    "        try rest.start();\n        self.rest_market_data = rest;\n\n        const ws = try self.allocator.create(binance_ws.WSClient);\n        ws.* = try binance_ws.WSClient.init(self.allocator, if (self.enable_metrics) &self.metrics_collector.? else null);\n        ws.startListener(self.symbol_map) catch |err| {\n            std.log.warn(\"Active Binance miniTicker WebSocket unavailable; continuing with REST only: {}\", .{err});\n            self.allocator.destroy(ws);\n            return;\n        };\n        self.ws_client = ws;\n        std.log.info(\"Active Binance miniTicker WebSocket started for event-driven toggle simulation\", .{});\n",
    "start WS alongside REST",
)

# 5) Process each exchange event only once and log the conditional lifecycle.
replace(
    "src/trade_handler/portfolio_manager.zig",
    "    last_observed_ms: i64,\n",
    "    last_observed_ms: i64,\n    last_processed_event_ms: i64,\n    stale_logged: bool,\n",
    "conditional observation fields",
)
replace(
    "src/trade_handler/portfolio_manager.zig",
    "                    const current_price = symbol_map.getLastClosePrice(self.symbol_map, sym_name) catch {\n                        return;\n                    };\n                    const now_ms: i64 = std.time.milliTimestamp();\n",
    "                    const quote = symbol_map.getMarketQuote(self.symbol_map, sym_name) catch {\n                        return;\n                    };\n                    const current_price = quote.price;\n                    const now_ms: i64 = std.time.milliTimestamp();\n                    if (quote.exchange_event_ms <= pos.last_processed_event_ms) return;\n                    pos.last_processed_event_ms = quote.exchange_event_ms;\n",
    "consume each exchange event once",
)
replace(
    "                            // Until the active-symbol WebSocket lands, the bulk-feed read time\n                            // is used as the event time. Gap and true-crossing protection are\n                            // active; upstream stale-feed detection requires exchange timestamps.\n                            .exchange_event_ms = now_ms,\n",
    "                            .exchange_event_ms = quote.exchange_event_ms,\n",
    "use Binance event timestamp",
)
replace(
    "                        .none, .pause_stale => {\n                            pos.last_observed_price = current_price;\n                            pos.last_observed_ms = now_ms;\n                        },\n",
    "                        .none => {\n                            pos.stale_logged = false;\n                            pos.last_observed_price = current_price;\n                            pos.last_observed_ms = now_ms;\n                        },\n                        .pause_stale => {\n                            if (!pos.stale_logged) {\n                                std.log.warn(\"[WS_STALE] symbol={s} data_age_ms={d}; new reversals paused while protective close remains armed\", .{ sym_name, decision.data_age_ms });\n                                pos.stale_logged = true;\n                            }\n                            pos.last_observed_price = current_price;\n                            pos.last_observed_ms = now_ms;\n                        },\n",
    "stale-feed protection logging",
)
replace(
    "                        .close_only_gap => {\n                            std.log.warn(\n",
    "                        .close_only_gap => {\n                            std.log.warn(\"[COND_TRIGGERED] symbol={s} action=close_only_gap trigger={d:.8} observed={d:.8}\", .{ sym_name, decision.trigger_price, current_price });\n                            std.log.warn(\n",
    "conditional gap trigger log",
)
replace(
    "                            _ = switch (pos.side) {\n                                .long => self.closeLong(pos, current_price),\n                                .short => self.closeShort(pos, current_price),\n                                .none => false,\n                            };\n",
    "                            const old_side = pos.side;\n                            const closed = switch (old_side) {\n                                .long => self.closeLong(pos, current_price),\n                                .short => self.closeShort(pos, current_price),\n                                .none => false,\n                            };\n                            if (closed) {\n                                std.log.info(\"[COND_CLOSE_FILLED] symbol={s} side={s} fill={d:.8}\", .{ sym_name, @tagName(old_side), current_price });\n                                std.log.info(\"[POSITION_RECONCILED_FLAT] symbol={s} reason=gap_exit\", .{sym_name});\n                            }\n",
    "gap close fill and reconciliation",
)
replace(
    "                        .reverse_to_long => self.flipPosition(pos, .long, current_price),\n                        .reverse_to_short => self.flipPosition(pos, .short, current_price),\n",
    "                        .reverse_to_long => self.executeConditionalReverse(pos, .long, current_price, decision.trigger_price),\n                        .reverse_to_short => self.executeConditionalReverse(pos, .short, current_price, decision.trigger_price),\n",
    "conditional reverse state machine",
)
replace(
    "    fn flipPosition(self: *PortfolioManager, pos: *PortfolioPosition, desired_side: PositionSide, current_price: f64) void {\n",
    "    fn executeConditionalReverse(self: *PortfolioManager, pos: *PortfolioPosition, desired_side: PositionSide, current_price: f64, trigger_price: f64) void {\n        const symbol_name = pos.symbol;\n        const old_side = pos.side;\n        std.log.info(\"[COND_TRIGGERED] symbol={s} close_side={s} reverse_to={s} trigger={d:.8} observed={d:.8}\", .{ symbol_name, @tagName(old_side), @tagName(desired_side), trigger_price, current_price });\n        self.flipPosition(pos, desired_side, current_price);\n        if (pos.is_open and pos.side == desired_side) {\n            std.log.info(\"[COND_CLOSE_FILLED] symbol={s} closed_side={s} fill={d:.8}\", .{ symbol_name, @tagName(old_side), current_price });\n            std.log.info(\"[POSITION_RECONCILED_FLAT] symbol={s} before_reverse=true\", .{symbol_name});\n            std.log.info(\"[COND_REVERSE_FILLED] symbol={s} new_side={s} fill={d:.8}\", .{ symbol_name, @tagName(desired_side), current_price });\n            const next_trigger = toggle_protection.triggerFor(if (desired_side == .long) .long else .short, pos.pivot_entry_price, .{});\n            std.log.info(\"[COND_REARMED] symbol={s} side={s} next_trigger={d:.8} pivot={d:.8}\", .{ symbol_name, @tagName(desired_side), next_trigger, pos.pivot_entry_price });\n        }\n    }\n\n    fn flipPosition(self: *PortfolioManager, pos: *PortfolioPosition, desired_side: PositionSide, current_price: f64) void {\n",
    "conditional reverse helper",
)
replace(
    "                .last_observed_ms = 0,\n",
    "                .last_observed_ms = 0,\n                .last_processed_event_ms = 0,\n                .stale_logged = false,\n",
    "initialize conditional fields",
)
replace(
    "        pos.last_observed_ms = std.time.milliTimestamp();\n",
    "        pos.last_observed_ms = std.time.milliTimestamp();\n        pos.last_processed_event_ms = 0;\n        pos.stale_logged = false;\n        const armed_trigger = toggle_protection.triggerFor(if (side == .long) .long else .short, pos.pivot_entry_price, .{});\n        std.log.info(\"[COND_ARMED] symbol={s} side={s} trigger={d:.8} pivot={d:.8} source=binance_miniTicker dry_run=true\", .{ pos.symbol, @tagName(side), armed_trigger, pos.pivot_entry_price });\n",
    "arm simulated conditional after fill",
)

print("\nPatch applied successfully. Run tests and build before any long dry run.")
