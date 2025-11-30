const std = @import("std");
const http = std.http;

pub const OrderSide = enum { buy, sell };
pub const PositionSide = enum { long, short };

pub const OrderResult = struct {
    order_id: i64,
    client_order_id: []const u8,
    executed_qty: f64,
    avg_price: f64,
    cum_quote: f64,
    status: []const u8,
};

pub const SymbolInfo = struct {
    step_size: f64,
    min_qty: f64,
    min_notional: f64,
    quantity_precision: u8,
};

pub const BinanceFuturesClient = struct {
    allocator: std.mem.Allocator,
    http_client: http.Client,
    api_key: []const u8,
    api_secret: []const u8,
    owns_api_key: bool,
    owns_api_secret: bool,
    recv_window: u64,
    enabled: bool,
    dry_run_reason: ?[]const u8,
    symbol_info: std.StringHashMap(SymbolInfo),

    pub const base_url: []const u8 = "https://fapi.binance.com";

    pub fn initFromEnv(allocator: std.mem.Allocator) !BinanceFuturesClient {
        const api_key_opt = std.process.getEnvVarOwned(allocator, "BINANCE_FUTURES_API_KEY") catch null;
        const api_secret_opt = std.process.getEnvVarOwned(allocator, "BINANCE_FUTURES_API_SECRET") catch null;
        const recv_window_str = std.process.getEnvVarOwned(allocator, "BINANCE_FUTURES_RECV_WINDOW") catch null;

        const recv_window: u64 = if (recv_window_str) |val| blk: {
            defer allocator.free(val);
            break :blk std.fmt.parseInt(u64, val, 10) catch 5000;
        } else 5000;

        var client = BinanceFuturesClient{
            .allocator = allocator,
            .http_client = http.Client{ .allocator = allocator },
            .api_key = api_key_opt orelse &[_]u8{},
            .api_secret = api_secret_opt orelse &[_]u8{},
            .owns_api_key = api_key_opt != null,
            .owns_api_secret = api_secret_opt != null,
            .recv_window = recv_window,
            .enabled = api_key_opt != null and api_secret_opt != null,
            .dry_run_reason = null,
            .symbol_info = std.StringHashMap(SymbolInfo).init(allocator),
        };

        if (!client.enabled) {
            client.dry_run_reason = try allocator.dupe(u8, "Missing BINANCE_FUTURES_API_KEY or BINANCE_FUTURES_API_SECRET");
            std.log.err("Live Binance futures trading disabled: {s}", .{client.dry_run_reason.?});
        }

        return client;
    }

    pub fn deinit(self: *BinanceFuturesClient) void {
        self.http_client.deinit();
        if (self.owns_api_key) self.allocator.free(self.api_key);
        if (self.owns_api_secret) self.allocator.free(self.api_secret);
        if (self.dry_run_reason) |msg| self.allocator.free(msg);
        self.symbol_info.deinit();
    }

    pub fn isLive(self: *const BinanceFuturesClient) bool {
        return self.enabled;
    }

    pub fn fetchUsdtBalance(self: *BinanceFuturesClient) !?f64 {
        if (!self.enabled) return null;
        const path = "/fapi/v2/account";
        const body = try self.signedRequest(.GET, path, "");
        defer self.allocator.free(body);

        var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, body, .{});
        defer parsed.deinit();
        const root = parsed.value;
        const assets = root.object.get("assets") orelse return null;
        if (assets != .array) return null;
        for (assets.array.items) |asset| {
            if (asset != .object) continue;
            const asset_obj = asset.object;
            const asset_name = asset_obj.get("asset") orelse continue;
            if (asset_name != .string) continue;
            if (!std.mem.eql(u8, asset_name.string, "USDT")) continue;
            const wallet_balance_val = asset_obj.get("walletBalance") orelse continue;
            if (wallet_balance_val != .string) continue;
            return std.fmt.parseFloat(f64, wallet_balance_val.string) catch null;
        }
        return null;
    }

    pub fn setMarginType(self: *BinanceFuturesClient, symbol: []const u8, margin_type: []const u8) !void {
        if (!self.enabled) return;
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();
        try query_buf.writer().print("symbol={s}&marginType={s}", .{ symbol, margin_type });
        const body = self.signedRequest(.POST, "/fapi/v1/marginType", query_buf.items) catch |err| {
            std.log.err("Failed to set margin type for {s}: {}", .{ symbol, err });
            return err;
        };
        defer self.allocator.free(body);
    }

    pub fn setLeverage(self: *BinanceFuturesClient, symbol: []const u8, leverage: u8) !void {
        if (!self.enabled) return;
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();
        try query_buf.writer().print("symbol={s}&leverage={d}", .{ symbol, leverage });
        const body = self.signedRequest(.POST, "/fapi/v1/leverage", query_buf.items) catch |err| {
            std.log.err("Failed to set leverage for {s}: {}", .{ symbol, err });
            return err;
        };
        defer self.allocator.free(body);
    }

    pub fn openLong(self: *BinanceFuturesClient, symbol: []const u8, usdt_notional: f64, leverage: f64) !OrderResult {
        return self.placeOrderWithNotional(symbol, usdt_notional, leverage, .buy, .long, false);
    }

    pub fn openShort(self: *BinanceFuturesClient, symbol: []const u8, usdt_notional: f64, leverage: f64) !OrderResult {
        return self.placeOrderWithNotional(symbol, usdt_notional, leverage, .sell, .short, false);
    }

    pub fn closeLong(self: *BinanceFuturesClient, symbol: []const u8, quantity: f64) !OrderResult {
        return self.placeOrderWithQuantity(symbol, quantity, .sell, .long, true);
    }

    pub fn closeShort(self: *BinanceFuturesClient, symbol: []const u8, quantity: f64) !OrderResult {
        return self.placeOrderWithQuantity(symbol, quantity, .buy, .short, true);
    }

    fn placeOrderWithNotional(
        self: *BinanceFuturesClient,
        symbol: []const u8,
        usdt_notional: f64,
        leverage: f64,
        side: OrderSide,
        position_side: PositionSide,
        reduce_only: bool,
    ) !OrderResult {
        _ = leverage; // currently unused, kept for API compatibility
        if (!self.enabled) return error.LiveTradingDisabled;
        const price = try self.getMarkPrice(symbol);
        const qty = usdt_notional / price;
        return self.placeOrderWithQuantity(symbol, qty, side, position_side, reduce_only);
    }

    fn placeOrderWithQuantity(
        self: *BinanceFuturesClient,
        symbol: []const u8,
        quantity: f64,
        side: OrderSide,
        position_side: PositionSide,
        reduce_only: bool,
    ) !OrderResult {
        if (!self.enabled) return error.LiveTradingDisabled;
        _ = try self.ensureSymbolInfo(symbol);
        const norm_qty = try self.normalizeQuantity(symbol, quantity, try self.getMarkPrice(symbol));
        const client_order_id = try self.generateClientOrderId(symbol, side, position_side, reduce_only);

                var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        const side_str = switch (side) {
            .buy => "BUY",
            .sell => "SELL",
        };

        const position_side_str = switch (position_side) {
            .long => "LONG",
            .short => "SHORT",
        };

        var writer = query_buf.writer();

        // Base mandatory params
        try writer.print(
            "symbol={s}&side={s}&type=MARKET&positionSide={s}&quantity={d:.8}",
            .{ symbol, side_str, position_side_str, norm_qty },
        );

        // Only send reduceOnly when true (for closing positions)
        if (reduce_only) {
            try writer.print("&reduceOnly=true", .{});
        }

        // Finally, client order id
        try writer.print("&newClientOrderId={s}", .{client_order_id});
        const body = try self.signedRequest(.POST, "/fapi/v1/order", query_buf.items);
        defer self.allocator.free(body);
        const result = try self.parseOrderResult(body);
        self.allocator.free(client_order_id);
        return result;
    }

    fn generateClientOrderId(
        self: *BinanceFuturesClient,
        symbol: []const u8,
        side: OrderSide,
        position_side: PositionSide,
        reduce_only: bool,
    ) ![]const u8 {
        return try std.fmt.allocPrint(self.allocator, "{s}_{s}_{s}_{s}", .{
            symbol,
            switch (side) {
                .buy => "BUY",
                .sell => "SELL",
            },
            switch (position_side) {
                .long => "LONG",
                .short => "SHORT",
            },
            if (reduce_only) "REDUCE" else "OPEN",
        });
    }

    fn parseOrderResult(self: *BinanceFuturesClient, body: []const u8) !OrderResult {
        var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, body, .{});
        defer parsed.deinit();
        const root = parsed.value;
        const order_id_val = root.object.get("orderId") orelse return error.ParseError;
        const client_order_val = root.object.get("clientOrderId") orelse return error.ParseError;
        const executed_qty_val = root.object.get("executedQty") orelse return error.ParseError;
        const avg_price_val = root.object.get("avgPrice");
        const cum_quote_val = root.object.get("cumQuote");
        const status_val = root.object.get("status");

        const order_id: i64 = switch (order_id_val) {
            .integer => |i| @intCast(i),
            .string => |s| std.fmt.parseInt(i64, s, 10) catch return error.ParseError,
            else => return error.ParseError,
        };

        const executed_qty = switch (executed_qty_val) {
            .string => |s| std.fmt.parseFloat(f64, s) catch return error.ParseError,
            .float => |f| f,
            else => return error.ParseError,
        };

        const avg_price = if (avg_price_val) |val| switch (val) {
            .string => |s| std.fmt.parseFloat(f64, s) catch 0.0,
            .float => |f| f,
            else => 0.0,
        } else 0.0;

        const cum_quote = if (cum_quote_val) |val| switch (val) {
            .string => |s| std.fmt.parseFloat(f64, s) catch 0.0,
            .float => |f| f,
            else => 0.0,
        } else 0.0;

        const client_order_id = switch (client_order_val) {
            .string => |s| try self.allocator.dupe(u8, s),
            else => return error.ParseError,
        };

        const status = if (status_val) |val| switch (val) {
            .string => |s| try self.allocator.dupe(u8, s),
            else => return error.ParseError,
        } else try self.allocator.dupe(u8, "UNKNOWN");

        return OrderResult{
            .order_id = order_id,
            .client_order_id = client_order_id,
            .executed_qty = executed_qty,
            .avg_price = avg_price,
            .cum_quote = cum_quote,
            .status = status,
        };
    }

    pub fn freeOrderResult(self: *BinanceFuturesClient, result: OrderResult) void {
        self.allocator.free(result.client_order_id);
        self.allocator.free(result.status);
    }

    fn normalizeQuantity(self: *BinanceFuturesClient, symbol: []const u8, quantity: f64, price: f64) !f64 {
        const info = try self.ensureSymbolInfo(symbol);
        const step = info.step_size;
        if (step <= 0) return error.InvalidStepSize;
        const floored_steps = std.math.floor(quantity / step);
        const adjusted_qty = floored_steps * step;
        if (adjusted_qty <= 0) return error.InvalidQuantity;
        if ((adjusted_qty * price) < info.min_notional or adjusted_qty < info.min_qty) {
            return error.QuantityTooSmall;
        }
        const precision_width: usize = @intCast(info.quantity_precision);
        const pow10 = std.math.pow(f64, 10, @floatFromInt(precision_width));
        const rounded = std.math.floor(adjusted_qty * pow10) / pow10;
        return rounded;
    }

    fn ensureSymbolInfo(self: *BinanceFuturesClient, symbol: []const u8) !SymbolInfo {
        if (self.symbol_info.get(symbol)) |info| return info;
        const info = try self.fetchSymbolInfo(symbol);
        try self.symbol_info.put(try self.allocator.dupe(u8, symbol), info);
        return info;
    }

    fn fetchSymbolInfo(self: *BinanceFuturesClient, symbol: []const u8) !SymbolInfo {
        var url_buf = std.ArrayList(u8).init(self.allocator);
        defer url_buf.deinit();
        try url_buf.writer().print("{s}/fapi/v1/exchangeInfo?symbol={s}", .{ base_url, symbol });
        const uri = try std.Uri.parse(url_buf.items);
        var req = try self.http_client.open(.GET, uri, .{ .server_header_buffer = try self.allocator.alloc(u8, 8192) });
        defer req.deinit();

        try req.send();
        try req.wait();
        if (req.response.status != .ok) return error.ExchangeInfoFailed;

        const body = try req.reader().readAllAlloc(self.allocator, 1024 * 1024);
        defer self.allocator.free(body);

        var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, body, .{});
        defer parsed.deinit();
        const root = parsed.value;
        const symbols = root.object.get("symbols") orelse return error.ExchangeInfoFailed;
        if (symbols != .array) return error.ExchangeInfoFailed;

        const default_step_size: f64 = 0.0001;
        const default_min_qty: f64 = default_step_size;
        const default_min_notional: f64 = 5.0;
        const default_precision: u8 = 8;

        for (symbols.array.items) |sym| {
            if (sym != .object) continue;
            const sym_obj = sym.object;
            const symbol_val = sym_obj.get("symbol") orelse continue;
            if (symbol_val != .string) continue;
            if (!std.mem.eql(u8, symbol_val.string, symbol)) continue;

            var step_size: f64 = default_step_size;
            var min_qty: f64 = default_min_qty;
            var min_notional: f64 = default_min_notional;
            var precision: u8 = default_precision;

            if (sym_obj.get("quantityPrecision")) |prec_val| {
                if (prec_val == .integer) {
                    precision = @intCast(prec_val.integer);
                }
            }

            if (sym_obj.get("filters")) |filters_val| {
                if (filters_val == .array) {
                    for (filters_val.array.items) |filter| {
                        if (filter != .object) continue;
                        const fobj = filter.object;
                        const filter_type = fobj.get("filterType") orelse continue;
                        if (filter_type != .string) continue;
                        if (std.mem.eql(u8, filter_type.string, "LOT_SIZE")) {
                            if (fobj.get("stepSize")) |step_val| {
                                if (step_val == .string) step_size = std.fmt.parseFloat(f64, step_val.string) catch step_size;
                            }
                            if (fobj.get("minQty")) |min_qty_val| {
                                if (min_qty_val == .string) min_qty = std.fmt.parseFloat(f64, min_qty_val.string) catch min_qty;
                            }
                        } else if (std.mem.eql(u8, filter_type.string, "MIN_NOTIONAL")) {
                            if (fobj.get("notional")) |notional_val| {
                                if (notional_val == .string) min_notional = std.fmt.parseFloat(f64, notional_val.string) catch min_notional;
                            }
                        }
                    }
                }
            }

            if (step_size == 0.0) step_size = 0.0001;
            if (min_qty == 0.0) min_qty = step_size;
            if (min_notional == 0.0) min_notional = 5.0;

            return SymbolInfo{
                .step_size = step_size,
                .min_qty = min_qty,
                .min_notional = min_notional,
                .quantity_precision = precision,
            };
        }

        return SymbolInfo{
            .step_size = default_step_size,
            .min_qty = default_min_qty,
            .min_notional = default_min_notional,
            .quantity_precision = default_precision,
        };
    }

    fn getMarkPrice(self: *BinanceFuturesClient, symbol: []const u8) !f64 {
        var url_buf = std.ArrayList(u8).init(self.allocator);
        defer url_buf.deinit();
        try url_buf.writer().print("{s}/fapi/v1/ticker/price?symbol={s}", .{ base_url, symbol });
        const uri = try std.Uri.parse(url_buf.items);
        var req = try self.http_client.open(.GET, uri, .{ .server_header_buffer = try self.allocator.alloc(u8, 4096) });
        defer req.deinit();
        try req.send();
        try req.wait();
        if (req.response.status != .ok) return error.PriceRequestFailed;
        const body = try req.reader().readAllAlloc(self.allocator, 4096);
        defer self.allocator.free(body);
        var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, body, .{});
        defer parsed.deinit();
        const root = parsed.value;
        const price_val = root.object.get("price") orelse return error.PriceRequestFailed;
        if (price_val != .string) return error.PriceRequestFailed;
        return std.fmt.parseFloat(f64, price_val.string);
    }

    fn signedRequest(self: *BinanceFuturesClient, method: http.Method, path: []const u8, base_query: []const u8) ![]const u8 {
        const timestamp: i64 = @intCast(std.time.milliTimestamp());
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        if (base_query.len > 0) {
            try query_buf.appendSlice(base_query);
            try query_buf.append('&');
        }

        try query_buf.writer().print("timestamp={d}&recvWindow={d}", .{ timestamp, self.recv_window });

        var sig_buf: [64]u8 = undefined;
        const sig = try self.signQuery(query_buf.items, &sig_buf);

        var url_buf = std.ArrayList(u8).init(self.allocator);
        defer url_buf.deinit();
        try url_buf.writer().print("{s}{s}?{s}&signature={s}", .{ base_url, path, query_buf.items, sig });
        const uri = try std.Uri.parse(url_buf.items);

        const extra_headers = [_]http.Header{
            .{ .name = "X-MBX-APIKEY", .value = self.api_key },
        };

        var req = try self.http_client.open(method, uri, .{
            .server_header_buffer = try self.allocator.alloc(u8, 8192),
            .extra_headers = &extra_headers,
        });
        defer req.deinit();

        try req.send();
        try req.wait();

        const body = try req.reader().readAllAlloc(self.allocator, 1024 * 1024);

        if (req.response.status != .ok and req.response.status != .created and req.response.status != .accepted) {
            self.logBinanceError(path, body);
            self.allocator.free(body);
            return error.BinanceError;
        }

        return body;
    }

    fn signQuery(self: *BinanceFuturesClient, query: []const u8, out_buf: *[64]u8) ![]const u8 {
        var mac = std.crypto.auth.hmac.sha2.HmacSha256.init(self.api_secret);
        mac.update(query);
        var digest: [32]u8 = undefined;
        mac.final(&digest);
        return std.fmt.bufPrint(out_buf, "{s}", .{std.fmt.fmtSliceHexLower(&digest)});
    }

    fn logBinanceError(self: *BinanceFuturesClient, path: []const u8, body: []const u8) void {
        var parsed = std.json.parseFromSlice(std.json.Value, self.allocator, body, .{}) catch |err| {
            std.log.err("Binance request {s} failed with status and unparsable body: {}", .{ path, err });
            return;
        };
        defer parsed.deinit();
        const root = parsed.value;
        const code_val = root.object.get("code");
        const msg_val = root.object.get("msg");
        if (code_val != null or msg_val != null) {
            if (code_val) |cv| {
                switch (cv) {
                    .integer => std.log.err("Binance error {s}: code {d} msg {s}", .{ path, cv.integer, if (msg_val != null and msg_val.? == .string) msg_val.?.string else "" }),
                    else => std.log.err("Binance error {s}: msg {s}", .{ path, if (msg_val != null and msg_val.? == .string) msg_val.?.string else "" }),
                }
            } else {
                std.log.err("Binance error {s}: msg {s}", .{ path, if (msg_val != null and msg_val.? == .string) msg_val.?.string else "" });
            }
        }
    }
};
