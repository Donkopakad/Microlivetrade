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

    pub fn ensureSymbolInfo(self: *BinanceFuturesClient, symbol: []const u8) !SymbolInfo {
        if (self.symbol_info.get(symbol)) |info| return info;

        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();
        try query_buf.writer().print("symbol={s}", .{ symbol });

        var url_buf = std.ArrayList(u8).init(self.allocator);
        defer url_buf.deinit();
        try url_buf.writer().print("{s}/fapi/v1/exchangeInfo?{s}", .{ base_url, query_buf.items });
        const uri = try std.Uri.parse(url_buf.items);

        var req = try self.http_client.request(.GET, uri, .{ .server_header_buffer = try self.allocator.alloc(u8, 4096) });
        defer req.deinit();

        try req.send();
        try req.wait();

        const body = try req.reader().readAllAlloc(self.allocator, 4096);
        defer self.allocator.free(body);
        var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, body, .{});
        defer parsed.deinit();
        const root = parsed.value;

        const symbols = root.object.get("symbols") orelse return error.ExchangeInfoParseError;
        if (symbols != .array) return error.ExchangeInfoParseError;
        for (symbols.array.items) |sym_info| {
            if (sym_info != .object) continue;
            const sym_obj = sym_info.object;
            const sym_name = sym_obj.get("symbol") orelse continue;
            if (sym_name != .string) continue;
            if (!std.mem.eql(u8, sym_name.string, symbol)) continue;

            const filters = sym_obj.get("filters") orelse return error.ExchangeInfoParseError;
            if (filters != .array) return error.ExchangeInfoParseError;

            var step_size: f64 = 0.0;
            var min_qty: f64 = 0.0;
            var min_notional: f64 = 0.0;
            var quantity_precision: u8 = 0;

            for (filters.array.items) |filter| {
                if (filter != .object) continue;
                const filter_obj = filter.object;
                const filter_type_val = filter_obj.get("filterType") orelse continue;
                if (filter_type_val != .string) continue;
                if (std.mem.eql(u8, filter_type_val.string, "LOT_SIZE")) {
                    const step_size_val = filter_obj.get("stepSize") orelse return error.ExchangeInfoParseError;
                    const min_qty_val = filter_obj.get("minQty") orelse return error.ExchangeInfoParseError;
                    if (step_size_val != .string or min_qty_val != .string) return error.ExchangeInfoParseError;
                    step_size = std.fmt.parseFloat(f64, step_size_val.string) catch return error.ExchangeInfoParseError;
                    min_qty = std.fmt.parseFloat(f64, min_qty_val.string) catch return error.ExchangeInfoParseError;
                } else if (std.mem.eql(u8, filter_type_val.string, "MIN_NOTIONAL")) {
                    const notional_val = filter_obj.get("notional") orelse return error.ExchangeInfoParseError;
                    if (notional_val != .string) return error.ExchangeInfoParseError;
                    min_notional = std.fmt.parseFloat(f64, notional_val.string) catch return error.ExchangeInfoParseError;
                }
            }

            const qty_precision_val = sym_obj.get("quantityPrecision") orelse return error.ExchangeInfoParseError;
            if (qty_precision_val != .integer) return error.ExchangeInfoParseError;
            quantity_precision = @intCast(qty_precision_val.integer);

            const info = SymbolInfo{
                .step_size = step_size,
                .min_qty = min_qty,
                .min_notional = min_notional,
                .quantity_precision = quantity_precision,
            };
            try self.symbol_info.put(try self.allocator.dupe(u8, symbol), info);
            return info;
        }

        return error.ExchangeInfoParseError;
    }

    pub fn getMarkPrice(self: *BinanceFuturesClient, symbol: []const u8) !f64 {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();
        try query_buf.writer().print("symbol={s}", .{ symbol });

        var url_buf = std.ArrayList(u8).init(self.allocator);
        defer url_buf.deinit();
        try url_buf.writer().print("{s}/fapi/v1/premiumIndex?{s}", .{ base_url, query_buf.items });
        const uri = try std.Uri.parse(url_buf.items);

        var req = try self.http_client.request(.GET, uri, .{ .server_header_buffer = try self.allocator.alloc(u8, 4096) });
        defer req.deinit();

        try req.send();
        try req.wait();

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

        var headers = http.Headers{ .allocator = self.allocator, .list = .{} };
        defer headers.deinit();
        try headers.append("X-MBX-APIKEY", self.api_key);

        var req = try self.http_client.request(method, uri, .{
            .server_header_buffer = try self.allocator.alloc(u8, 8192),
            .headers = &headers,
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
