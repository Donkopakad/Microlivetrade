const std = @import("std");
const http = std.http;
const time = std.time;
const json = std.json;

const _ = @import("../errors.zig");
const SymbolMap = @import("../symbol-map.zig").SymbolMap;
const Symbol = @import("../types.zig").Symbol;
const WSClient = @import("binance_ws.zig").WSClient;
const metrics = @import("../metrics.zig");

const REST_ENDPOINTS = [3][]const u8{
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
};

const PING_API = "/fapi/v1/ping";
const EXCHANGE_INFO_API = "/fapi/v1/exchangeInfo";
const TIME_API = "/fapi/v1/time";

const PingResult = struct {
    endpoint: []const u8,
    latency_ms: u64 = 0,
    ok: bool = false,
};

pub const Client = struct {
    selected_endpoint: []const u8,
    http_client: http.Client,
    ws_client: WSClient,
    allocator: std.mem.Allocator,
    metrics_collector: ?*metrics.MetricsCollector,

    pub fn init(allocator: std.mem.Allocator, metrics_collector: ?*metrics.MetricsCollector) !Client {
        return Client{
            .selected_endpoint = REST_ENDPOINTS[0],
            .http_client = http.Client{
                .allocator = allocator,
            },
            .ws_client = try WSClient.init(allocator, metrics_collector),
            .allocator = allocator,
            .metrics_collector = metrics_collector,
        };
    }

    pub fn deinit(self: *Client) void {
        self.http_client.deinit();
        self.ws_client.deinit();
    }

    pub fn connect(self: *Client) !void {
        try self.selectBestEndpoint();
        std.debug.print("Connecting to Binance using endpoint: {s}\n", .{self.selected_endpoint});
    }

    pub fn loadSymbols(self: *Client, sym_map: *SymbolMap) !void {
        std.debug.print("Loading symbols from exchange info...\n", .{});
        var exchange_url_buf: [256]u8 = undefined;
        const exchange_url = try std.fmt.bufPrint(&exchange_url_buf, "{s}{s}", .{ self.selected_endpoint, EXCHANGE_INFO_API });
        const uri = try std.Uri.parse(exchange_url);
        var req = try self.http_client.open(.GET, uri, .{
            .server_header_buffer = try self.allocator.alloc(u8, 8192),
        });
        defer req.deinit();

        try req.send();
        try req.wait();

        if (req.response.status != .ok) {
            std.log.err(
                "Failed to load Binance Futures symbols from exchange info: HTTP status {d}",
                .{@intFromEnum(req.response.status)},
            );
            return error.ExchangeInfoRequestFailed;
        }

        const body = req.reader().readAllAlloc(self.allocator, 1024 * 1024 * 20) catch |err| {
            std.log.err("Failed to read exchange info body: {}", .{err});
            return err;
        };
        defer self.allocator.free(body);

        self.parseAndStoreSymbols(body, sym_map) catch |err| {
            std.log.err("Failed to load Binance Futures symbols from exchange info: {}", .{err});
            return err;
        };

        if (sym_map.count() == 0) {
            std.log.err("No Binance Futures USDT-PERP symbols available; aborting startup", .{});
            return error.EmptySymbolUniverse;
        }

        std.debug.print("Loaded {} symbols\n", .{sym_map.count()});
    }

    fn parseAndStoreSymbols(self: *Client, json_data: []const u8, sym_map: *SymbolMap) !void {
        var parsed = json.parseFromSlice(json.Value, self.allocator, json_data, .{}) catch |err| {
            std.log.err("Failed to parse JSON: {}", .{err});
            return err;
        };
        defer parsed.deinit();
        const root = parsed.value;

        if (root != .object) return error.InvalidExchangeInfoFormat;

        const symbols_array = root.object.get("symbols") orelse return error.NoSymbolsFound;
        if (symbols_array != .array) return error.InvalidSymbolsFormat;

        // Futures USDT-PERP universe only
        for (symbols_array.array.items) |symbol_value| {
            if (symbol_value != .object) continue;
            const symbol_obj = symbol_value.object;

            const symbol_name_val = symbol_obj.get("symbol") orelse continue;
            if (symbol_name_val != .string) continue;
            const symbol_str = symbol_name_val.string;
            if (symbol_str.len < 5 or !std.mem.endsWith(u8, symbol_str, "USDT")) continue;

            const quote_asset_val = symbol_obj.get("quoteAsset") orelse continue;
            if (quote_asset_val != .string or !std.mem.eql(u8, quote_asset_val.string, "USDT")) continue;

            const contract_type_val = symbol_obj.get("contractType") orelse continue;
            if (contract_type_val != .string or !std.mem.eql(u8, contract_type_val.string, "PERPETUAL")) continue;

            const status_val = symbol_obj.get("status") orelse continue;
            if (status_val != .string or !std.mem.eql(u8, status_val.string, "TRADING")) continue;

            const owned_symbol = try self.allocator.dupe(u8, symbol_str);
            const empty_symbol = Symbol.init();
            try sym_map.put(owned_symbol, empty_symbol);
        }

        if (sym_map.count() == 0) return error.EmptySymbolUniverse;
    }

       fn selectBestEndpoint(self: *Client) !void {
        var ping_results = [_]PingResult{
            .{ .endpoint = REST_ENDPOINTS[0] },
            .{ .endpoint = REST_ENDPOINTS[1] },
            .{ .endpoint = REST_ENDPOINTS[2] },
        };

        std.debug.print("Testing ping for {} Binance REST_ENDPOINTS...\n", .{REST_ENDPOINTS.len});
        for (ping_results, 0..) |*result, i| {
            _ = i; // silence unused
            const ping_result = self.pingEndpoint(result.endpoint) catch |err| {
                std.debug.print("Failed to ping {s}: {}\n", .{ result.endpoint, err });
                continue;
            };

            result.latency_ms = ping_result;
            result.ok = true;
            std.debug.print("Endpoint {s}: {}ms\n", .{ result.endpoint, ping_result });
        }
        var best_endpoint: ?PingResult = null;
        for (ping_results) |result| {
            if (!result.ok) continue;
            if (best_endpoint == null or result.latency_ms < best_endpoint.?.latency_ms) {
                best_endpoint = result;
            }
        }
        if (best_endpoint) |result| {
            self.selected_endpoint = result.endpoint;
            std.debug.print("Selected best endpoint: {s} ({}ms)\n", .{ result.endpoint, result.latency_ms });
        } else {
            std.debug.print(
                "All Binance Futures endpoints failed ping; falling back to {s}\n",
                .{ REST_ENDPOINTS[0] },
            );
            self.selected_endpoint = REST_ENDPOINTS[0];
        }
    }

    fn pingEndpoint(self: *Client, endpoint: []const u8) !u64 {
        const start_time = time.nanoTimestamp();
        var ping_url_buf: [256]u8 = undefined;
        const ping_url = try std.fmt.bufPrint(&ping_url_buf, "{s}{s}", .{ endpoint, PING_API });
        const uri = try std.Uri.parse(ping_url);

        var req = try self.http_client.open(.GET, uri, .{
            .server_header_buffer = try self.allocator.alloc(u8, 4096),
        });
        defer req.deinit();

        try req.send();
        try req.wait();

        const end_time = time.nanoTimestamp();
        const ping_ms = @as(u64, @intCast(@divTrunc(end_time - start_time, time.ns_per_ms)));
        if (req.response.status != .ok) {
            return error.BadResponse;
        }
        return ping_ms;
    }
};
