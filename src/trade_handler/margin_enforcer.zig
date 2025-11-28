const std = @import("std");
const binance_client = @import("binance_futures_client.zig");

pub const FuturesMarginMode: []const u8 = "ISOLATED"; // CROSS is disabled in this build.

pub const MarginEnforcer = struct {
    allocator: std.mem.Allocator,
    testnet_enabled: bool,
    isolated_symbols: std.StringHashMap(void),
    binance: ?*binance_client.BinanceFuturesClient,

    pub fn init(
        allocator: std.mem.Allocator,
        testnet_enabled: bool,
        binance: ?*binance_client.BinanceFuturesClient,
    ) MarginEnforcer {
        return .{
            .allocator = allocator,
            .testnet_enabled = testnet_enabled,
            .isolated_symbols = std.StringHashMap(void).init(allocator),
            .binance = binance,
        };
    }

    pub fn deinit(self: *MarginEnforcer) void {
        var iterator = self.isolated_symbols.iterator();
        while (iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.isolated_symbols.deinit();
    }

    pub fn ensureIsolatedMargin(self: *MarginEnforcer, symbol: []const u8) !void {
        if (self.isolated_symbols.get(symbol) != null) {
            return;
        }

        if (self.binance) |client| {
            if (client.isLive()) {
                client.setMarginType(symbol, FuturesMarginMode) catch |err| {
                    std.log.err("Failed to set isolated margin on Binance for {s}: {}", .{ symbol, err });
                };
            }
        }

        const owned_symbol = try self.allocator.dupe(u8, symbol);
        errdefer self.allocator.free(owned_symbol);
        try self.isolated_symbols.put(owned_symbol, {});

        std.log.debug("Margin mode set to {s} for {s} (CROSS disabled)", .{ FuturesMarginMode, symbol });
    }
};
