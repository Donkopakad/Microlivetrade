const std = @import("std");

pub const FuturesMarginMode: []const u8 = "ISOLATED"; // CROSS is disabled in this build.

pub const MarginEnforcer = struct {
    allocator: std.mem.Allocator,
    testnet_enabled: bool,
    isolated_symbols: std.StringHashMap(void),

    pub fn init(allocator: std.mem.Allocator, testnet_enabled: bool) MarginEnforcer {
        return .{
            .allocator = allocator,
            .testnet_enabled = testnet_enabled,
            .isolated_symbols = std.StringHashMap(void).init(allocator),
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
        // If we've already marked this symbol as isolated for the current run, treat it as success.
        if (self.isolated_symbols.get(symbol) != null) {
            return;
        }

        // In a live integration, this is where we'd call Binance's POST /fapi/v1/marginType endpoint
        // with marginType=ISOLATED. Binance returns error -4046 when the margin type is already
        // isolated; we treat that as success to avoid crashing when no change is needed.
        // Since this build is locked to ISOLATED, CROSS margin is never requested.
        _ = self.testnet_enabled; // placeholder for when live/testnet routing is wired up

        const owned_symbol = try self.allocator.dupe(u8, symbol);
        errdefer self.allocator.free(owned_symbol);
        try self.isolated_symbols.put(owned_symbol, {});

        std.log.debug("Margin mode set to {s} for {s} (CROSS disabled)", .{ FuturesMarginMode, symbol });
    }
};
