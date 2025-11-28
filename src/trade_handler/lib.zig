const std = @import("std");
const types = @import("../types.zig");
const SymbolMap = @import("../symbol-map.zig").SymbolMap;
const portfolio_manager = @import("portfolio_manager.zig");
const PortfolioManager = portfolio_manager.PortfolioManager;
const PositionSide = portfolio_manager.PositionSide;
const TradingSignal = types.TradingSignal;

const EXIT_INTERVAL_NS: u64 = 500_000_000; // 500ms

pub const TradeHandler = struct {
    allocator: std.mem.Allocator,
    signal_queue: std.ArrayList(TradingSignal),
    signal_thread: ?std.Thread,
    exit_thread: ?std.Thread,
    run_flag: std.atomic.Value(bool),
    mutex: std.Thread.Mutex,
    condition: std.Thread.Condition,
    portfolio_mutex: std.Thread.Mutex,
    portfolio_manager: PortfolioManager,

    pub fn init(allocator: std.mem.Allocator, symbol_map: *const SymbolMap) TradeHandler {
        return TradeHandler{
            .allocator = allocator,
            .signal_queue = std.ArrayList(TradingSignal).init(allocator),
            .signal_thread = null,
            .exit_thread = null,
            .run_flag = std.atomic.Value(bool).init(true),
            .mutex = .{},
            .condition = .{},
            .portfolio_mutex = .{},
            .portfolio_manager = PortfolioManager.init(allocator, symbol_map),
        };
    }

    pub fn deinit(self: *TradeHandler) void {
        self.run_flag.store(false, .seq_cst);
        self.condition.signal();
        if (self.signal_thread) |thread| {
            thread.join();
        }
        if (self.exit_thread) |thread| {
            thread.join();
        }
        self.signal_queue.deinit();
        self.portfolio_manager.deinit();
    }

    pub fn start(self: *TradeHandler) !void {
        self.signal_thread = try std.Thread.spawn(.{ .allocator = self.allocator }, signalThreadFunction, .{self});
        self.exit_thread = try std.Thread.spawn(.{ .allocator = self.allocator }, exitThreadFunction, .{self});
    }

    pub fn addSignal(self: *TradeHandler, signal: TradingSignal) !void {
        self.mutex.lock();
        const append_result = self.signal_queue.append(signal);
        self.condition.signal();
        self.mutex.unlock();
        if (append_result) |_| {
            // ok
        } else |err| {
            return err;
        }
    }

    pub inline fn getPositionSide(self: *TradeHandler, symbol_name: []const u8) PositionSide {
        return self.portfolio_manager.getPositionSide(symbol_name);
    }

    fn signalThreadFunction(self: *TradeHandler) !void {
        std.log.info("Trade handler thread started", .{});
        while (self.run_flag.load(.seq_cst)) {
            self.mutex.lock();
            while (self.signal_queue.items.len == 0 and self.run_flag.load(.seq_cst)) {
                self.condition.wait(&self.mutex);
            }
            if (!self.run_flag.load(.seq_cst)) {
                self.mutex.unlock();
                break;
            }
            const signal = self.signal_queue.orderedRemove(0);
            self.mutex.unlock();

            self.portfolio_mutex.lock();
            self.portfolio_manager.processSignal(signal) catch |err| {
                std.log.err("Failed to process signal: {}", .{err});
            };

            self.portfolio_manager.checkStopLossConditions() catch |err| {
                std.log.warn("Failed to apply time-based exit checks: {}", .{err});
            };
            self.portfolio_mutex.unlock();
        }
    }

    fn exitThreadFunction(self: *TradeHandler) !void {
        while (self.run_flag.load(.seq_cst)) {
            std.time.sleep(EXIT_INTERVAL_NS);
            if (!self.run_flag.load(.seq_cst)) break;

            self.portfolio_mutex.lock();
            self.portfolio_manager.checkStopLossConditions() catch |err| {
                std.log.warn("Forced candle exit check failed: {}", .{err});
            };
            self.portfolio_mutex.unlock();
        }
    }
};
