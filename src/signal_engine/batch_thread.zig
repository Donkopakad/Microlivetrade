const std = @import("std");
const StatCalc = @import("../stat_calc/lib.zig").StatCalc;
const SymbolMap = @import("../symbol-map.zig").SymbolMap;
const types = @import("../types.zig");
const GPUBatchResult = types.GPUBatchResult;
const engine_types = @import("types.zig");

pub const BatchThread = struct {
    stat_calc: *StatCalc,
    symbol_map: *const SymbolMap,
    run_flag: *std.atomic.Value(bool),
    queue_mutex: *std.Thread.Mutex,
    queue_cond: *std.Thread.Condition,
    queue: *std.ArrayList(GPUBatchResult),

    pub fn loop(self: BatchThread) void {
        while (self.run_flag.load(.seq_cst)) {
            const batch_result = self.stat_calc.calculateSymbolMapBatch(self.symbol_map, 0) catch |err| {
                std.log.err("GPU batch calculation failed: {}", .{err});
                std.time.sleep(engine_types.BATCH_INTERVAL_NS);
                continue;
            };

            self.queue_mutex.lock();
            const append_result = self.queue.append(batch_result);
            self.queue_cond.signal();
            self.queue_mutex.unlock();

            if (append_result) |_| {
                // intentionally empty
            } else |append_err| {
                std.log.err("Failed to queue GPU batch result: {}", .{append_err});
            }

            std.time.sleep(engine_types.BATCH_INTERVAL_NS);
        }
    }
};
