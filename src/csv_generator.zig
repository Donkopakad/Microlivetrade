const std = @import("std");
const AtomicBool = std.atomic.Value(bool);

/// Legacy CSV generator kept for optional logging. Trading no longer depends on it.
pub fn run(stop_flag: *AtomicBool) !void {
    std.log.info("CSV generator is idle (GPU trading active). Set flag to false to exit thread.", .{});
    while (stop_flag.load(.SeqCst)) {
        std.time.sleep(1_000_000_000);
    }
    std.log.info("CSV generator thread exiting", .{});
}
