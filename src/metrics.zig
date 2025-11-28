const std = @import("std");

pub const MetricType = enum {
    depth_handler_msg,
    ticker_handler_msg,
    depth_handler_duration,
    ticker_handler_duration,
};

pub const MetricData = struct {
    metric_type: MetricType,
    value: f64,
    timestamp: i64,
};

pub const MetricsChannel = struct {
    allocator: std.mem.Allocator,
    storage: std.ArrayListUnmanaged(MetricData),
    head_index: usize,
    running: bool,

    pub fn init(allocator: std.mem.Allocator) !*MetricsChannel {
        const channel = try allocator.create(MetricsChannel);
        channel.* = MetricsChannel{
            .allocator = allocator,
            .storage = .{},
            .head_index = 0,
            .running = true,
        };
        return channel;
    }

    pub fn deinit(self: *MetricsChannel) void {
        self.storage.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    pub fn send(self: *MetricsChannel, metric: MetricData) bool {
        if (!self.running) return false;
        self.storage.append(self.allocator, metric) catch return false;
        return true;
    }

    pub fn receive(self: *MetricsChannel) ?MetricData {
        if (self.head_index >= self.storage.items.len) {
            return null;
        }

        const metric = self.storage.items[self.head_index];
        self.head_index += 1;
        if (self.head_index >= self.storage.items.len) {
            self.storage.clearRetainingCapacity();
            self.head_index = 0;
        }
        return metric;
    }

    pub fn stop(self: *MetricsChannel) void {
        self.running = false;
    }

    pub fn isRunning(self: *MetricsChannel) bool {
        return self.running;
    }

    pub fn hasData(self: *MetricsChannel) bool {
        return self.head_index < self.storage.items.len;
    }
};

pub const MetricsCollector = struct {
    channel: *MetricsChannel,
    depth_msg_count: usize,
    ticker_msg_count: usize,

    pub fn init(channel: *MetricsChannel) MetricsCollector {
        return MetricsCollector{
            .channel = channel,
            .depth_msg_count = 0,
            .ticker_msg_count = 0,
        };
    }

    pub fn recordDepthMessage(self: *MetricsCollector, duration_us: f64) void {
        _ = duration_us;
        self.depth_msg_count += 1;
    }

    pub fn recordTickerMessage(self: *MetricsCollector, duration_us: f64) void {
        _ = duration_us;
        self.ticker_msg_count += 1;
    }

    pub fn flush(self: *MetricsCollector) void {
        const timestamp = std.time.microTimestamp();

        if (self.depth_msg_count > 0) {
            const metric = MetricData{
                .metric_type = .depth_handler_msg,
                .value = @floatFromInt(self.depth_msg_count),
                .timestamp = timestamp,
            };
            _ = self.channel.send(metric);
            self.depth_msg_count = 0;
        }

        if (self.ticker_msg_count > 0) {
            const metric = MetricData{
                .metric_type = .ticker_handler_msg,
                .value = @floatFromInt(self.ticker_msg_count),
                .timestamp = timestamp,
            };
            _ = self.channel.send(metric);
            self.ticker_msg_count = 0;
        }
    }
};

pub fn metricsThread(channel: *MetricsChannel) void {
    while (channel.isRunning() or channel.hasData()) {
        if (channel.receive()) |_| {
            // Metrics are intentionally ignored in this stub implementation.
        } else {
            std.time.sleep(100_000_000);
        }
    }
}
