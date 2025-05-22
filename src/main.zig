const std = @import("std");

pub fn VectorDB(comptime T: type) type {
    return struct {
        vectors: std.ArrayList([]T),
        allocator: std.mem.Allocator,

        pub fn init(allocator: std.mem.Allocator) !@This() {
            return .{
                .vectors = try std.ArrayList([]T).initCapacity(allocator, 16),
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *@This()) void {
            self.vectors.deinit();
        }
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var viktor = try VectorDB(u8).init(allocator);
    defer viktor.deinit();
    std.debug.print("hello world", .{});
}
