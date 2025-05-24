const std = @import("std");
const BagOfWords = @import("processing").BagOfWords;

fn setupBag(allocator: std.mem.Allocator) !BagOfWords {
    var Bag = try BagOfWords.init(allocator);
    const documents = [_][]const u8{ "hello world", "hello zig", "zig is great", "completely different string" };

    try Bag.fit(&documents);
    return Bag;
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var bag = try setupBag(allocator);
    defer bag.deinit();

    const string1 = "different world world";
    const s = try bag.transform(string1);

    std.debug.print("Transformed vector: ", .{});
    for (s, 0..) |val, i| {
        if (i > 0) std.debug.print(", ", .{});
        std.debug.print("{}", .{val});
    }
    std.debug.print("\n", .{});
    allocator.free(s);

    std.debug.print("hello world", .{});
}
