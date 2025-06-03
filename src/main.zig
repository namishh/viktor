const std = @import("std");
const BagOfWords = @import("processing").BagOfWords;
const shimmer = @import("shimmer");

fn setupBag(allocator: std.mem.Allocator) !BagOfWords {
    var Bag = try BagOfWords.init(allocator);
    const documents = [_][]const u8{ "hello world", "hello zig", "zig is great", "completely different string" };

    try Bag.fit(&documents);
    return Bag;
}

pub const Message = struct {
    to: []const u8,
    message: []const u8,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try shimmer.Environment.init(allocator);
    defer env.deinit();

    env.set_time_logging(true, &.{ .Transaction, .Database });

    const db_id = try env.open("new_database.db");
    const db = try env.get_db(db_id);
    db.setImmutable(false);

    const txn_id = try env.begin_txn(.ReadWrite);
    const txn = try env.get_txn(txn_id);

    try db.putTyped(Message, txn, "birthday_boy", Message{ .to = "@seatedro", .message = "happy birthday big bro" }, allocator);

    if (try db.getTyped(Message, txn, "birthday_boy")) |person| {
        defer person.deinit();
        std.debug.print("TO = {s}\nMESSAGE = {s}\n", .{ person.data.to, person.data.message });
    }

    try env.commit_txn(txn_id);
}
