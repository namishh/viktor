const std = @import("std");
const BagOfWords = @import("processing").BagOfWords;
const shimmer = @import("shimmer");

fn setupBag(allocator: std.mem.Allocator) !BagOfWords {
    var Bag = try BagOfWords.init(allocator);
    const documents = [_][]const u8{ "hello world", "hello zig", "zig is great", "completely different string" };

    try Bag.fit(&documents);
    return Bag;
}

pub const Person = struct {
    name: []const u8,
    isCool: bool,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try shimmer.Environment.init(allocator);
    defer env.deinit();

    const db_id = try env.open("testing_db");
    const db = try env.get_db(db_id);
    db.setImmutable(false);

    const txn_id = try env.begin_txn(.ReadWrite);
    const txn = try env.get_txn(txn_id);

    try db.putTyped(Person, txn, "candidate_2", Person{ .isCool = true, .name = "rizzmobly" }, allocator);

    if (try db.getTyped(Person, txn, "candidate_2")) |person| {
        defer person.deinit();
        std.debug.print("Person: name = {s}, isCool = {}\n", .{ person.data.name, person.data.isCool });
    }

    try env.commit_txn(txn_id);
}
