const std = @import("std");
const Environment = @import("shimmer.zig").Environment;
const Database = @import("shimmer.zig").Database;
const Transaction = @import("shimmer.zig").Transaction;
const Value = @import("shimmer.zig").Value;
const DatabaseError = @import("shimmer.zig").DatabaseError;
const TransactionState = @import("shimmer.zig").TransactionState;

const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const expectError = testing.expectError;

fn setupTestEnvironment(allocator: std.mem.Allocator) !Environment {
    return try Environment.init(allocator);
}

test "Environment: Basic initialization and cleanup" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    try expectEqual(@as(u32, 1), env.next_db_id);
    try expectEqual(@as(u32, 1), env.next_txn_id);
    try expectEqual(@as(usize, 0), env.databases.count());
    try expectEqual(@as(usize, 0), env.transactions.count());
}

test "Database: Basic initialization" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    const db_id = try env.open("test_database");
    const db = try env.get_db(db_id);

    try expectEqual(@as(u32, 1), db.id);
    try expectEqual(@as(u32, 1), db.root_page);
    try expectEqual(@as(u32, 2), db.next_page_id);
    try expect(std.mem.eql(u8, "test_database", db.name));
    try expectEqual(@as(usize, 1), db.pages.count());
}

test "Database: root page initialization" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    const db_id = try env.open("test_db");
    const db = try env.get_db(db_id);

    const root_page = db.pages.get(1).?;
    try expectEqual(@as(u32, 1), root_page.header.page_id);
    try expectEqual(@as(u32, 0), root_page.header.parent_id);
    try expect(root_page.header.is_leaf);
    try expectEqual(@as(u32, 0), root_page.header.key_count);
}

test "Value: signed integer types" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    {
        const val = Value(i32){ .data = -123456789 };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value(i32).fromBytes(bytes, allocator);
        try expectEqual(@as(i32, -123456789), restored.data);
    }

    {
        const val = Value(i64){ .data = -9223372036854775807 };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value(i64).fromBytes(bytes, allocator);
        try expectEqual(@as(i64, -9223372036854775807), restored.data);
    }
}

test "Value: unsigned integer types" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    {
        const val = Value(u32){ .data = 4294967295 };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value(u32).fromBytes(bytes, allocator);
        try expectEqual(@as(u32, 4294967295), restored.data);
    }

    {
        const val = Value(u64){ .data = 18446744073709551615 };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value(u64).fromBytes(bytes, allocator);
        try expectEqual(@as(u64, 18446744073709551615), restored.data);
    }
}

test "Value: float types" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    {
        const val = Value(f32){ .data = 3.14159 };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value(f32).fromBytes(bytes, allocator);
        try expect(@abs(restored.data - 3.14159) < 0.0001);
    }

    {
        const val = Value(f64){ .data = 2.718281828459045 };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value(f64).fromBytes(bytes, allocator);
        try expect(@abs(restored.data - 2.718281828459045) < 0.000000000001);
    }
}

test "Value: boolean type" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    {
        const val = Value(bool){ .data = true };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value(bool).fromBytes(bytes, allocator);
        try expectEqual(true, restored.data);
    }

    {
        const val = Value(bool){ .data = false };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value(bool).fromBytes(bytes, allocator);
        try expectEqual(false, restored.data);
    }
}

test "Value: array types" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    {
        const val = Value([5]u8){ .data = [_]u8{ 1, 2, 3, 4, 5 } };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value([5]u8).fromBytes(bytes, allocator);
        try expect(std.mem.eql(u8, &val.data, &restored.data));
    }

    {
        const val = Value([3]i32){ .data = [_]i32{ -1, 0, 1 } };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value([3]i32).fromBytes(bytes, allocator);
        try expect(std.mem.eql(i32, &val.data, &restored.data));
    }
}

// ACTUAL GOOD TESTS

test "Database: dupe key prevention" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    const db_id = try env.open("test_db");
    const db = try env.get_db(db_id);
    const txn_id = try env.begin_txn(.ReadWrite);
    const txn = try env.get_txn(txn_id);

    try db.putTyped(i32, txn, "duplicate_key", 123, allocator);

    try expectError(DatabaseError.KeyExists, db.putTyped(i32, txn, "duplicate_key", 456, allocator));

    const result = try db.getTyped(i32, txn, "duplicate_key");
    try expect(result != null);
    try expectEqual(@as(i32, 123), result.?.data);
}

test "Transaction: Basic commit flow" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    const db_id = try env.open("txn_test_db");
    const db = try env.get_db(db_id);
    const txn_id = try env.begin_txn(.ReadWrite);
    const txn = try env.get_txn(txn_id);

    try db.putTyped(i32, txn, "test_key", 42, allocator);

    try expectEqual(TransactionState.Active, txn.state);
    try env.commit_txn(txn_id);

    try expectError(DatabaseError.InvalidTransaction, env.get_txn(txn_id));
}

test "Transaction: ReadOnly transaction restrictions" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    const db_id = try env.open("readonly_test_db");
    const db = try env.get_db(db_id);

    const rw_txn_id = try env.begin_txn(.ReadWrite);
    const rw_txn = try env.get_txn(rw_txn_id);
    try db.putTyped(i32, rw_txn, "existing_key", 123, allocator);
    try env.commit_txn(rw_txn_id);

    const ro_txn_id = try env.begin_txn(.ReadOnly);
    const ro_txn = try env.get_txn(ro_txn_id);

    const result = try db.getTyped(i32, ro_txn, "existing_key");
    try expect(result != null);
    try expectEqual(@as(i32, 123), result.?.data);

    try expectError(DatabaseError.InvalidTransaction, db.putTyped(i32, ro_txn, "new_key", 456, allocator));
    try expectError(DatabaseError.InvalidTransaction, db.delete(ro_txn, "existing_key"));
}

test "Transaction: WriteOnly transaction restrictions" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    const db_id = try env.open("writeonly_test_db");
    const db = try env.get_db(db_id);
    const wo_txn_id = try env.begin_txn(.WriteOnly);
    const wo_txn = try env.get_txn(wo_txn_id);

    try db.putTyped(i32, wo_txn, "write_key", 789, allocator);

    try expectError(DatabaseError.InvalidTransaction, db.get(wo_txn, "write_key"));
    try expectError(DatabaseError.InvalidTransaction, db.getTyped(i32, wo_txn, "write_key"));
}

test "Transaction: Basic abort functionality" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    const db_id = try env.open("abort_test_db");
    const db = try env.get_db(db_id);
    const txn_id = try env.begin_txn(.ReadWrite);
    const txn = try env.get_txn(txn_id);

    try db.putTyped(i32, txn, "abort_key", 555, allocator);

    const before_abort = try db.getTyped(i32, txn, "abort_key");
    try expect(before_abort != null);
    try expectEqual(@as(i32, 555), before_abort.?.data);

    try env.abort_txn(txn, db_id);
    try expectEqual(TransactionState.Aborted, txn.state);
}

test "Transaction: Undo insert operation" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    const db_id = try env.open("undo_insert_db");
    const db = try env.get_db(db_id);

    const setup_txn_id = try env.begin_txn(.ReadWrite);
    const setup_txn = try env.get_txn(setup_txn_id);
    try db.putTyped(i32, setup_txn, "existing", 100, allocator);
    try env.commit_txn(setup_txn_id);

    const undo_txn_id = try env.begin_txn(.ReadWrite);
    const undo_txn = try env.get_txn(undo_txn_id);

    try db.putTyped(i32, undo_txn, "undo_key", 999, allocator);

    const before_undo = try db.getTyped(i32, undo_txn, "undo_key");
    try expect(before_undo != null);
    try expectEqual(@as(i32, 999), before_undo.?.data);

    try env.abort_txn(undo_txn, db_id);
    undo_txn.deinit();
    _ = env.transactions.remove(undo_txn_id);

    const check_txn_id = try env.begin_txn(.ReadOnly);
    const check_txn = try env.get_txn(check_txn_id);
    defer {
        check_txn.deinit();
        _ = env.transactions.remove(check_txn_id);
    }

    const existing_result = try db.getTyped(i32, check_txn, "existing");
    try expect(existing_result != null);
    try expectEqual(@as(i32, 100), existing_result.?.data);

    const undone_result = try db.getTyped(i32, check_txn, "undo_key");
    try expect(undone_result == null);
}

test "Database: Delete non-existent key" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    const db_id = try env.open("delete_nonexistent_db");
    const db = try env.get_db(db_id);
    const txn_id = try env.begin_txn(.ReadWrite);
    const txn = try env.get_txn(txn_id);

    try expectError(DatabaseError.NotFound, db.delete(txn, "nonexistent_key"));
}

test "Database: Basic delete operation" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    const db_id = try env.open("delete_test_db");
    const db = try env.get_db(db_id);
    const txn_id = try env.begin_txn(.ReadWrite);
    const txn = try env.get_txn(txn_id);

    try db.putTyped(i32, txn, "delete_me", 777, allocator);
    try db.putTyped(i32, txn, "keep_me", 888, allocator);

    var result = try db.getTyped(i32, txn, "delete_me");
    try expect(result != null);
    result = try db.getTyped(i32, txn, "keep_me");
    try expect(result != null);

    try db.delete(txn, "delete_me");

    result = try db.getTyped(i32, txn, "delete_me");
    try expect(result == null);

    result = try db.getTyped(i32, txn, "keep_me");
    try expect(result != null);
    try expectEqual(@as(i32, 888), result.?.data);
}
