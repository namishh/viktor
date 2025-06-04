const std = @import("std");
const Environment = @import("environment.zig").Environment;
const Database = @import("database.zig").Database;
const Transaction = @import("transaction.zig").Transaction;
const Value = @import("value.zig").Value;
const TransactionState = @import("transaction.zig").TransactionState;

const DatabaseError = @import("errors.zig").DatabaseError;

const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const expectError = testing.expectError;
const expectEqualStrings = testing.expectEqualStrings;

fn setupTestEnvironment(allocator: std.mem.Allocator) !Environment {
    var env = try Environment.init(allocator);
    env.set_time_logging(true, &.{ .Database, .Transaction, .Locking });
    return env;
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
    const test_start = std.time.nanoTimestamp();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    {
        const start = std.time.nanoTimestamp();
        const val = Value(i32){ .data = -123456789, .allocator = allocator };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value(i32).fromBytes(bytes, allocator);
        const duration = std.time.nanoTimestamp() - start;
        std.debug.print("Time taken for i32 conversion: {} ns\n", .{duration});
        try expectEqual(@as(i32, -123456789), restored.data);
    }

    {
        const start = std.time.nanoTimestamp();
        const val = Value(i64){ .data = -9223372036854775807, .allocator = allocator };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value(i64).fromBytes(bytes, allocator);
        const duration = std.time.nanoTimestamp() - start;
        std.debug.print("Time taken for i64 conversion: {} ns\n", .{duration});
        try expectEqual(@as(i64, -9223372036854775807), restored.data);
    }

    const test_end = std.time.nanoTimestamp();
    const total_time = test_end - test_start;
    std.debug.print("Total time for test: {} ns\n", .{total_time});
}

test "Value: unsigned integer types" {
    const test_start = std.time.nanoTimestamp();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    {
        const start = std.time.nanoTimestamp();
        const val = Value(u32){ .data = 4294967295, .allocator = allocator };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value(u32).fromBytes(bytes, allocator);
        const duration = std.time.nanoTimestamp() - start;
        std.debug.print("Time taken for u32 conversion: {} ns\n", .{duration});
        try expectEqual(@as(u32, 4294967295), restored.data);
    }

    {
        const start = std.time.nanoTimestamp();
        const val = Value(u64){ .data = 18446744073709551615, .allocator = allocator };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value(u64).fromBytes(bytes, allocator);
        const duration = std.time.nanoTimestamp() - start;
        std.debug.print("Time taken for u64 conversion: {} ns\n", .{duration});
        try expectEqual(@as(u64, 18446744073709551615), restored.data);
    }

    const test_end = std.time.nanoTimestamp();
    const total_time = test_end - test_start;
    std.debug.print("Total time for test: {} ns\n", .{total_time});
}

test "Value: float types" {
    const test_start = std.time.nanoTimestamp();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    {
        const start = std.time.nanoTimestamp();
        const val = Value(f32){ .data = 3.14159, .allocator = allocator };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value(f32).fromBytes(bytes, allocator);
        const duration = std.time.nanoTimestamp() - start;
        std.debug.print("Time taken for f32 conversion: {} ns\n", .{duration});
        try expect(@abs(restored.data - 3.14159) < 0.0001);
    }

    {
        const start = std.time.nanoTimestamp();
        const val = Value(f64){ .data = 2.718281828459045, .allocator = allocator };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value(f64).fromBytes(bytes, allocator);
        const duration = std.time.nanoTimestamp() - start;
        std.debug.print("Time taken for f64 conversion: {} ns\n", .{duration});
        try expect(@abs(restored.data - 2.718281828459045) < 0.000000000001);
    }

    const test_end = std.time.nanoTimestamp();
    const total_time = test_end - test_start;
    std.debug.print("Total time for test: {} ns\n", .{total_time});
}

test "Value: boolean type" {
    const test_start = std.time.nanoTimestamp();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    {
        const start = std.time.nanoTimestamp();
        const val = Value(bool){ .data = true, .allocator = allocator };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value(bool).fromBytes(bytes, allocator);
        const duration = std.time.nanoTimestamp() - start;
        std.debug.print("Time taken for bool true conversion: {} ns\n", .{duration});
        try expectEqual(true, restored.data);
    }

    {
        const start = std.time.nanoTimestamp();
        const val = Value(bool){ .data = false, .allocator = allocator };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value(bool).fromBytes(bytes, allocator);
        const duration = std.time.nanoTimestamp() - start;
        std.debug.print("Time taken for bool false conversion: {} ns\n", .{duration});
        try expectEqual(false, restored.data);
    }

    const test_end = std.time.nanoTimestamp();
    const total_time = test_end - test_start;
    std.debug.print("Total time for test: {} ns\n", .{total_time});
}

test "Value: array types" {
    const test_start = std.time.nanoTimestamp();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    {
        const start = std.time.nanoTimestamp();
        const val = Value([5]u8){ .data = [_]u8{ 1, 2, 3, 4, 5 }, .allocator = allocator };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value([5]u8).fromBytes(bytes, allocator);
        const duration = std.time.nanoTimestamp() - start;
        std.debug.print("Time taken for [5]u8 conversion: {} ns\n", .{duration});
        try expect(std.mem.eql(u8, &val.data, &restored.data));
    }

    {
        const start = std.time.nanoTimestamp();
        const val = Value([3]i32){ .data = [_]i32{ -1, 0, 1 }, .allocator = allocator };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value([3]i32).fromBytes(bytes, allocator);
        const duration = std.time.nanoTimestamp() - start;
        std.debug.print("Time taken for [3]i32 conversion: {} ns\n", .{duration});
        try expect(std.mem.eql(i32, &val.data, &restored.data));
    }

    const test_end = std.time.nanoTimestamp();
    const total_time = test_end - test_start;
    std.debug.print("Total time for test: {} ns\n", .{total_time});
}

test "Value: struct type" {
    const test_start = std.time.nanoTimestamp();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const TestStruct2 = struct {
        id: i32,
        name: []const u8,
        value: f64,
        data: [10]u8,
    };

    {
        const start = std.time.nanoTimestamp();
        const val = Value(TestStruct2){
            .data = TestStruct2{
                .id = 42,
                .name = "test_name",
                .value = 3.14,
                .data = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 },
            },
            .allocator = allocator,
        };
        const bytes = try val.convertToBytes(&allocator);
        defer allocator.free(bytes);
        const restored = try Value(TestStruct2).fromBytes(bytes, allocator);
        defer restored.deinit();
        const duration = std.time.nanoTimestamp() - start;
        std.debug.print("Time taken for TestStruct conversion: {} ns\n", .{duration});

        try expectEqual(42, restored.data.id);
        try expectEqualStrings("test_name", restored.data.name);
        try expectEqual(3.14, restored.data.value);
        try expect(std.mem.eql(u8, &val.data.data, &restored.data.data));
    }

    const test_end = std.time.nanoTimestamp();
    const total_time = test_end - test_start;
    std.debug.print("Total time for TestStruct test: {} ns\n", .{total_time});
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

test "immutable database behavior" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    const db_id = try env.open("test_db");
    var db = try env.get_db(db_id);
    db.setImmutable(true);

    // test 1: immutable database (default) should reject duplicate keys
    {
        const txn_id = try env.begin_txn(.ReadWrite);
        const txn = try env.get_txn(txn_id);

        try db.putTyped([]const u8, txn, "key1", "value1", allocator);

        const result = db.putTyped([]const u8, txn, "key1", "value2", allocator);
        try testing.expectError(DatabaseError.KeyExists, result);

        try env.commit_txn(txn_id);
    }

    // test 2: mutable database should allow duplicate keys (updates)
    {
        db.setImmutable(false);

        const txn_id = try env.begin_txn(.ReadWrite);
        const txn = try env.get_txn(txn_id);

        try db.putTyped([]const u8, txn, "key2", "value2", allocator);
        try db.putTyped([]const u8, txn, "key2", "updated_value2", allocator);

        const retrieved = try db.getTyped([]const u8, txn, "key2");
        try testing.expect(retrieved != null);
        try testing.expectEqualStrings("updated_value2", retrieved.?.data);
        defer if (retrieved) |r| allocator.free(r.data);

        try env.commit_txn(txn_id);
    }

    // test 3: switch back to immutable and test again
    {
        db.setImmutable(true);

        const txn_id = try env.begin_txn(.ReadWrite);
        const txn = try env.get_txn(txn_id);

        try db.putTyped([]const u8, txn, "key3", "value3", allocator);

        const result = db.putTyped([]const u8, txn, "key3", "new_value3", allocator);
        try testing.expectError(DatabaseError.KeyExists, result);

        try env.commit_txn(txn_id);
    }
}

test "basic shared lock compatibility" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    const db_id = try env.open("test_db");
    var db = try env.get_db(db_id);

    const txn1_id = try env.begin_txn(.ReadOnly);
    const txn2_id = try env.begin_txn(.ReadOnly);

    try db.lock_manager.lockPage(txn1_id, 1, .S);
    try db.lock_manager.lockPage(txn2_id, 1, .S);

    try env.commit_txn(txn1_id);
    try env.commit_txn(txn2_id);
}

test "lock release allows waiting transactions" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    const db_id = try env.open("test_db");
    var db = try env.get_db(db_id);

    const txn1_id = try env.begin_txn(.ReadWrite);
    const txn2_id = try env.begin_txn(.ReadOnly);

    // first transaction gets exclusive lock
    try db.lock_manager.lockPage(txn1_id, 1, .X);

    // release the lock
    try db.lock_manager.releaseLock(txn1_id, (@as(u64, @intFromEnum(@import("main.zig").ResourceType.Page)) << 32) | 1);

    // Now second transaction should be able to get shared lock
    try db.lock_manager.lockPage(txn2_id, 1, .S);

    try env.commit_txn(txn1_id);
    try env.commit_txn(txn2_id);
}

test "transaction abort releases all locks" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    const db_id = try env.open("test_db");
    var db = try env.get_db(db_id);

    const txn1_id = try env.begin_txn(.ReadWrite);
    const txn2_id = try env.begin_txn(.ReadOnly);

    const txn1 = try env.get_txn(txn1_id);

    try db.lock_manager.lockPage(txn1_id, 1, .X);
    try db.lock_manager.lockPage(txn1_id, 2, .X);

    try env.abort_txn(txn1, db_id);

    try db.lock_manager.lockPage(txn2_id, 1, .S);
    try db.lock_manager.lockPage(txn2_id, 2, .S);

    try env.commit_txn(txn2_id);
}

test "lock upgrade from shared to exclusive" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    const db_id = try env.open("test_db");
    var db = try env.get_db(db_id);

    const txn1_id = try env.begin_txn(.ReadWrite);

    try db.lock_manager.lockPage(txn1_id, 1, .S);

    try db.lock_manager.lockPage(txn1_id, 1, .X);

    try env.commit_txn(txn1_id);
}

test "database level locking" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    const db_id = try env.open("test_db");
    var db = try env.get_db(db_id);

    const txn1_id = try env.begin_txn(.ReadWrite);
    const txn2_id = try env.begin_txn(.ReadOnly);

    try db.lock_manager.lockDatabase(txn1_id, db_id, .X);

    try env.commit_txn(txn1_id);
    try env.commit_txn(txn2_id);
}

test "record level locking" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try setupTestEnvironment(allocator);
    defer env.deinit();

    const db_id = try env.open("test_db");
    var db = try env.get_db(db_id);

    const txn1_id = try env.begin_txn(.ReadWrite);
    const txn2_id = try env.begin_txn(.ReadWrite);

    try db.lock_manager.lockRecord(txn1_id, 1, 100, .X); // key hash 100
    try db.lock_manager.lockRecord(txn2_id, 1, 200, .X); // key hash 200

    try env.commit_txn(txn1_id);
    try env.commit_txn(txn2_id);
}

const TestStruct = struct {
    id: i32,
    name: []const u8,
    value: f64,
    data: [10]u8,
};

pub fn pathExists(path: []const u8) !bool {
    const file = std.fs.cwd().openFile(path, .{}) catch |err| {
        if (err == error.FileNotFound) {
            return false;
        }
        return err;
    };
    file.close();
    return true;
}

test "Database: Insert and retrieve 1000 large objects of different types" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // delete any existing test database
    const test_db_path = "large_objects_test";
    const test_db_exists = try pathExists(test_db_path);
    if (test_db_exists) {
        try std.fs.cwd().deleteFile(test_db_path);
    }

    var env = try setupTestEnvironment(allocator);
    env.set_time_logging(true, &.{ .Database, .Transaction });
    defer env.deinit();

    const db_id = try env.open("large_objects_test");
    var db = try env.get_db(db_id);
    db.setImmutable(false);

    const txn_id = try env.begin_txn(.ReadWrite);
    const txn = try env.get_txn(txn_id);

    const num_entries = 1000;
    const insertion_start = std.time.nanoTimestamp();

    for (0..num_entries) |i| {
        const key = try std.fmt.allocPrint(allocator, "key{}", .{i});
        defer allocator.free(key);

        const hash = std.hash.Fnv1a_32.hash(key);
        const type_index = hash % 4;

        switch (type_index) {
            0 => {
                const value = try allocator.alloc(u8, 1024);
                defer allocator.free(value);
                @memset(value, 'a' + @as(u8, @intCast(i % 26)));
                try db.putTyped([]const u8, txn, key, value, allocator);
            },
            1 => {
                const value: f128 = @floatFromInt(i);
                try db.putTyped(f128, txn, key, value, allocator);
            },
            2 => {
                const value: i128 = @intCast(i);
                try db.putTyped(i128, txn, key, value, allocator);
            },
            3 => {
                const value = TestStruct{
                    .id = @intCast(i),
                    .name = try std.fmt.allocPrint(allocator, "name{}", .{i}),
                    .value = @floatFromInt(i),
                    .data = [_]u8{@intCast(i % 256)} ** 10,
                };
                defer allocator.free(value.name);
                try db.putTyped(TestStruct, txn, key, value, allocator);
            },
            else => unreachable,
        }
    }

    try env.commit_txn(txn_id);
    const insertion_end = std.time.nanoTimestamp();
    const insertion_time = insertion_end - insertion_start;
    const iff = @import("environment.zig").formatDuration(@as(f128, @floatFromInt(insertion_time)));
    std.debug.print("\nTotal insertion time for 1000 large objects: {d:.3}{s}\n", .{ iff.value, iff.unit });

    const read_txn_id = try env.begin_txn(.ReadOnly);
    const read_txn = try env.get_txn(read_txn_id);
    defer {
        read_txn.deinit();
        _ = env.transactions.remove(read_txn_id);
    }

    const retrieval_start = std.time.nanoTimestamp();

    for (0..num_entries) |i| {
        const key = try std.fmt.allocPrint(allocator, "key{}", .{i});
        defer allocator.free(key);

        const hash = std.hash.Fnv1a_32.hash(key);
        const type_index = hash % 4;

        switch (type_index) {
            0 => {
                const result = try db.getTyped([]const u8, read_txn, key);
                try expect(result != null);
                try expectEqual(@as(usize, 1024), result.?.data.len);
                try expectEqual('a' + @as(u8, @intCast(i % 26)), result.?.data[0]);
                defer if (result) |r| allocator.free(r.data);
            },
            1 => {
                const result = try db.getTyped(f128, read_txn, key);
                try expect(result != null);
                try expectEqual(@as(f64, @floatFromInt(i)), result.?.data);
            },
            2 => {
                const result = try db.getTyped(i128, read_txn, key);
                try expect(result != null);
                try expectEqual(@as(i64, @intCast(i)), result.?.data);
            },
            3 => {
                const result = try db.getTyped(TestStruct, read_txn, key);
                try expect(result != null);
                const expected = TestStruct{
                    .id = @intCast(i),
                    .name = try std.fmt.allocPrint(allocator, "name{}", .{i}),
                    .value = @floatFromInt(i),
                    .data = [_]u8{@intCast(i % 256)} ** 10,
                };
                defer allocator.free(expected.name);
                try expectEqual(expected.id, result.?.data.id);
                try expectEqualStrings(expected.name, result.?.data.name);
                try expectEqual(expected.value, result.?.data.value);
                try expect(std.mem.eql(u8, &expected.data, &result.?.data.data));
                defer if (result) |r| allocator.free(r.data.name);
            },
            else => unreachable,
        }
    }

    const retrieval_end = std.time.nanoTimestamp();
    const retrieval_time = retrieval_end - retrieval_start;
    const rf = @import("environment.zig").formatDuration(@as(f128, @floatFromInt(retrieval_time)));
    std.debug.print("Total retrieval time for 1000 large objects: {d:.3}{s}\n", .{ rf.value, rf.unit });
}
