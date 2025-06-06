const std = @import("std");
const DatabaseError = @import("errors.zig").DatabaseError;
const Database = @import("database.zig").Database;
const TransactionType = @import("transaction.zig").TransactionType;
const Transaction = @import("transaction.zig").Transaction;
const Page = @import("page.zig").Page;

// the most simple file in the entirety of the shimmer module, just think of it is as an wrapper around the database and transaction structs

pub const TimeLoggingCategory = enum { Database, Transaction, Locking };
pub var time_logging_mutex: std.Thread.Mutex = .{};

pub fn formatDuration(duration_ns: f128) struct { value: f128, unit: []const u8 } {
    const abs_duration = @abs(duration_ns);

    if (abs_duration >= 1_000_000.0) {
        return .{ .value = abs_duration / 1_000_000.0, .unit = "ms" };
    } else if (abs_duration >= 1_000.0) {
        return .{ .value = abs_duration / 1_000.0, .unit = "us" };
    } else {
        return .{ .value = abs_duration, .unit = "ns" };
    }
}

pub const Environment = struct {
    databases: std.HashMap(u32, Database, std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage),
    transactions: std.HashMap(u32, Transaction, std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage),
    next_db_id: u32,
    next_txn_id: u32,
    allocator: std.mem.Allocator,
    time_logging_categories: std.EnumSet(TimeLoggingCategory) = std.EnumSet(TimeLoggingCategory).initEmpty(),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) !Self {
        return Self{
            .databases = std.HashMap(u32, Database, std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage).init(allocator),
            .transactions = std.HashMap(u32, Transaction, std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage).init(allocator),
            .next_db_id = 1,
            .next_txn_id = 1,
            .allocator = allocator,
        };
    }

    pub fn set_time_logging(self: *Self, enable: bool, categories: []const TimeLoggingCategory) void {
        if (enable) {
            self.time_logging_categories = std.EnumSet(TimeLoggingCategory).initEmpty();
            for (categories) |cat| {
                self.time_logging_categories.insert(cat);
            }
        } else {
            self.time_logging_categories = std.EnumSet(TimeLoggingCategory).initEmpty();
        }
    }

    pub fn deinit(self: *Self) void {
        var db_iter = self.databases.iterator();
        while (db_iter.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.databases.deinit();

        var txn_iter = self.transactions.iterator();
        while (txn_iter.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.transactions.deinit();
    }

    pub fn open(self: *Self, name: []const u8) !u32 {
        const db_id = self.next_db_id;
        self.next_db_id += 1;

        const db = try Database.init(self.allocator, db_id, name, self.time_logging_categories);
        try self.databases.put(db_id, db);

        return db_id;
    }

    pub fn begin_txn(self: *Self, tx_type: TransactionType) !u32 {
        const start_time = std.time.nanoTimestamp();
        defer {
            if (self.time_logging_categories.contains(TimeLoggingCategory.Transaction)) {
                const end_time = std.time.nanoTimestamp();
                const duration = formatDuration(@as(f128, @floatFromInt(end_time - start_time)));
                time_logging_mutex.lock();
                defer time_logging_mutex.unlock();
                std.debug.print("TIME[TXN]: begin transaction: {d:.3}{s}\n", .{ duration.value, duration.unit });
            }
        }
        const txn_id = self.next_txn_id;
        self.next_txn_id += 1;

        const txn = Transaction.init(self.allocator, txn_id, tx_type);
        try self.transactions.put(txn_id, txn);

        return txn_id;
    }

    pub fn get_txn(self: *Self, txn_id: u32) !*Transaction {
        return self.transactions.getPtr(txn_id) orelse DatabaseError.InvalidTransaction;
    }

    pub fn get_db(self: *Self, db_id: u32) !*Database {
        return self.databases.getPtr(db_id) orelse DatabaseError.InvalidDatabase;
    }

    pub fn commit_txn(self: *Self, txn_id: u32) !void {
        const start_time = std.time.nanoTimestamp();
        defer {
            if (self.time_logging_categories.contains(TimeLoggingCategory.Transaction)) {
                const end_time = std.time.nanoTimestamp();
                const duration = formatDuration(@as(f128, @floatFromInt(end_time - start_time)));
                time_logging_mutex.lock();
                defer time_logging_mutex.unlock();
                std.debug.print("TIME[TXN]: commit transaction {}: {d:.3}{s}\n", .{ txn_id, duration.value, duration.unit });
            }
        }
        var txn = try self.get_txn(txn_id);

        for (txn.dirty_pages.items) |_| {
            var db_iter = self.databases.iterator();
            while (db_iter.next()) |entry| {
                const db = entry.value_ptr;
                try db.saveToDisk();
            }
            break;
        }

        try txn.commit();
        txn.deinit();
        _ = self.transactions.remove(txn_id);
    }

    pub fn abort_txn(self: *Self, txn: *Transaction, db_id: u32) !void {
        if (txn.state != .Active) return DatabaseError.TransactionNotActive;

        const db = self.databases.getPtr(db_id) orelse return DatabaseError.InvalidDatabase;
        var root_page = db.pages.getPtr(db.root_page) orelse return DatabaseError.InvalidDatabase;

        const start_time = std.time.nanoTimestamp();
        defer {
            if (self.time_logging_categories.contains(TimeLoggingCategory.Transaction)) {
                const end_time = std.time.nanoTimestamp();
                const duration = formatDuration(@as(f128, @floatFromInt(end_time - start_time)));
                time_logging_mutex.lock();
                defer time_logging_mutex.unlock();
                std.debug.print("TIME[TXN]: abort transaction {}: {d:.3}{s}\n", .{ txn.*.id, duration.value, duration.unit });
            }
        }

        while (txn.undo_log.pop()) |entry| {
            switch (entry.operation) {
                .Insert => {
                    if (root_page.search(entry.key)) |index| {
                        self.allocator.free(root_page.keys[index]);
                        self.allocator.free(root_page.values[index]);
                        var j = index;
                        while (j < root_page.header.key_count - 1) {
                            root_page.keys[j] = root_page.keys[j + 1];
                            root_page.values[j] = root_page.values[j + 1];
                            j += 1;
                        }
                        root_page.header.key_count -= 1;
                    }
                },
                .Update => {
                    if (entry.old_value) |old_val| {
                        if (root_page.search(entry.key)) |index| {
                            self.allocator.free(root_page.values[index]);
                            root_page.values[index] = try self.allocator.dupe(u8, old_val);
                        }
                    }
                },
                .Delete => {
                    if (entry.old_value) |old_val| {
                        var pos: usize = 0;
                        while (pos < root_page.header.key_count) {
                            const cmp = std.mem.order(u8, root_page.keys[pos], entry.key);
                            if (cmp == .gt) break;
                            pos += 1;
                        }
                        var j = root_page.header.key_count;
                        while (j > pos) {
                            root_page.keys[j] = root_page.keys[j - 1];
                            root_page.values[j] = root_page.values[j - 1];
                            j -= 1;
                        }
                        root_page.keys[pos] = try self.allocator.dupe(u8, entry.key);
                        root_page.values[pos] = try self.allocator.dupe(u8, old_val);
                        root_page.header.key_count += 1;
                    }
                },
            }

            self.allocator.free(entry.table_name);
            self.allocator.free(entry.key);
            if (entry.old_value) |old_val| {
                self.allocator.free(old_val);
            }
        }

        txn.state = .Aborted;
    }
};
