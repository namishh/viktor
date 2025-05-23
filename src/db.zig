// man this is getting out of hand
const std = @import("std");

const MAX_KEYS_PER_PAGE = 4096;
const PAGE_SIZE = 64 * 4096;

const DatabaseError = error{ KeyExists, NotFound, InvalidDataType, InvalidDatabase, InvalidTransaction, InvalidSize, TransactionNotActive, InvalidKey };

// comptime struct for storing a value
pub fn Value(comptime T: type) type {
    return struct {
        data: T,
        const Self = @This();

        pub fn convertToBytes(self: *const Self, allocator: *const std.mem.Allocator) ![]u8 {
            return switch (@typeInfo(T)) {
                .int, .float, .comptime_int, .comptime_float => {
                    const bytes = try allocator.alloc(u8, @sizeOf(T));
                    std.mem.writeInt(std.meta.Int(.unsigned, @sizeOf(T) * 8), bytes[0..@sizeOf(T)], @bitCast(self.data), .little);
                    return bytes;
                },
                .bool => {
                    const bytes = try allocator.alloc(u8, 1);
                    bytes[0] = if (self.data) 1 else 0;
                    return bytes;
                },
                .array => |info| {
                    if (info.child == u8) {
                        return try allocator.dupe(u8, &self.data);
                    } else {
                        const bytes = try allocator.alloc(u8, @sizeOf(T));
                        @memcpy(bytes, std.mem.asBytes(&self.data));
                        return bytes;
                    }
                },
                .@"struct" => {
                    const bytes = try allocator.alloc(u8, @sizeOf(T));
                    @memcpy(bytes, std.mem.asBytes(&self.data));
                    return bytes;
                },
                .pointer => |info| {
                    if (info.child == u8 and info.size == .slice) {
                        return try allocator.dupe(u8, self.data);
                    } else if (info.size == .Slice) {
                        const total_size = self.data.len * @sizeOf(info.child);
                        const bytes = try allocator.alloc(u8, @sizeOf(usize) + total_size);
                        std.mem.writeInt(usize, bytes[0..@sizeOf(usize)], self.data.len, .little);
                        @memcpy(bytes[@sizeOf(usize)..], std.mem.sliceAsBytes(self.data));
                        return bytes;
                    } else {
                        return DatabaseError.InvalidDataType;
                    }
                },
                else => DatabaseError.InvalidDataType,
            };
        }

        pub fn fromBytes(bytes: []const u8, allocator: std.mem.Allocator) !Self {
            return switch (@typeInfo(T)) {
                .int, .float, .comptime_int, .comptime_float => {
                    if (bytes.len != @sizeOf(T)) return error.InvalidSize;
                    const int_val = std.mem.readInt(std.meta.Int(.unsigned, @sizeOf(T) * 8), bytes[0..@sizeOf(T)], .little);
                    return Self{ .data = @bitCast(int_val) };
                },
                .bool => {
                    if (bytes.len != 1) return error.InvalidSize;
                    return Self{ .data = bytes[0] != 0 };
                },
                .array => |info| {
                    if (info.child == u8) {
                        if (bytes.len != info.len) return DatabaseError.InvalidDataType;
                        var arr: T = undefined;
                        @memcpy(&arr, bytes);
                        return Self{ .data = arr };
                    } else {
                        if (bytes.len != @sizeOf(T)) return DatabaseError.InvalidDataType;
                        var data: T = undefined;
                        @memcpy(std.mem.asBytes(&data), bytes);
                        return Self{ .data = data };
                    }
                },
                .pointer => |info| {
                    if (info.child == u8 and info.size == .slice) {
                        return Self{ .data = try allocator.dupe(u8, bytes) };
                    } else if (info.size == .Slice) {
                        if (bytes.len < @sizeOf(usize)) return DatabaseError.InvalidDataType;
                        const len = std.mem.readInt(usize, bytes[0..@sizeOf(usize)], .little);
                        const element_size = @sizeOf(info.child);
                        if (bytes.len != @sizeOf(usize) + len * element_size) return DatabaseError.InvalidDataType;

                        const slice_bytes = bytes[@sizeOf(usize)..];
                        const typed_slice = std.mem.bytesAsSlice(info.child, slice_bytes);
                        return Self{ .data = try allocator.dupe(info.child, typed_slice) };
                    } else {
                        return DatabaseError.InvalidDataType;
                    }
                },
                .@"struct" => {
                    if (bytes.len != @sizeOf(T)) return DatabaseError.InvalidDataType;
                    var data: T = undefined;
                    @memcpy(std.mem.asBytes(&data), bytes);
                    return Self{ .data = data };
                },
                else => DatabaseError.InvalidDataType,
            };
        }
    };
}

const KeyValue = struct {
    key: []const u8,
    value: []const u8,
};

// simple b+ tree storage

const PageHeader = struct {
    page_id: u32,
    parent_id: u32,
    is_leaf: bool,
    key_count: u32,
    prev: u32,
    next: u32,
};

const Page = struct {
    header: PageHeader,
    keys: [][]const u8,
    values: [][]const u8,
    children: []u32,
    data: []u8,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, page_id: u32, is_leaf: bool) !Self {
        const keys = try allocator.alloc([]const u8, MAX_KEYS_PER_PAGE);
        const values = try allocator.alloc([]const u8, MAX_KEYS_PER_PAGE);
        const children = try allocator.alloc(u32, MAX_KEYS_PER_PAGE + 1);
        const data = try allocator.alloc(u8, PAGE_SIZE);

        return Self{
            .header = PageHeader{
                .page_id = page_id,
                .parent_id = 0,
                .is_leaf = is_leaf,
                .key_count = 0,
                .next = 0,
                .prev = 0,
            },
            .keys = keys,
            .values = values,
            .children = children,
            .data = data,
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        for (self.keys[0..self.header.key_count]) |key| {
            allocator.free(key);
        }
        for (self.values[0..self.header.key_count]) |value| {
            allocator.free(value);
        }

        allocator.free(self.keys);
        allocator.free(self.values);
        allocator.free(self.children);
        allocator.free(self.data);
    }

    pub fn search(self: *const Self, key: []const u8) ?usize {
        var left: usize = 0;
        var right: usize = self.header.key_count;

        while (left < right) {
            const mid = left + (right - left) / 2;
            const cmp = std.mem.order(u8, self.keys[mid], key);

            switch (cmp) {
                .eq => return mid,
                .lt => left = mid + 1,
                .gt => right = mid,
            }
        }
        return null;
    }

    pub fn insert(self: *Self, allocator: *std.mem.Allocator, key: []const u8, value: []const u8) !void {
        // finding where to insert
        var pos: usize = 0;
        while (pos < self.header.key_count) {
            const cmp = std.mem.order(u8, self.keys[pos], key);
            if (cmp == .gt) break;
            if (cmp == .eq) return DatabaseError.KeyExists;
            pos += 1;
        }

        // shifting shit
        var i = self.header.key_count;
        while (i > pos) {
            self.keys[i] = self.keys[i - 1];
            self.values[i] = self.values[i - 1];
            i -= 1;
        }

        self.keys[pos] = try allocator.dupe(u8, key);
        self.values[pos] = try allocator.dupe(u8, value);
        self.header.key_count += 1;
    }
};

// Transactions

const TransactionType = enum { ReadOnly, WriteOnly, ReadWrite };

const TransactionState = enum { Active, Committed, Aborted };

const UndoEntry = struct {
    operation: enum { Insert, Update, Delete },
    table_name: []const u8,
    key: []const u8,
    old_value: ?[]const u8,
};

pub const Transaction = struct {
    id: u32,
    txn_type: TransactionType,
    dirty_pages: std.ArrayList(u32),
    allocator: std.mem.Allocator,
    is_active: bool,
    state: TransactionState = .Active,
    undo_log: std.ArrayList(UndoEntry),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, id: u32, tx_type: TransactionType) Self {
        return Self{
            .id = id,
            .txn_type = tx_type,
            .dirty_pages = std.ArrayList(u32).init(allocator),
            .allocator = allocator,
            .undo_log = std.ArrayList(UndoEntry).init(allocator),
            .is_active = true,
        };
    }

    pub fn deinit(self: *Self) void {
        self.dirty_pages.deinit();
        self.is_active = false;
    }

    pub fn commit(self: *Self) !void {
        if (self.state != .active) return DatabaseError.TransactionNotActive;

        self.state = .Committed;
        self.undo_log.clearAndFree();
    }

    pub fn abort(self: *Self) !void {
        if (self.state != .Active) return DatabaseError.TransactionNotActive;

        self.state = .Aborted;
    }
};

pub const Database = struct {
    id: u32,
    name: []const u8,
    root_page: u32,
    pages: std.HashMap(u32, Page, std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage),
    next_page_id: u32,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, id: u32, name: []const u8) !Self {
        var pages = std.HashMap(u32, Page, std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage).init(allocator);

        const root_page = try Page.init(allocator, 1, true);
        try pages.put(1, root_page);

        return Self{
            .id = id,
            .name = try allocator.dupe(u8, name),
            .root_page = 1,
            .pages = pages,
            .next_page_id = 2,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.name);

        var page_iter = self.pages.iterator();
        while (page_iter.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }

        self.pages.deinit();
    }

    pub fn get(self: *Self, txn: *Transaction, key: []const u8) !?[]const u8 {
        if (txn.txn_type == .WriteOnly) return DatabaseError.InvalidTransaction;
        const root_page = self.pages.get(self.root_page) orelse return DatabaseError.NotFound;
        if (root_page.search(key)) |index| {
            return root_page.values[index];
        }

        return null;
    }

    pub fn getTyped(self: *Self, comptime T: type, txn: *Transaction, key: []const u8) !?Value(T) {
        const value = try self.get(txn, key);
        if (value) |v| {
            const va = try Value(T).fromBytes(v, self.allocator);
            return va;
        }
        return null;
    }

    pub fn put(self: *Self, txn: *Transaction, key: []const u8, value: []const u8) !void {
        if (!txn.is_active) return DatabaseError.InvalidTransaction;
        if (txn.txn_type == .ReadOnly) return DatabaseError.InvalidTransaction;

        var root_page = self.pages.getPtr(self.root_page) orelse return DatabaseError.InvalidDatabase;

        try root_page.insert(&self.allocator, key, value);
        try txn.dirty_pages.append(self.root_page);
    }

    pub fn putTyped(self: *Self, comptime T: type, txn: *Transaction, key: []const u8, value: T, allocator: std.mem.Allocator) !void {
        const value_wrapper = Value(T){ .data = value };
        const bytes = try value_wrapper.convertToBytes(&allocator);
        defer allocator.free(bytes);
        try self.put(txn, key, bytes);
    }

    pub fn delete(self: *Self, txn: *Transaction, key: []const u8) !void {
        if (!txn.is_active) return DatabaseError.InvalidTransaction;
        if (txn.txn_type == .ReadOnly) return DatabaseError.InvalidTransaction;

        var root_page = self.pages.getPtr(self.root_page) orelse return DatabaseError.InvalidDatabase;

        if (root_page.search(key)) |index| {
            var i = index;
            while (i < root_page.header.key_count - 1) {
                root_page.keys[i] = root_page.keys[i + 1];
                root_page.values[i] = root_page.values[i + 1];
                i += 1;
            }
            root_page.header.key_count -= 1;
            try txn.dirty_pages.append(self.root_page);
        } else {
            return DatabaseError.NotFound;
        }
    }
};

const Environment = struct {
    databases: std.HashMap(u32, Database, std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage),
    transactions: std.HashMap(u32, Transaction, std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage),
    next_db_id: u32,
    next_txn_id: u32,
    allocator: std.mem.Allocator,

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

        const db = try Database.init(self.allocator, db_id, name);
        try self.databases.put(db_id, db);

        return db_id;
    }

    pub fn begin_txn(self: *Self, tx_type: TransactionType) !u32 {
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
        var txn = try self.get_txn(txn_id);
        try txn.commit();
        txn.deinit();
        _ = self.transactions.remove(txn_id);
    }

    pub fn abort_txn(self: *Self, txn: *Transaction, db_id: u32) !void {
        if (txn.state != .Active) return DatabaseError.TransactionNotActive;

        var i = txn.undo_log.items.len;
        const db = self.databases.getPtr(db_id) orelse return DatabaseError.InvalidDatabase;
        while (i > 0) {
            i -= 1;
            const entry = txn.undo_log.items[i];

            switch (entry.operation) {
                .Insert => {
                    try db.delete(txn, entry.key);
                },
                .Update => {
                    if (entry.old_value) |old_val| {
                        try db.put(txn, entry.key, old_val);
                    }
                },
                .Delete => {
                    if (entry.old_value) |old_val| {
                        try db.put(txn, entry.key, old_val);
                    }
                },
            }
        }

        txn.state = .Aborted;
        txn.undo_log.clearAndFree();
    }
};

test "Basic DATABASE" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try Environment.init(allocator);
    defer env.deinit();

    const db_id = try env.open("test_db");
    const db = try env.get_db(db_id);
    const txn_id = try env.begin_txn(.ReadWrite);
    const txn = try env.get_txn(txn_id);
    defer env.abort_txn(txn, db_id) catch |err| {
        std.debug.print("Failed to abort transaction: {}\n", .{err});
    };
    try db.putTyped(u64, txn, "int_key", 12345678901234, allocator);

    const result = try db.getTyped(u64, txn, "int_key");
    if (result) |value| {
        std.debug.print("Value: {}\n", .{value.data});
    } else {
        std.debug.print("Key not found\n", .{});
    }
}
