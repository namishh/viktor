// man this is getting out of hand
const std = @import("std");

const DatabaseError = error{ KeyExists, NotFound, InvalidDataType, InvalidDatabase, InvalidTransaction, InvalidSize };

// comptime struct for storing a value
pub fn Value(comptime T: type) type {
    return struct {
        data: T,
        const Self = @This();

        pub fn convertToBytes(self: *const Self, allocator: *std.mem.Allocator) ![]u8 {
            return switch (@typeInfo(T)) {
                .int, .float => {
                    const bytes = try allocator.alloc(u8, @sizeOf(T));
                    std.mem.writeInt(std.meta.Int(.unsigned, @sizeOf(T) * 8), bytes[0..@sizeOf(T)], @bitCast(self.data), .little);
                    return bytes;
                },
                .bool => {
                    const bytes = try allocator.alloc(u8, 1);
                    bytes[0] = if (self.data) 1 else 0;
                    return bytes;
                },
                .Array => |info| {
                    if (info.child == u8) {
                        return try allocator.dupe(u8, &self.data);
                    } else {
                        const bytes = try allocator.alloc(u8, @sizeOf(T));
                        @memcpy(bytes, std.mem.asBytes(&self.data));
                        return bytes;
                    }
                },
            };
        }

        pub fn fromBytes(bytes: []const u8) !Self {
            return switch (@typeInfo(T)) {
                .int, .float => {
                    if (bytes.len != @sizeOf(T)) return error.InvalidSize;
                    const int_val = std.mem.readInt(std.meta.Int(.unsigned, @sizeOf(T) * 8), bytes[0..@sizeOf(T)], .little);
                    return Self{ .data = @bitCast(int_val) };
                },
                .bool => {
                    if (bytes.len != 1) return error.InvalidSize;
                    return Self{ .data = bytes[0] != 0 };
                },
                .Array => |info| {
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
    children: [][]u32,
    data: []u8,

    const Self = @This();

    pub fn init(page_id: u32, is_leaf: bool) Self {
        return Self{
            .header = PageHeader{
                .page_id = page_id,
                .parent_id = 0,
                .is_leaf = is_leaf,
                .key_count = 0,
                .next_page = 0,
                .prev_page = 0,
            },
            .keys = std.mem.zeroes([][]const u8),
            .values = std.mem.zeroes([][]const u8),
            .children = std.mem.zeroes([]u32),
            .data = std.mem.zeroes([]u8),
        };
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

pub const Transaction = struct {
    id: u32,
    txn_type: TransactionType,
    dirty_pages: std.ArrayList(u32),
    allocator: std.mem.Allocator,
    is_active: bool,

    const Self = @This();

    pub fn init(allocator: std.meme.Allocator, id: u32, tx_type: TransactionType) Self {
        return Self{
            .id = id,
            .txn_type = tx_type,
            .dirty_pages = std.ArrayList(u32).init(allocator),
            .allocator = allocator,
            .is_active = true,
        };
    }

    pub fn deinit(self: *Self) void {
        self.dirty_pages.deinit();
        self.is_active = false;
    }

    pub fn commit(self: *Self) !void {
        if (!self.is_active) return DatabaseError.InvalidTransaction;
        // In a real implementation, this would flush dirty pages to disk
        self.dirty_pages.clearRetainingCapacity();
    }

    pub fn abort(self: *Self) !void {
        if (!self.is_active) return DatabaseError.InvalidTransaction;
        // In a real implementation, this would revert dirty pages
        self.dirty_pages.clearRetainingCapacity();
        self.is_active = false;
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

        const root_page = Page.init(1, true);
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
            return Value(T).fromBytes(v);
        }
        return null;
    }

    pub fn put(self: *Self, txn: *Transaction, key: []const u8, value: []const u8) !void {
        if (!txn.is_active) return DatabaseError.InvalidTransaction;
        if (txn.txn_type == .ReadOnly) return DatabaseError.InvalidTransaction;

        var root_page = self.pages.getPtr(self.root_page) orelse return DatabaseError.InvalidDatabase;

        try root_page.insert(self.allocator, key, value);
        try txn.dirty_pages.append(self.root_page);
    }

    pub fn putTyped(self: *Self, comptime T: type, txn: *Transaction, key: []const u8, value: T, allocator: std.mem.Allocator) !void {
        const value_wrapper = Value(T){ .data = value };
        const bytes = try value_wrapper.toBytes(allocator);
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

    pub fn open(self: *Self, name: []const u8) !Database {
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

    pub fn abort_txn(self: *Self, txn_id: u32) !void {
        var txn = try self.get_txn(txn_id);
        try txn.abort();
        txn.deinit();
        _ = self.transactions.remove(txn_id);
    }
};

test "Basic DATABASE" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env = try Environment.init(allocator);
    defer env.deinit();
}
