// shimmer - a very simple key value store
const std = @import("std");

pub const MAX_KEYS_PER_PAGE = 8192;
pub const PAGE_SIZE = 64 * 8192;

pub const DatabaseError = error{ KeyExists, NotFound, DiskWriteError, InvalidDataType, InvalidDatabase, InvalidTransaction, InvalidSize, TransactionNotActive, InvalidKey };

// serialization code taken from https://github.com/ziglibs/s2s/

fn AlignedInt(comptime T: type) type {
    return std.math.ByteAlignedInt(T);
}

pub fn Value(comptime T: type) type {
    return struct {
        data: T,
        const Self = @This();

        fn serializeRecursive(stream: anytype, comptime T2: type, value: T2) @TypeOf(stream).Error!void {
            switch (@typeInfo(T2)) {
                .void => {},
                .bool => try stream.writeByte(@intFromBool(value)),
                .float => switch (T2) {
                    f16 => try stream.writeInt(u16, @bitCast(value), .little),
                    f32 => try stream.writeInt(u32, @bitCast(value), .little),
                    f64 => try stream.writeInt(u64, @bitCast(value), .little),
                    f80 => try stream.writeInt(u80, @bitCast(value), .little),
                    f128 => try stream.writeInt(u128, @bitCast(value), .little),
                    else => unreachable,
                },
                .int => {
                    if (T2 == usize) {
                        try stream.writeInt(u64, value, .little);
                    } else {
                        try stream.writeInt(AlignedInt(T2), value, .little);
                    }
                },
                .pointer => |ptr| {
                    switch (ptr.size) {
                        .one => try serializeRecursive(stream, ptr.child, value.*),
                        .slice => {
                            try stream.writeInt(u64, value.len, .little);
                            if (ptr.child == u8) {
                                try stream.writeAll(value);
                            } else {
                                for (value) |item| {
                                    try serializeRecursive(stream, ptr.child, item);
                                }
                            }
                        },
                        .c => unreachable,
                        .many => unreachable,
                    }
                },
                .@"struct" => |str| {
                    inline for (str.fields) |fld| {
                        try serializeRecursive(stream, fld.type, @field(value, fld.name));
                    }
                },
                else => return DatabaseError.InvalidDataType,
            }
        }

        fn recursiveDeserialize(
            stream: anytype,
            comptime T2: type,
            allocator: ?std.mem.Allocator,
            target: *T2,
        ) (@TypeOf(stream).Error || error{ UnexpectedData, OutOfMemory, EndOfStream })!void {
            switch (@typeInfo(T2)) {
                .void => target.* = {},
                .bool => target.* = (try stream.readByte()) != 0,
                .float => target.* = @bitCast(switch (T2) {
                    f16 => try stream.readInt(u16, .little),
                    f32 => try stream.readInt(u32, .little),
                    f64 => try stream.readInt(u64, .little),
                    f80 => try stream.readInt(u80, .little),
                    f128 => try stream.readInt(u128, .little),
                    else => unreachable,
                }),
                .int => target.* = if (T2 == usize)
                    std.math.cast(usize, try stream.readInt(u64, .little)) orelse return error.UnexpectedData
                else
                    @truncate(try stream.readInt(AlignedInt(T2), .little)),
                .pointer => |ptr| {
                    switch (ptr.size) {
                        .one => {
                            const pointer = try allocator.?.create(ptr.child);
                            errdefer allocator.?.destroy(pointer);
                            try recursiveDeserialize(stream, ptr.child, allocator, pointer);
                            target.* = pointer;
                        },
                        .slice => {
                            const length = std.math.cast(usize, try stream.readInt(u64, .little)) orelse return error.UnexpectedData;
                            const slice = try allocator.?.alloc(ptr.child, length);
                            errdefer allocator.?.free(slice);
                            if (ptr.child == u8) {
                                try stream.readNoEof(slice);
                            } else {
                                for (slice) |*item| {
                                    try recursiveDeserialize(stream, ptr.child, allocator, item);
                                }
                            }
                            target.* = slice;
                        },
                        .c => unreachable,
                        .many => unreachable,
                    }
                },
                .@"struct" => |str| {
                    inline for (str.fields) |fld| {
                        try recursiveDeserialize(stream, fld.type, allocator, &@field(target.*, fld.name));
                    }
                },
                else => return DatabaseError.InvalidDataType,
            }
        }

        pub fn convertToBytes(self: *const Self, allocator: *const std.mem.Allocator) ![]u8 {
            var list = std.ArrayList(u8).init(allocator.*);
            defer list.deinit();

            try serializeRecursive(list.writer(), T, self.data);
            return try allocator.dupe(u8, list.items);
        }

        pub fn fromBytes(bytes: []const u8, allocator: std.mem.Allocator) !Self {
            var stream = std.io.fixedBufferStream(bytes);
            var result: T = undefined;
            try recursiveDeserialize(stream.reader(), T, allocator, &result);
            return Self{ .data = result };
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
            if (cmp == .eq) {
                allocator.free(self.values[pos]);
                self.values[pos] = try allocator.dupe(u8, value);
                return;
            }
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

pub const TransactionType = enum { ReadOnly, WriteOnly, ReadWrite };

pub const TransactionState = enum { Active, Committed, Aborted };

pub const UndoEntry = struct {
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
        for (self.undo_log.items) |entry| {
            self.allocator.free(entry.table_name);
            self.allocator.free(entry.key);
            if (entry.old_value) |old_val| {
                self.allocator.free(old_val);
            }
        }

        self.dirty_pages.deinit();
        self.undo_log.deinit();
        self.is_active = false;
    }

    pub fn commit(self: *Self) !void {
        if (self.state != .Active) return DatabaseError.TransactionNotActive;

        for (self.undo_log.items) |entry| {
            self.allocator.free(entry.table_name);
            self.allocator.free(entry.key);
            if (entry.old_value) |old_val| {
                self.allocator.free(old_val);
            }
        }

        self.state = .Committed;
        self.undo_log.clearAndFree();
    }

    pub fn abort(self: *Self) !void {
        if (self.state != .Active) return DatabaseError.TransactionNotActive;

        for (self.undo_log.items) |entry| {
            self.allocator.free(entry.table_name);
            self.allocator.free(entry.key);
            if (entry.old_value) |old_val| {
                self.allocator.free(old_val);
            }
        }

        self.state = .Aborted;
        self.undo_log.clearAndFree();
    }
};

pub const DiskConfig = struct {
    enabled: bool = false,
    file_path: ?[]const u8 = null,
    sync_on_commit: bool = true,
};

const SerializedPage = struct {
    page_id: u32,
    parent_id: u32,
    is_leaf: bool,
    key_count: u32,
    prev: u32,
    next: u32,
    keys: [][]const u8,
    values: [][]const u8,
};

const DatabaseData = struct {
    id: u32,
    name: []const u8,
    root_page: u32,
    next_page_id: u32,
    pages: []SerializedPage,
};

pub const Database = struct {
    id: u32,
    name: []const u8,
    root_page: u32,
    pages: std.HashMap(u32, Page, std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage),
    next_page_id: u32,
    disk_config: DiskConfig = .{},
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

        if (self.disk_config.file_path) |path| {
            self.allocator.free(path);
        }

        var page_iter = self.pages.iterator();
        while (page_iter.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }

        self.pages.deinit();
    }

    pub fn enableDiskStorage(self: *Self, file_path: []const u8, sync_on_commit: bool) !void {
        if (self.disk_config.enabled) return DatabaseError.InvalidDatabase;
        self.disk_config = DiskConfig{
            .enabled = true,
            .file_path = try self.allocator.dupe(u8, file_path),
            .sync_on_commit = sync_on_commit,
        };

        try self.loadFromDisk();
    }

    pub fn saveToDisk(self: *Self) !void {
        if (!self.disk_config.enabled or self.disk_config.file_path == null) return;

        const file = std.fs.cwd().createFile(self.disk_config.file_path.?, .{}) catch {
            return DatabaseError.DiskWriteError;
        };
        defer file.close();

        const serialized_pages = try self.allocator.alloc(SerializedPage, self.pages.count());
        defer self.allocator.free(serialized_pages);

        var page_iter = self.pages.iterator();
        var i: usize = 0;
        while (page_iter.next()) |entry| {
            const page = entry.value_ptr;
            serialized_pages[i] = SerializedPage{
                .page_id = page.header.page_id,
                .parent_id = page.header.parent_id,
                .is_leaf = page.header.is_leaf,
                .key_count = page.header.key_count,
                .prev = page.header.prev,
                .next = page.header.next,
                .keys = page.keys[0..page.header.key_count],
                .values = page.values[0..page.header.key_count],
            };
            i += 1;
        }

        const db_data = DatabaseData{
            .id = self.id,
            .name = self.name,
            .root_page = self.root_page,
            .next_page_id = self.next_page_id,
            .pages = serialized_pages,
        };

        const value_wrapper = Value(DatabaseData){ .data = db_data };
        const bytes = try value_wrapper.convertToBytes(&self.allocator);
        defer self.allocator.free(bytes);

        try file.writeAll(bytes);

        if (self.disk_config.sync_on_commit) {
            try file.sync();
        }
    }

    fn loadFromDisk(self: *Self) !void {
        if (!self.disk_config.enabled or self.disk_config.file_path == null) return;

        const file = std.fs.cwd().openFile(self.disk_config.file_path.?, .{}) catch {
            return;
        };
        defer file.close();

        const file_size = try file.getEndPos();
        const bytes = try self.allocator.alloc(u8, file_size);
        defer self.allocator.free(bytes);

        _ = try file.readAll(bytes);

        const value_wrapper = try Value(DatabaseData).fromBytes(bytes, self.allocator);
        const db_data = value_wrapper.data;
        defer {
            self.allocator.free(db_data.name);
            for (db_data.pages) |page| {
                for (page.keys) |key| {
                    self.allocator.free(key);
                }
                for (page.values) |value| {
                    self.allocator.free(value);
                }
                self.allocator.free(page.keys);
                self.allocator.free(page.values);
            }
            self.allocator.free(db_data.pages);
        }

        for (db_data.pages) |serialized_page| {
            if (self.pages.getPtr(serialized_page.page_id)) |current_page| {
                for (serialized_page.keys, serialized_page.values) |key, value| {
                    if (current_page.search(key) == null) {
                        try current_page.insert(&self.allocator, key, value);
                    }
                }
            } else {
                var new_page = try Page.init(self.allocator, serialized_page.page_id, serialized_page.is_leaf);
                new_page.header.parent_id = serialized_page.parent_id;
                new_page.header.prev = serialized_page.prev;
                new_page.header.next = serialized_page.next;

                for (serialized_page.keys, serialized_page.values) |key, value| {
                    try new_page.insert(&self.allocator, key, value);
                }

                try self.pages.put(serialized_page.page_id, new_page);
            }
        }

        if (db_data.next_page_id > self.next_page_id) {
            self.next_page_id = db_data.next_page_id;
        }
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

        const existing_value = if (root_page.search(key)) |idx|
            try self.allocator.dupe(u8, root_page.values[idx])
        else
            null;

        if (existing_value) |old_val| {
            try txn.undo_log.append(UndoEntry{
                .operation = .Update,
                .table_name = try self.allocator.dupe(u8, "default"),
                .key = try self.allocator.dupe(u8, key),
                .old_value = old_val,
            });
        } else {
            try txn.undo_log.append(UndoEntry{
                .operation = .Insert,
                .table_name = try self.allocator.dupe(u8, "default"),
                .key = try self.allocator.dupe(u8, key),
                .old_value = null,
            });
        }

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
            try txn.undo_log.append(UndoEntry{
                .operation = .Delete,
                .table_name = try self.allocator.dupe(u8, "default"),
                .key = try self.allocator.dupe(u8, key),
                .old_value = try self.allocator.dupe(u8, root_page.values[index]),
            });

            self.allocator.free(root_page.keys[index]);
            self.allocator.free(root_page.values[index]);

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

pub const Environment = struct {
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

        for (txn.dirty_pages.items) |_| {
            var db_iter = self.databases.iterator();
            while (db_iter.next()) |entry| {
                const db = entry.value_ptr;
                if (db.disk_config.enabled) {
                    try db.saveToDisk();
                }
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
