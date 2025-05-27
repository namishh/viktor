const std = @import("std");
const DatabaseError = @import("errors.zig").DatabaseError;
const Value = @import("value.zig").Value;
const Page = @import("page.zig").Page;
const LockManager = @import("locking.zig").LockManager;
const Transaction = @import("transaction.zig").Transaction;
const UndoEntry = @import("transaction.zig").UndoEntry;

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
    immutable: bool = true,
    disk_config: DiskConfig = .{},
    lock_manager: LockManager,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn setImmutable(self: *Self, immutable: bool) void {
        self.immutable = immutable;
    }

    pub fn init(allocator: std.mem.Allocator, id: u32, name: []const u8) !Self {
        var pages = std.HashMap(u32, Page, std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage).init(allocator);

        const root_page = try Page.init(allocator, 1, true);
        try pages.put(1, root_page);

        const lm = LockManager.init(allocator);

        return Self{
            .id = id,
            .name = try allocator.dupe(u8, name),
            .root_page = 1,
            .pages = pages,
            .next_page_id = 2,
            .lock_manager = lm,
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

        self.lock_manager.deinit();

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

        const value_wrapper = Value(DatabaseData){ .data = db_data, .allocator = self.allocator };
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

        try self.lock_manager.lockPage(txn.id, self.root_page, .S);
        return null;
    }

    pub fn getTyped(self: *Self, comptime T: type, txn: *Transaction, key: []const u8) !?Value(T) {
        if (!txn.is_active or txn.state != .Active) return DatabaseError.InvalidTransaction;
        if (txn.txn_type == .WriteOnly) return DatabaseError.InvalidTransaction;

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

        if (self.immutable and root_page.search(key) != null) {
            return DatabaseError.KeyExists;
        }

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
        try self.lock_manager.lockPage(txn.id, self.root_page, .X);
    }

    pub fn putTyped(self: *Self, comptime T: type, txn: *Transaction, key: []const u8, value: T, allocator: std.mem.Allocator) !void {
        const value_wrapper = Value(T){ .data = value, .allocator = allocator };
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

            try root_page.remove(self.allocator, key);
            try txn.dirty_pages.append(self.root_page);
        } else {
            return DatabaseError.NotFound;
        }
        try self.lock_manager.lockPage(txn.id, self.root_page, .X);
    }
};
