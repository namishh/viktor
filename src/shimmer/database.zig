const std = @import("std");
const DatabaseError = @import("errors.zig").DatabaseError;
const Value = @import("value.zig").Value;
const Page = @import("page.zig").Page;
const LockManager = @import("locking.zig").LockManager;
const Transaction = @import("transaction.zig").Transaction;
const UndoEntry = @import("transaction.zig").UndoEntry;

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
    sync_on_commit: bool = true,
    file_path: []const u8,
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

        var db = Self{
            .id = id,
            .name = try allocator.dupe(u8, name),
            .root_page = 1,
            .file_path = try allocator.dupe(u8, name),
            .pages = pages,
            .next_page_id = 2,
            .lock_manager = lm,
            .allocator = allocator,
        };

        try loadFromDisk(&db);

        return db;
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.name);

        self.allocator.free(self.file_path);

        var page_iter = self.pages.iterator();
        while (page_iter.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }

        self.lock_manager.deinit();

        self.pages.deinit();
    }

    pub fn saveToDisk(self: *Self) !void {
        const file = std.fs.cwd().createFile(self.file_path, .{}) catch {
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

        if (self.sync_on_commit) {
            try file.sync();
        }
    }

    fn loadFromDisk(self: *Self) !void {
        const file = std.fs.cwd().openFile(self.file_path, .{}) catch {
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

    fn insertIntoPage(self: *Self, txn: *Transaction, page_id: u32, key: []const u8, value: []const u8) !void {
        var page = self.pages.getPtr(page_id) orelse return DatabaseError.InvalidDatabase;

        try self.lock_manager.lockPage(txn.id, page_id, .X);

        if (page.header.is_leaf) {
            if (!page.isFull()) {
                try page.insert(&self.allocator, key, value);
                try txn.dirty_pages.append(page_id);
            } else {
                const new_page_id = self.next_page_id;
                self.next_page_id += 1;
                var new_page = try page.split(self.allocator, new_page_id);
                try self.pages.put(new_page_id, new_page);

                if (std.mem.order(u8, key, page.keys[page.header.key_count - 1]) == .gt) {
                    try new_page.insert(&self.allocator, key, value);
                    try txn.dirty_pages.append(new_page_id);
                } else {
                    try page.insert(&self.allocator, key, value);
                    try txn.dirty_pages.append(page_id);
                }

                const promoted_key = try self.allocator.dupe(u8, new_page.keys[0]);
                if (page.header.is_root) {
                    const new_root_id = self.next_page_id;
                    self.next_page_id += 1;
                    var new_root = try Page.init(self.allocator, new_root_id, false);
                    new_root.header.is_root = true;
                    new_root.children[0] = page_id;
                    new_root.children[1] = new_page_id;
                    try new_root.insert(&self.allocator, promoted_key, "");
                    self.root_page = new_root_id;
                    try self.pages.put(new_root_id, new_root);
                    page.header.is_root = false;
                    page.header.parent_id = new_root_id;
                    new_page.header.parent_id = new_root_id;
                    try txn.dirty_pages.append(new_root_id);
                } else {
                    try self.insertIntoPage(txn, page.header.parent_id, promoted_key, "");
                }
            }
        } else {
            const pos = page.findInsertPosition(key);
            const child_id = page.children[pos];
            try self.insertIntoPage(txn, child_id, key, value);
        }
    }

    pub fn put(self: *Self, txn: *Transaction, key: []const u8, value: []const u8) !void {
        if (!txn.is_active) return DatabaseError.InvalidTransaction;
        if (txn.txn_type == .ReadOnly) return DatabaseError.InvalidTransaction;

        const root_page = self.pages.getPtr(self.root_page) orelse return DatabaseError.InvalidDatabase;

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

        try self.insertIntoPage(txn, self.root_page, key, value);
    }

    pub fn putTyped(self: *Self, comptime T: type, txn: *Transaction, key: []const u8, value: T, allocator: std.mem.Allocator) !void {
        const value_wrapper = Value(T){ .data = value, .allocator = allocator };
        const bytes = try value_wrapper.convertToBytes(&allocator);
        defer allocator.free(bytes);
        try self.put(txn, key, bytes);
    }

    fn deleteFromPage(self: *Self, txn: *Transaction, page_id: u32, key: []const u8) !void {
        var page = self.pages.getPtr(page_id) orelse return DatabaseError.InvalidDatabase;

        try self.lock_manager.lockPage(txn.id, page_id, .X);

        if (page.header.is_leaf) {
            if (page.search(key)) |_| {
                try page.remove(self.allocator, key);
                try txn.dirty_pages.append(page_id);

                if (page.isUnderflow() and !page.header.is_root) {
                    var parent = self.pages.getPtr(page.header.parent_id) orelse return DatabaseError.InvalidDatabase;
                    var sibling_id: ?u32 = null;
                    var sibling_pos: ?usize = null;
                    var separator_pos: usize = 0;

                    for (parent.children[0 .. parent.header.key_count + 1], 0..) |child_id, i| {
                        if (child_id == page_id) {
                            if (i > 0) {
                                sibling_id = parent.children[i - 1];
                                separator_pos = i - 1;
                            } else if (i < parent.header.key_count) {
                                sibling_id = parent.children[i + 1];
                                separator_pos = i;
                            }
                            sibling_pos = i;
                            break;
                        }
                    }

                    if (sibling_id) |sid| {
                        var sibling = self.pages.getPtr(sid) orelse return DatabaseError.InvalidDatabase;
                        const separator_key = parent.keys[separator_pos];

                        if (sibling.canLendKey()) {
                            const new_separator = if (sibling_pos.? > 0)
                                try page.redistributeFromLeft(self.allocator, sibling, separator_key)
                            else
                                try page.redistributeFromRight(self.allocator, sibling, separator_key);
                            self.allocator.free(parent.keys[separator_pos]);
                            parent.keys[separator_pos] = new_separator;
                            try txn.dirty_pages.append(sid);
                            try txn.dirty_pages.append(parent.header.page_id);
                        } else {
                            if (sibling_pos.? > 0) {
                                try sibling.merge(self.allocator, page, separator_key);
                                parent.children[sibling_pos.?] = sid;
                            } else {
                                try page.merge(self.allocator, sibling, separator_key);
                            }
                            self.allocator.free(parent.keys[separator_pos]);
                            for (separator_pos..parent.header.key_count - 1) |i| {
                                parent.keys[i] = parent.keys[i + 1];
                                parent.children[i + 1] = parent.children[i + 2];
                            }
                            parent.header.key_count -= 1;
                            try txn.dirty_pages.append(parent.header.page_id);

                            if (parent.isUnderflow()) {
                                try self.deleteFromPage(txn, parent.header.parent_id, separator_key);
                            }
                        }
                    }
                }
            } else {
                return DatabaseError.NotFound;
            }
        } else {
            const pos = page.findInsertPosition(key);
            const child_id = page.children[pos];
            try self.deleteFromPage(txn, child_id, key);
        }
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

            try self.deleteFromPage(txn, self.root_page, key);
        } else {
            return DatabaseError.NotFound;
        }
    }
};
