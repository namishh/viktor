// shimmer - a very simple key value store
const std = @import("std");
const DatabaseError = @import("errors.zig").DatabaseError;
const Value = @import("value.zig").Value;

pub const MAX_KEYS_PER_PAGE = 1024;
pub const PAGE_SIZE = 64 * 1024;

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

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, page_id: u32, is_leaf: bool) !Self {
        const keys = try allocator.alloc([]const u8, MAX_KEYS_PER_PAGE);
        const values = try allocator.alloc([]const u8, MAX_KEYS_PER_PAGE);
        const children = try allocator.alloc(u32, MAX_KEYS_PER_PAGE + 1);

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

pub const LockType = enum {
    Shared, // multiple readers can hold this lock. S
    Exclusive, // only one writer can hold this lock. X
    IntentShared, // a transaction intends to read. IS
    IntentExclusive, // a transaction intends to write. IX
    SharedIntentExclusive, // a transaction intends to read and write. SIX
};

pub const LockMode = enum { S, X, IS, IX, SIX, None };

const LOCK_COMPATIBILITY = [_][6]bool{
    //       None  IS    IX    S     SIX   X
    [_]bool{ true, true, true, true, true, true }, // None
    [_]bool{ true, true, true, true, true, false }, // IS
    [_]bool{ true, true, true, false, false, false }, // IX
    [_]bool{ true, true, false, true, false, false }, // S
    [_]bool{ true, true, false, false, false, false }, // SIX
    [_]bool{ true, false, false, false, false, false }, // X
};

pub const ResourceType = enum {
    Database,
    Page,
    Record,
};

pub const LockRequest = struct {
    transaction_id: u32,
    resource_id: u64,
    resource_type: ResourceType,
    lock_mode: LockMode,
    granted: bool = false,
    timestamp: i64,

    const Self = @This();

    pub fn init(txn_id: u32, res_id: u64, res_type: ResourceType, mode: LockMode) Self {
        return Self{
            .transaction_id = txn_id,
            .resource_id = res_id,
            .resource_type = res_type,
            .lock_mode = mode,
            .timestamp = std.time.milliTimestamp(),
        };
    }
};

pub const DeadlockDetector = struct {
    wait_graph: std.HashMap(u32, std.ArrayList(u32), std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage),
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .wait_graph = std.HashMap(u32, std.ArrayList(u32), std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        var iter = self.wait_graph.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.wait_graph.deinit();
    }

    pub fn addEdge(self: *Self, from: u32, to: u32) !void {
        const result = try self.wait_graph.getOrPut(from);
        if (!result.found_existing) {
            result.value_ptr.* = std.ArrayList(u32).init(self.allocator);
        }
        try result.value_ptr.append(to);
    }

    pub fn removeEdge(self: *Self, from: u32, to: u32) void {
        if (self.wait_graph.getPtr(from)) |edges| {
            for (edges.items, 0..) |edge, i| {
                if (edge == to) {
                    _ = edges.swapRemove(i);
                    break;
                }
            }
        }
    }

    pub fn detectCycle(self: *Self) ?u32 {
        var visited = std.HashMap(u32, bool, std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage).init(self.allocator);
        defer visited.deinit();

        var rec_stack = std.HashMap(u32, bool, std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage).init(self.allocator);
        defer rec_stack.deinit();

        var iter = self.wait_graph.iterator();
        while (iter.next()) |entry| {
            const node = entry.key_ptr.*;
            if (!visited.contains(node)) {
                if (self.dfsDetectCycle(node, &visited, &rec_stack)) |victim| {
                    return victim;
                }
            }
        }
        return null;
    }

    fn dfsDetectCycle(self: *Self, node: u32, visited: *std.HashMap(u32, bool, std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage), rec_stack: *std.HashMap(u32, bool, std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage)) ?u32 {
        visited.put(node, true) catch return null;
        rec_stack.put(node, true) catch return null;

        if (self.wait_graph.get(node)) |edges| {
            for (edges.items) |neighbor| {
                if (!visited.contains(neighbor)) {
                    if (self.dfsDetectCycle(neighbor, visited, rec_stack)) |victim| {
                        return victim;
                    }
                } else if (rec_stack.contains(neighbor)) {
                    return if (node > neighbor) node else neighbor;
                }
            }
        }

        rec_stack.put(node, false) catch {};
        return null;
    }
};

pub const LockManager = struct {
    lock_table: std.HashMap(u64, std.ArrayList(LockRequest), std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage),
    transaction_locks: std.HashMap(u32, std.ArrayList(u64), std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage),
    deadlock_detector: DeadlockDetector,
    mutex: std.Thread.Mutex = .{},
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .lock_table = std.HashMap(u64, std.ArrayList(LockRequest), std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage).init(allocator),
            .transaction_locks = std.HashMap(u32, std.ArrayList(u64), std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage).init(allocator),
            .deadlock_detector = DeadlockDetector.init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        var lock_iter = self.lock_table.iterator();
        while (lock_iter.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.lock_table.deinit();

        var txn_iter = self.transaction_locks.iterator();
        while (txn_iter.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.transaction_locks.deinit();
        self.deadlock_detector.deinit();
    }

    fn isCompatible(mode1: LockMode, mode2: LockMode) bool {
        const idx1 = @intFromEnum(mode1);
        const idx2 = @intFromEnum(mode2);
        return LOCK_COMPATIBILITY[idx1][idx2];
    }

    pub fn acquireLock(self: *Self, txn_id: u32, resource_id: u64, resource_type: ResourceType, mode: LockMode, timeout_ms: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.lock_table.get(resource_id)) |requests| {
            for (requests.items) |*request| {
                if (request.transaction_id == txn_id and request.granted) {
                    if (canUpgrade(request.lock_mode, mode)) {
                        request.lock_mode = mode;
                        return;
                    } else if (request.lock_mode == mode) {
                        return;
                    }
                }
            }
        }

        var new_request = LockRequest.init(txn_id, resource_id, resource_type, mode);

        var can_grant = true;
        if (self.lock_table.get(resource_id)) |requests| {
            for (requests.items) |*existing| {
                if (existing.granted and existing.transaction_id != txn_id) {
                    if (!isCompatible(existing.lock_mode, mode)) {
                        can_grant = false;
                        try self.deadlock_detector.addEdge(txn_id, existing.transaction_id);
                        break;
                    }
                }
            }
        }

        if (can_grant) {
            new_request.granted = true;
            try self.addLockToTables(resource_id, new_request, txn_id);
        } else {
            if (self.deadlock_detector.detectCycle()) |victim_txn| {
                if (victim_txn == txn_id) {
                    return DatabaseError.DeadlockDetected;
                }
                try self.abortTransaction(victim_txn);
                new_request.granted = true;
                try self.addLockToTables(resource_id, new_request, txn_id);
            } else {
                try self.addLockToTables(resource_id, new_request, txn_id);
                std.time.sleep(timeout_ms * std.time.ns_per_ms);
                return DatabaseError.LockTimeout;
            }
        }
    }

    fn canUpgrade(current: LockMode, requested: LockMode) bool {
        return switch (current) {
            .IS => requested == .S or requested == .X or requested == .IX or requested == .SIX,
            .IX => requested == .X or requested == .SIX,
            .S => requested == .X or requested == .SIX,
            else => false,
        };
    }

    fn addLockToTables(self: *Self, resource_id: u64, request: LockRequest, txn_id: u32) !void {
        const lock_result = try self.lock_table.getOrPut(resource_id);
        if (!lock_result.found_existing) {
            lock_result.value_ptr.* = std.ArrayList(LockRequest).init(self.allocator);
        }
        try lock_result.value_ptr.append(request);

        if (request.granted) {
            const txn_result = try self.transaction_locks.getOrPut(txn_id);
            if (!txn_result.found_existing) {
                txn_result.value_ptr.* = std.ArrayList(u64).init(self.allocator);
            }
            try txn_result.value_ptr.append(resource_id);
        }
    }

    pub fn releaseLock(self: *Self, txn_id: u32, resource_id: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.lock_table.getPtr(resource_id)) |requests| {
            for (requests.items, 0..) |*request, i| {
                if (request.transaction_id == txn_id and request.granted) {
                    _ = requests.swapRemove(i);
                    break;
                }
            }

            try self.processWaitQueue(resource_id);
        }

        if (self.transaction_locks.getPtr(txn_id)) |txn_locks| {
            for (txn_locks.items, 0..) |res_id, i| {
                if (res_id == resource_id) {
                    _ = txn_locks.swapRemove(i);
                    break;
                }
            }
        }
    }

    fn processWaitQueue(self: *Self, resource_id: u64) !void {
        if (self.lock_table.getPtr(resource_id)) |requests| {
            var granted_any = false;

            for (requests.items) |*request| {
                if (!request.granted) {
                    var can_grant = true;

                    for (requests.items) |*existing| {
                        if (existing.granted and existing.transaction_id != request.transaction_id) {
                            if (!isCompatible(existing.lock_mode, request.lock_mode)) {
                                can_grant = false;
                                break;
                            }
                        }
                    }

                    if (can_grant) {
                        request.granted = true;
                        granted_any = true;

                        const txn_result = try self.transaction_locks.getOrPut(request.transaction_id);
                        if (!txn_result.found_existing) {
                            txn_result.value_ptr.* = std.ArrayList(u64).init(self.allocator);
                        }
                        try txn_result.value_ptr.append(resource_id);

                        for (requests.items) |*other| {
                            if (other.granted and other.transaction_id != request.transaction_id) {
                                self.deadlock_detector.removeEdge(request.transaction_id, other.transaction_id);
                            }
                        }
                    }
                }
            }

            if (granted_any) {
                try self.processWaitQueue(resource_id);
            }
        }
    }

    pub fn releaseAllLocks(self: *Self, txn_id: u32) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.transaction_locks.get(txn_id)) |txn_locks| {
            const locks_copy = try self.allocator.dupe(u64, txn_locks.items);
            defer self.allocator.free(locks_copy);

            for (locks_copy) |resource_id| {
                try self.releaseLockInternal(txn_id, resource_id);
            }
        }

        if (self.transaction_locks.getPtr(txn_id)) |txn_locks| {
            txn_locks.deinit();
            _ = self.transaction_locks.remove(txn_id);
        }
    }

    fn releaseLockInternal(self: *Self, txn_id: u32, resource_id: u64) !void {
        if (self.lock_table.getPtr(resource_id)) |requests| {
            for (requests.items, 0..) |*request, i| {
                if (request.transaction_id == txn_id and request.granted) {
                    _ = requests.swapRemove(i);
                    try self.processWaitQueue(resource_id);
                    break;
                }
            }
        }
    }

    pub fn abortTransaction(self: *Self, txn_id: u32) !void {
        try self.releaseAllLocks(txn_id);

        var iter = self.lock_table.iterator();
        while (iter.next()) |entry| {
            const requests = entry.value_ptr;
            for (requests.items, 0..) |*request, i| {
                if (request.transaction_id == txn_id and !request.granted) {
                    _ = requests.swapRemove(i);
                    break;
                }
            }
        }
    }

    pub fn lockPage(self: *Self, txn_id: u32, page_id: u32, mode: LockMode) !void {
        const resource_id = (@as(u64, @intFromEnum(ResourceType.Page)) << 32) | page_id;
        try self.acquireLock(txn_id, resource_id, .Page, mode, 5000);
    }

    pub fn lockRecord(self: *Self, txn_id: u32, page_id: u32, key_hash: u32, mode: LockMode) !void {
        const resource_id = (@as(u64, page_id) << 32) | key_hash;
        try self.acquireLock(txn_id, resource_id, .Record, mode, 5000);
    }

    pub fn lockDatabase(self: *Self, txn_id: u32, db_id: u32, mode: LockMode) !void {
        const resource_id = (@as(u64, @intFromEnum(ResourceType.Database)) << 32) | db_id;
        try self.acquireLock(txn_id, resource_id, .Database, mode, 10000); // 10 second timeout
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

        try self.lock_manager.lockPage(txn.id, self.root_page, .S);
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
        try self.lock_manager.lockPage(txn.id, self.root_page, .X);
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
