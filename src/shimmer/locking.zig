const std = @import("std");
const DatabaseError = @import("errors.zig").DatabaseError;

pub const LockType = enum {
    Shared,
    Exclusive,
    IntentShared,
    IntentExclusive,
    SharedIntentExclusive,
};

pub const LockMode = enum { S, X, IS, IX, SIX, None };

const LOCK_COMPATIBILITY = [_][6]bool{
    //       None  IS    IX    S     SIX   X
    [_]bool{ true, true, true, true, true, true },
    [_]bool{ true, true, true, true, true, false },
    [_]bool{ true, true, true, false, false, false },
    [_]bool{ true, true, false, true, false, false },
    [_]bool{ true, true, false, false, false, false },
    [_]bool{ true, false, false, false, false, false },
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
        try self.acquireLock(txn_id, resource_id, .Database, mode, 10000);
    }
};
