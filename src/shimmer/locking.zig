const std = @import("std");
const DatabaseError = @import("errors.zig").DatabaseError;

const TimeLoggingCategory = @import("environment.zig").TimeLoggingCategory;
const formatDuration = @import("environment.zig").formatDuration;

pub const LockType = enum {
    Shared, // allows multiple transactions to read the resource simultaneously but prevents any transaction from modifying it
    Exclusive, // grants a transaction exclusive access to a resource,
    //            allowing it to both read and modify the resource while preventing any other transaction from accessing it.
    IntentShared, // transaction intends to acquire shared locks on some sub-resources of the current resource
    IntentExclusive, // transaction intends to acquire exclusive locks on some sub-resources of the current resource
    SharedIntentExclusive, // allowing a transaction to read the entire resource
    //                        while also indicating an intention to modify some sub-resources
};

pub const LockMode = enum { S, X, IS, IX, SIX, None };

// determines whether different lock modes can coexist on the same resource
const LOCK_COMPATIBILITY = [_][6]bool{
    //       None  IS    IX    S     SIX   X
    [_]bool{ true, true, true, true, true, true }, //      None
    [_]bool{ true, true, true, true, true, false }, //     IS
    [_]bool{ true, true, true, false, false, false }, //   IX
    [_]bool{ true, true, false, true, false, false }, //   S
    [_]bool{ true, true, false, false, false, false }, //  SIX
    [_]bool{ true, false, false, false, false, false }, // X
};

pub const ResourceType = enum {
    Database,
    Page,
    Record,
};

// structure represents a request for a lock on a resource.
// it contains
//      transaction_id: unique identifier for the transaction requesting the lock
//      resource_id: unique identifier for the resource being locked
//      resource_type: type of the resource being locked (Database, Page, Record)
//      lock_mode: the mode of the lock being requested (S, X, IS, IX, SIX)
//      granted: flag indicating whether the lock has been granted
//      timestamp: the time when the lock request was made, used for timeout and deadlock detection
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

// TODO: implemnet an actual graph instead of a hash map
// A deadlock occurs when two or more transactions are waiting for each other to release locks, creating a cycle of dependencies.
// this structure is for detecting and resolving deadlocks.
// it contains
//      wait_graph: implemented as a hash map that maps transaction IDs to lists of transaction IDs that they are waiting for
//      allocator: memory allocator used for managing the wait graph
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

    // recursively checks for cycles in the wait graph using depth-first search (DFS).
    // if a cycle is detected, it returns the transaction ID of one of the transactions involved in the cycle.
    // it maintains two hash maps:
    //      visited: keeps track of visited nodes to avoid reprocessing them
    //      rec_stack: keeps track of the recursion stack to detect cycles
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

    // recursively explores the wait-for graph starting from a given node.
    // if it encounters a node that is already in the recursion stack, it has detected a cycle (deadlock).
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

// this struct manages locks for transactions in a database system.
// it contains
//      lock_table: a hash map that maps resource IDs to lists of lock requests
//      transaction_locks: a hash map that maps transaction IDs to lists of resource IDs that the transaction holds locks on
//      deadlock_detector: an instance of DeadlockDetector to handle deadlocks
//      mutex: a mutex to ensure thread safety when acquiring and releasing locks
//      allocator: memory allocator used for managing lock requests and transaction locks
pub const LockManager = struct {
    lock_table: std.HashMap(u64, std.ArrayList(LockRequest), std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage),
    transaction_locks: std.HashMap(u32, std.ArrayList(u64), std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage),
    deadlock_detector: DeadlockDetector,
    mutex: std.Thread.Mutex = .{},
    allocator: std.mem.Allocator,
    time_logging_categories: std.EnumSet(TimeLoggingCategory),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, time_logging_categories: std.EnumSet(TimeLoggingCategory)) Self {
        return Self{
            .lock_table = std.HashMap(u64, std.ArrayList(LockRequest), std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage).init(allocator),
            .transaction_locks = std.HashMap(u32, std.ArrayList(u64), std.hash_map.AutoContext(u32), std.hash_map.default_max_load_percentage).init(allocator),
            .deadlock_detector = DeadlockDetector.init(allocator),
            .allocator = allocator,
            .time_logging_categories = time_logging_categories,
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

    // checks if two lock modes are compatible with each other using the LOCK_COMPATIBILITY matrix.
    // returns true if the modes are compatible, false otherwise.
    fn isCompatible(mode1: LockMode, mode2: LockMode) bool {
        const idx1 = @intFromEnum(mode1);
        const idx2 = @intFromEnum(mode2);
        return LOCK_COMPATIBILITY[idx1][idx2];
    }

    // acquires a lock for a transaction on a specified resource.
    // first checks  if the transaction already holds a lock on the resource, and if so, whether it can upgrade the lock mode.
    // if a new lock request is needed, it checks if the lock can be granted immediately or if it needs to wait.
    // if the lock can be granted, marks the request as granted and adds it to the lock tables
    // if the lock cannot be granted, it checks for deadlocks using the deadlock detector
    pub fn acquireLock(self: *Self, txn_id: u32, resource_id: u64, resource_type: ResourceType, mode: LockMode, timeout_ms: u64) !void {
        const start_time = std.time.nanoTimestamp();
        defer {
            if (self.time_logging_categories.contains(.Locking)) {
                const end_time = std.time.nanoTimestamp();
                const duration = formatDuration(@as(f128, @floatFromInt(end_time - start_time)));
                @import("environment.zig").time_logging_mutex.lock();
                defer @import("environment.zig").time_logging_mutex.unlock();
                std.debug.print("TIME[LOCK]: acquire lock txn {} resource {}: {d:.3}{s}\n", .{ txn_id, resource_id, duration.value, duration.unit });
            }
        }
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

    // releases a lock for a transaction on a specified resource.
    // it processes the wait queue for the resource to see if any other transactions can be granted locks.
    // and removes the resource from the transaction's lock list.
    pub fn releaseLock(self: *Self, txn_id: u32, resource_id: u64) !void {
        const start_time = std.time.nanoTimestamp();
        defer {
            if (self.time_logging_categories.contains(.Locking)) {
                const end_time = std.time.nanoTimestamp();
                const duration = formatDuration(@as(f128, @floatFromInt(end_time - start_time)));
                @import("environment.zig").time_logging_mutex.lock();
                defer @import("environment.zig").time_logging_mutex.unlock();
                std.debug.print("TIME[LOCK]: release lock txn {} resource {}: {d:.3}{s}\n", .{ txn_id, resource_id, duration.value, duration.unit });
            }
        }
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

    // reponsible for granting locks to transactions that are waiting for a resource.
    // this method checks each waiting lock request to see if it can be granted based on compatibility with existing granted locks. If a lock can be granted, it updates the lock
    // tables and removes any corresponding edges from the wait graph in the deadlock detector. The method is recursive, continuing to process the wait queue until no more
    // locks can be granted
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

    // these methods are used in database.zig
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
