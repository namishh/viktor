const std = @import("std");
const DatabaseError = @import("errors.zig").DatabaseError;

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
