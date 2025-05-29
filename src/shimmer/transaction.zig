const std = @import("std");
const DatabaseError = @import("errors.zig").DatabaseError;

pub const TransactionType = enum { ReadOnly, WriteOnly, ReadWrite };

// transactions.zig is the core implementation of the `Transaction` struct, which is used in a higher level in database.zig

// TODO:
// right now the flushing and undoing of transactions is implemented at a higher level in the database.zig file.
// the future goal is to implement it here

// All the possile states a transaction can be in.
// active - transaction is currently being processed
// committed - transaction has been successfully completed
// aborted - transaction has been rolled back due to an error or explicit abort
pub const TransactionState = enum { Active, Committed, Aborted };

// Represents a single entry in the undo log, which records the information needed to undo an operation.
pub const UndoEntry = struct {
    operation: enum { Insert, Update, Delete },
    table_name: []const u8,
    key: []const u8,
    old_value: ?[]const u8,
};

// transactions are implemented to ensure data consistency and reliability even in the face of concurrent access and
// potential system failures.
// this is the basic transactoin structure. it contains
//      transaction_id:unique identifier for the transaction
//      txn_type: type of transaction (read-only, write-only, read-write)
//      dirty_pages: list of pages that have been modified during the transaction
//      allocator: memory allocator used for managing transaction resources
//      is_active: flag indicating whether the transaction is currently active
//      state: current state of the transaction (active, committed, aborted)
//      undo_log: a log of operations that can be used to undo changes made by the transaction
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
            .is_active = true, // initially, the transaction is active
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

    // when the operation in a transaction have been completed, the transaction can be committed.
    pub fn commit(self: *Self) !void {
        if (self.state != .Active) return DatabaseError.TransactionNotActive;

        // free the undo log entries as they are no longer needed
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

    // for now, this is the same as commit, but instead of committing, it sets the state to aborted
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
