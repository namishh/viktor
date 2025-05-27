const std = @import("std");
const DatabaseError = @import("errors.zig").DatabaseError;
const Value = @import("value.zig").Value;

pub const MAX_KEYS_PER_PAGE = 1024;
pub const PAGE_SIZE = 64 * 1024;

const PageHeader = struct {
    page_id: u32,
    parent_id: u32,
    is_leaf: bool,
    key_count: u32,
    prev: u32,
    next: u32,
    is_root: bool = false,
    height: u32 = 0,
};

pub const Page = struct {
    header: PageHeader,
    keys: [][]const u8,
    values: [][]const u8,
    children: []u32,

    const Self = @This();
    const MIN_KEYS = MAX_KEYS_PER_PAGE / 2;

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
                .is_root = page_id == 1,
                .height = 0,
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

    pub fn findInsertPosition(self: *const Self, key: []const u8) usize {
        var pos: usize = 0;
        while (pos < self.header.key_count) {
            const cmp = std.mem.order(u8, self.keys[pos], key);
            if (cmp == .gt) break;
            pos += 1;
        }
        return pos;
    }

    pub fn isFull(self: *const Self) bool {
        return self.header.key_count >= MAX_KEYS_PER_PAGE;
    }

    pub fn isUnderflow(self: *const Self) bool {
        return self.header.key_count < MIN_KEYS and !self.header.is_root;
    }

    pub fn canLendKey(self: *const Self) bool {
        return self.header.key_count > MIN_KEYS;
    }

    pub fn insert(self: *Self, allocator: *std.mem.Allocator, key: []const u8, value: []const u8) !void {
        if (self.search(key)) |index| {
            allocator.free(self.values[index]);
            self.values[index] = try allocator.dupe(u8, value);
        } else {
            if (self.isFull()) {
                return error.PageFull;
            }
            const pos = self.findInsertPosition(key);
            var i = self.header.key_count;
            while (i > pos) {
                self.keys[i] = self.keys[i - 1];
                self.values[i] = self.values[i - 1];
                if (!self.header.is_leaf) {
                    self.children[i + 1] = self.children[i];
                }
                i -= 1;
            }
            self.keys[pos] = try allocator.dupe(u8, key);
            self.values[pos] = try allocator.dupe(u8, value);
            self.header.key_count += 1;
        }
    }

    pub fn remove(self: *Self, allocator: std.mem.Allocator, key: []const u8) !void {
        if (self.search(key)) |index| {
            allocator.free(self.keys[index]);
            allocator.free(self.values[index]);

            var i = index;
            while (i < self.header.key_count - 1) {
                self.keys[i] = self.keys[i + 1];
                self.values[i] = self.values[i + 1];
                if (!self.header.is_leaf) {
                    self.children[i + 1] = self.children[i + 2];
                }
                i += 1;
            }

            self.header.key_count -= 1;
        }
    }

    pub fn split(self: *Self, allocator: std.mem.Allocator, new_page_id: u32) !Self {
        const mid = self.header.key_count / 2;
        var new_page = try Self.init(allocator, new_page_id, self.header.is_leaf);

        for (mid..self.header.key_count, 0..) |i, j| {
            new_page.keys[j] = try allocator.dupe(u8, self.keys[i]);
            new_page.values[j] = try allocator.dupe(u8, self.values[i]);
            allocator.free(self.keys[i]);
            allocator.free(self.values[i]);
        }

        if (!self.header.is_leaf) {
            for (mid + 1..self.header.key_count + 1, 0..) |i, j| {
                new_page.children[j] = self.children[i];
            }
        }

        new_page.header.key_count = self.header.key_count - mid;
        self.header.key_count = mid;

        if (self.header.is_leaf) {
            new_page.header.next = self.header.next;
            new_page.header.prev = self.header.page_id;
            self.header.next = new_page.header.page_id;
        }

        return new_page;
    }

    pub fn merge(self: *Self, allocator: std.mem.Allocator, sibling: *Self, separator_key: []const u8) !void {
        if (!self.header.is_leaf) {
            self.keys[self.header.key_count] = try allocator.dupe(u8, separator_key);
            self.values[self.header.key_count] = try allocator.dupe(u8, "");
            self.header.key_count += 1;
        }

        for (0..sibling.header.key_count, self.header.key_count..) |i, j| {
            self.keys[j] = sibling.keys[i];
            self.values[j] = sibling.values[i];
        }

        if (!self.header.is_leaf) {
            for (0..sibling.header.key_count + 1, self.header.key_count..) |i, j| {
                self.children[j] = sibling.children[i];
            }
        }

        self.header.key_count += sibling.header.key_count;

        if (self.header.is_leaf) {
            self.header.next = sibling.header.next;
        }
    }

    pub fn redistributeFromLeft(self: *Self, allocator: std.mem.Allocator, left_sibling: *Self, separator_key: []const u8) ![]const u8 {
        var i = self.header.key_count;
        while (i > 0) {
            self.keys[i] = self.keys[i - 1];
            self.values[i] = self.values[i - 1];
            if (!self.header.is_leaf) {
                self.children[i + 1] = self.children[i];
            }
            i -= 1;
        }
        if (!self.header.is_leaf) {
            self.children[1] = self.children[0];
        }

        if (self.header.is_leaf) {
            const last_idx = left_sibling.header.key_count - 1;
            self.keys[0] = left_sibling.keys[last_idx];
            self.values[0] = left_sibling.values[last_idx];
            left_sibling.header.key_count -= 1;
        } else {
            self.keys[0] = try allocator.dupe(u8, separator_key);
            self.values[0] = try allocator.dupe(u8, "");
            const last_idx = left_sibling.header.key_count - 1;
            self.children[0] = left_sibling.children[last_idx + 1];
            allocator.free(left_sibling.keys[last_idx]);
            allocator.free(left_sibling.values[last_idx]);
            left_sibling.header.key_count -= 1;
        }

        self.header.key_count += 1;
        return try allocator.dupe(u8, left_sibling.keys[left_sibling.header.key_count - 1]);
    }

    pub fn redistributeFromRight(self: *Self, allocator: std.mem.Allocator, right_sibling: *Self, separator_key: []const u8) ![]const u8 {
        if (self.header.is_leaf) {
            self.keys[self.header.key_count] = right_sibling.keys[0];
            self.values[self.header.key_count] = right_sibling.values[0];

            for (0..right_sibling.header.key_count - 1) |i| {
                right_sibling.keys[i] = right_sibling.keys[i + 1];
                right_sibling.values[i] = right_sibling.values[i + 1];
            }
            right_sibling.header.key_count -= 1;
        } else {
            self.keys[self.header.key_count] = try allocator.dupe(u8, separator_key);
            self.values[self.header.key_count] = try allocator.dupe(u8, "");
            self.children[self.header.key_count + 1] = right_sibling.children[0];

            allocator.free(right_sibling.keys[0]);
            allocator.free(right_sibling.values[0]);

            for (0..right_sibling.header.key_count - 1) |i| {
                right_sibling.keys[i] = right_sibling.keys[i + 1];
                right_sibling.values[i] = right_sibling.values[i + 1];
                right_sibling.children[i] = right_sibling.children[i + 1];
            }
            right_sibling.children[right_sibling.header.key_count - 1] = right_sibling.children[right_sibling.header.key_count];
            right_sibling.header.key_count -= 1;
        }

        self.header.key_count += 1;
        return try allocator.dupe(u8, self.keys[self.header.key_count - 1]);
    }
};
