const std = @import("std");
const DatabaseError = @import("errors.zig").DatabaseError;
const Value = @import("value.zig").Value;

// each page can hold a maximum of 1024 keys, and each page is 64KB in size.
pub const MAX_KEYS_PER_PAGE = 1024;
pub const PAGE_SIZE = 64 * 1024;

// B Tree is the core data structure for retrieving and storing in this key-value store.
// page.zig is the core implementation of a B-Tree, and the higher level operations are in database.zig.

// BASIC PROPERTIES OF A B-TREE:
// 1. All leaf nodes are at the same height.
// 2. Internal (non-leaf) nodes store keys and pointers to child nodes
// 3. Keys in each node are sorted in ascending order
// 4. For any internal node, all keys in the subtree rooted at child[i] are less than the key at position i
// 5. For any internal node, all keys in the subtree rooted at child[i+1] are greater than or equal to the key at position i

// PageHeader contains the metadata for a page in the B-Tree.
//      page_id: unique identifier for the page.
//      parent_id: identifier of the parent page, if any. for root page, this is 0.
//      is_leaf: indicates if the page is a leaf page (contains actual key-value pairs) or an internal page (contains keys and child pointers).
//      key_count: number of keys currently stored in the page.
//      prev: identifier of the previous page in the linked list of pages.
//      next: identifier of the next page in the linked list of pages.
//      is_root: indicates if the page is the root page of the B-Tree.
//      height: the height of the page in the B-Tree, used for balancing and traversal. 0 for leaf nodes.
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

// Page represents a single node in the B-Tree.
//      header: metadata for the page.
//      keys: array of keys stored in the page.
//      values: array of values corresponding to the keys, only used for leaf pages.
//      children: array of child page identifiers, only used for internal pages.

pub const Page = struct {
    header: PageHeader,
    keys: [][]const u8,
    values: [][]const u8,
    children: []u32,

    const Self = @This();
    const MIN_KEYS = MAX_KEYS_PER_PAGE / 2;

    // this function initializes a new page
    // it takes in
    // - allocator: the memory allocator to use for allocating the page's keys, values, and children arrays.
    // - page_id: the unique identifier for the page.
    // - is_leaf: a boolean indicating if the page is a leaf page.

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

    // deinit function frees the memory allocated for the page's keys, values, and children arrays.
    // it only takes in the allocator
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

    // CAPACITY AND STATE CHECKS

    // it returns true if the key_count is greater than or equal to MAX_KEYS_PER_PAGE.
    pub fn isFull(self: *const Self) bool {
        return self.header.key_count >= MAX_KEYS_PER_PAGE;
    }

    // it returns true if the key_count is less than MIN_KEYS and the page is not the root.
    pub fn isUnderflow(self: *const Self) bool {
        return self.header.key_count < MIN_KEYS and !self.header.is_root;
    }

    // it returns true if the key_count is greater than MIN_KEYS.
    pub fn canLendKey(self: *const Self) bool {
        return self.header.key_count > MIN_KEYS;
    }

    // TODO: can it be made more efficient?
    // this function determines where the key should be inserted in the keys array. it scans the keys array linearly to find the
    // first position where the existing key is greater than the new key.
    // it takes in the key to insert
    // returns the position where the key should be inserted.
    pub fn findInsertPosition(self: *const Self, key: []const u8) usize {
        var pos: usize = 0;
        while (pos < self.header.key_count) {
            const cmp = std.mem.order(u8, self.keys[pos], key);
            if (cmp == .gt) break;
            pos += 1;
        }
        return pos;
    }

    // B TREE OPERATIONS

    // performs basic binary search on the keys array to find the index of the key.
    // it takes in the key to search
    // returns the index of the key if found, or null if not found.
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

    // this function inserts a key-value pair into the page.
    // if the key already exists, it updates the value by freeing the old value and duplicating the new value.
    // if not, it shifts the existing keys and values to make space for the new key-value pair.
    // it takes in
    // - allocator: the memory allocator to use for allocating the new key and value.
    // - key: the key to insert.
    // - value: the value to insert.
    // it returns an error if the page is full.
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

    // this function removes a key from the page. if the key exists, it frees the memory allocated for the key and value and
    // shifts the existing keys and values to fill the gap.
    // it takes in
    // - allocator: the memory allocator to use for freeing the key and value.
    // - key: the key to remove.
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

    // B TREE OPTIMIZATIONS

    // when a page becomes full, it needs to be split into two pages. this function splits it at the midpoint, creating a new page that
    // takes upper half of the keys and values, and returns the new page.
    // for non-leaf pages, it also copies the child pointers to the new page.
    // for leaf pages, it updates the next and previous pointers to maintain the linked list of leaf pages.
    // it takes in
    // - allocator: the memory allocator to use for allocating the new page.
    // - new_page_id: the unique identifier for the new page.
    // it returns the new page if the split is successful
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

    // when the page has too few keys, after deleteion, it may need to be merged with a sibling page.
    // for leaf pages, merges the current page with a sibling page, copying the keys and values from the sibling page.
    // for non-leaf pages, it includes the separator key from the parent page to maintain the B-Tree properties.
    // it takes in
    // - allocator: the memory allocator to use for allocating the new keys and values.
    // - sibling: the sibling page to merge with.
    // - separator_key: the key from the parent page that separates the two pages.
    // it returns an error if the merge is unsuccessful.
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

    // KEY REDISTRIBUTION -> when a page has too few keys but its siblings have more than the minimum, keys can be
    // redistributed rather than merging pages

    // this function redistributes a key from the left sibling to the current page.
    // it shifts the existing keys and values to make space for the new key, and updates the header's key count.
    // for leaf pages, it takes the last key from the left sibling.
    // for non leaf, it takes a separator key from the parent moves the rightmost child pointer from the left sibling.
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

    // this function redistributes a key from the right sibling to the current page.
    // it shifts the existing keys and values to make space for the new key, and updates the header's key count.
    // for leaf pages, it takes the first key from the right sibling.
    // for non leaf, it takes a separator key from the parent moves the leftmost child pointer from the right sibling.
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
