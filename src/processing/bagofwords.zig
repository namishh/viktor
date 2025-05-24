const std = @import("std");

pub const BagOfWords = struct {
    vocabulary: std.StringHashMap(u32),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) !BagOfWords {
        return BagOfWords{
            .allocator = allocator,
            .vocabulary = std.StringHashMap(u32).init(allocator),
        };
    }

    pub fn fit(self: *BagOfWords, documents: []const []const u8) !void {
        for (documents) |document| {
            var words = std.mem.tokenizeAny(u8, document, " \t\n");
            while (words.next()) |word| {
                const result = try self.vocabulary.getOrPut(word);
                if (!result.found_existing) {
                    result.value_ptr.* = 1;
                } else {
                    result.value_ptr.* += 1;
                }
            }
        }
    }

    pub fn transform(self: *BagOfWords, document: []const u8) ![]u32 {
        var words = std.mem.tokenizeAny(u8, document, " \t\n");
        var vector = try self.allocator.alloc(u32, self.vocabulary.count());
        @memset(vector, 0);

        var word_counts = std.StringHashMap(u32).init(self.allocator);
        defer word_counts.deinit();

        while (words.next()) |word| {
            const result = try word_counts.getOrPut(word);
            if (!result.found_existing) {
                result.value_ptr.* = 1;
            } else {
                result.value_ptr.* += 1;
            }
        }

        var vocab_iter = self.vocabulary.iterator();
        var index: u32 = 0;
        while (vocab_iter.next()) |entry| {
            if (word_counts.get(entry.key_ptr.*)) |count| {
                vector[index] = count;
            }
            index += 1;
        }

        return vector;
    }

    pub fn transformMany(self: *BagOfWords, documents: []const []const u8) ![][]u32 {
        var vectors = try self.allocator.alloc([]u32, documents.len);
        for (documents, 0..) |document, i| {
            vectors[i] = try self.transform(document);
        }
        return vectors;
    }

    pub fn transformManyDynamic(self: *BagOfWords, documents: std.ArrayList([]const u8)) ![][]u32 {
        return self.transformMany(documents.items);
    }

    pub fn deinit(self: *BagOfWords) void {
        self.vocabulary.deinit();
    }
};
