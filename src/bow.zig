// not related to the vector database, but just a very simple example to conver text into a vector to feed
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

test "BagOfWords fit and vocabulary" {
    const testing = std.testing;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var bow = try BagOfWords.init(allocator);
    defer bow.deinit();

    const documents = [_][]const u8{ "hello world", "world of code", "hello code" };

    std.debug.print("\n=== Testing fit and vocabulary ===\n", .{});
    std.debug.print("Documents:\n", .{});
    for (documents, 0..) |doc, i| {
        std.debug.print("  [{}]: \"{s}\"\n", .{ i, doc });
    }

    try bow.fit(&documents);

    std.debug.print("\nVocabulary (count: {}):\n", .{bow.vocabulary.count()});
    var vocab_iter = bow.vocabulary.iterator();
    var vocab_index: u32 = 0;
    while (vocab_iter.next()) |entry| {
        std.debug.print("  [{}]: \"{s}\" -> {}\n", .{ vocab_index, entry.key_ptr.*, entry.value_ptr.* });
        vocab_index += 1;
    }

    try testing.expect(bow.vocabulary.count() == 4);
    std.debug.print("Vocabulary test passed\n", .{});
}

test "BagOfWords transform single document" {
    const testing = std.testing;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var bow = try BagOfWords.init(allocator);
    defer bow.deinit();

    const documents = [_][]const u8{ "hello world", "world of code", "hello code" };

    try bow.fit(&documents);

    std.debug.print("\n=== Testing transform single document ===\n", .{});

    const test_cases = [_][]const u8{ "hello world", "code hello", "world", "unknown word" };

    for (test_cases) |test_doc| {
        std.debug.print("\nTransforming: \"{s}\"\n", .{test_doc});
        const vector = try bow.transform(test_doc);
        defer allocator.free(vector);

        std.debug.print("Vector: [", .{});
        for (vector, 0..) |val, i| {
            if (i > 0) std.debug.print(", ", .{});
            std.debug.print("{}", .{val});
        }
        std.debug.print("]\n", .{});

        try testing.expect(vector.len == bow.vocabulary.count());
    }

    std.debug.print("Transform single document test passed\n", .{});
}

test "BagOfWords transformMany" {
    const testing = std.testing;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var bow = try BagOfWords.init(allocator);
    defer bow.deinit();

    const fit_documents = [_][]const u8{ "hello world", "world of code", "hello code" };

    try bow.fit(&fit_documents);

    std.debug.print("\nTesting transformMany\n", .{});

    const test_documents = [_][]const u8{ "hello world", "code world hello", "of code", "new words here" };

    std.debug.print("Transform documents:\n", .{});
    for (test_documents, 0..) |doc, i| {
        std.debug.print("  [{}]: \"{s}\"\n", .{ i, doc });
    }

    const vectors = try bow.transformMany(&test_documents);
    defer {
        for (vectors) |vec| {
            allocator.free(vec);
        }
        allocator.free(vectors);
    }

    std.debug.print("\nResults:\n", .{});
    for (vectors, 0..) |vec, i| {
        std.debug.print("  \"{s}\" -> [", .{test_documents[i]});
        for (vec, 0..) |val, j| {
            if (j > 0) std.debug.print(", ", .{});
            std.debug.print("{}", .{val});
        }
        std.debug.print("]\n", .{});
    }

    try testing.expect(vectors.len == test_documents.len);
    for (vectors) |vec| {
        try testing.expect(vec.len == bow.vocabulary.count());
    }

    std.debug.print("TransformMany test passed\n", .{});
}

test "BagOfWords transformManyDynamic" {
    const testing = std.testing;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var bow = try BagOfWords.init(allocator);
    defer bow.deinit();

    const fit_documents = [_][]const u8{ "hello world", "world of code", "hello code" };

    try bow.fit(&fit_documents);

    std.debug.print("\nTesting transformManyDynamic\n", .{});

    var doc_list = std.ArrayList([]const u8).init(allocator);
    defer doc_list.deinit();

    try doc_list.append("hello world");
    try doc_list.append("code world");
    try doc_list.append("hello code of world");
    try doc_list.append("unknown words");

    std.debug.print("Dynamic documents:\n", .{});
    for (doc_list.items, 0..) |doc, i| {
        std.debug.print("  [{}]: \"{s}\"\n", .{ i, doc });
    }

    const dynamic_vectors = try bow.transformManyDynamic(doc_list);
    defer {
        for (dynamic_vectors) |vec| {
            allocator.free(vec);
        }
        allocator.free(dynamic_vectors);
    }

    std.debug.print("\nResults:\n", .{});
    for (dynamic_vectors, 0..) |vec, i| {
        std.debug.print("  \"{s}\" -> [", .{doc_list.items[i]});
        for (vec, 0..) |val, j| {
            if (j > 0) std.debug.print(", ", .{});
            std.debug.print("{}", .{val});
        }
        std.debug.print("]\n", .{});
    }

    try testing.expect(dynamic_vectors.len == doc_list.items.len);
    for (dynamic_vectors) |vec| {
        try testing.expect(vec.len == bow.vocabulary.count());
    }

    std.debug.print("TransformManyDynamic test passed\n", .{});
}
