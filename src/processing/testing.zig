const std = @import("std");
const BagOfWords = @import("main.zig").BagOfWords;

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

    std.debug.print("\nTesting transform single document\n", .{});

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
