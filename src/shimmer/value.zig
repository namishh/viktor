const std = @import("std");
const DatabaseError = @import("errors.zig").DatabaseError;

// utility function that returns a type that is aligned to the size of T.
fn AlignedInt(comptime T: type) type {
    return std.math.ByteAlignedInt(T);
}

// Value is a generic type that can serialize and deserialize any type T.
// It takes in
// - a type parameter `T` which is the type of the value to be stored.
// - an optional allocator to manage memory for complex types, slices, structs, pointers.

// much of the serialization logic is based on this repo https://github.com/ziglibs/s2s/

pub fn Value(comptime T: type) type {
    return struct {
        data: T,
        allocator: ?std.mem.Allocator,
        const Self = @This();

        // This function recursively serializes the value of type T into a stream by taking into account its type information.
        // for primitive types, it writes the raw bytes directly.
        // for complex types, it recursively serializes each field or element.
        // It takes in
        // - `stream`: the output stream to write the serialized data to.
        // - `comptime T2`: the type of the value to be serialized.
        // - `value`: the value of type T2 to be serialized.
        // It returns an error if the serialization fails.

        fn serializeRecursive(stream: anytype, comptime T2: type, value: T2) !void {
            switch (@typeInfo(T2)) {
                .void => {},
                .bool => try stream.writeByte(@intFromBool(value)),
                .float => switch (T2) {
                    f16 => try stream.writeInt(u16, @bitCast(value), .little),
                    f32 => try stream.writeInt(u32, @bitCast(value), .little),
                    f64 => try stream.writeInt(u64, @bitCast(value), .little),
                    f80 => try stream.writeInt(u80, @bitCast(value), .little),
                    f128 => try stream.writeInt(u128, @bitCast(value), .little),
                    else => unreachable,
                },
                .int => {
                    if (T2 == usize) {
                        try stream.writeInt(u64, value, .little);
                    } else {
                        try stream.writeInt(AlignedInt(T2), value, .little);
                    }
                },
                .pointer => |ptr| {
                    switch (ptr.size) {
                        .one => serializeRecursive(stream, ptr.child, value.*),
                        .slice => {
                            try stream.writeInt(u64, @as(usize, value.len), .little);
                            if (ptr.child == u8) {
                                try stream.writeAll(value);
                            } else {
                                for (value) |item| {
                                    try serializeRecursive(stream, ptr.child, item);
                                }
                            }
                        },
                        .c => unreachable,
                        .many => unreachable,
                    }
                },
                .array => |arr| {
                    if (arr.child == u8) {
                        try stream.writeAll(&value);
                    } else {
                        for (value) |item| {
                            try serializeRecursive(stream, arr.child, item);
                        }
                    }
                },
                .@"struct" => |str| {
                    inline for (str.fields) |fld| {
                        try serializeRecursive(stream, fld.type, @field(value, fld.name));
                    }
                },
                else => return DatabaseError.InvalidDataType,
            }
        }

        // This function recursively deserializes data from a stream into a value of type T2.
        // it reads the data based on the type information of T2 and populates the target pointer.
        // It takes in
        // - `stream`: the input stream to read the serialized data from.
        // - `comptime T2`: the type of the value to be deserialized.
        // - `allocator`: an optional allocator to manage memory for complex types, slices, structs, pointers.
        // - `target`: a pointer to the target value of type T2 where the deserialized data will be stored.
        // returns an error if the deserialization fails, such as unexpected data, out of memory, or end of stream.
        fn recursiveDeserialize(
            stream: anytype,
            comptime T2: type,
            allocator: ?std.mem.Allocator,
            target: *T2,
        ) (@TypeOf(stream).Error || error{ UnexpectedData, OutOfMemory, EndOfStream })!void {
            switch (@typeInfo(T2)) {
                .void => target.* = {},
                .bool => target.* = (try stream.readByte()) != 0,
                .float => target.* = @bitCast(switch (T2) {
                    f16 => try stream.readInt(u16, .little),
                    f32 => try stream.readInt(u32, .little),
                    f64 => try stream.readInt(u64, .little),
                    f80 => try stream.readInt(u80, .little),
                    f128 => try stream.readInt(u128, .little),
                    else => unreachable,
                }),
                .int => target.* = if (T2 == usize)
                    std.math.cast(usize, try stream.readInt(u64, .little)) orelse return error.UnexpectedData
                else
                    @truncate(try stream.readInt(AlignedInt(T2), .little)),
                .pointer => |ptr| {
                    switch (ptr.size) {
                        .one => {
                            const pointer = try allocator.?.create(ptr.child);
                            errdefer allocator.?.destroy(pointer);
                            try recursiveDeserialize(stream, ptr.child, allocator, pointer);
                            target.* = pointer;
                        },
                        .slice => {
                            const length = std.math.cast(usize, try stream.readInt(u64, .little)) orelse return error.UnexpectedData;
                            const slice = try allocator.?.alloc(ptr.child, length);
                            errdefer allocator.?.free(slice);
                            if (ptr.child == u8) {
                                try stream.readNoEof(slice);
                            } else {
                                for (slice) |*item| {
                                    try recursiveDeserialize(stream, ptr.child, allocator, item);
                                }
                            }
                            target.* = slice;
                        },
                        .c => unreachable,
                        .many => unreachable,
                    }
                },
                .array => |arr| {
                    if (arr.child == u8) {
                        try stream.readNoEof(target);
                    } else {
                        for (target.*) |*item| {
                            try recursiveDeserialize(stream, arr.child, allocator, item);
                        }
                    }
                },
                .@"struct" => |str| {
                    inline for (str.fields) |fld| {
                        try recursiveDeserialize(stream, fld.type, allocator, &@field(target.*, fld.name));
                    }
                },
                else => return DatabaseError.InvalidDataType,
            }
        }

        // TODO: is there a way to avoid duplicating the data?
        // This function serializes the value of type T into a byte array.
        // It takes in
        // - `allocator`: an allocator to manage memory for complex types, slices, structs, pointers.
        // returns a byte array containing the serialized data or an error if serialization fails.
        pub fn convertToBytes(self: *const Self, allocator: *const std.mem.Allocator) ![]u8 {
            var list = std.ArrayList(u8).init(allocator.*);
            defer list.deinit();

            try serializeRecursive(list.writer(), T, self.data);
            return try allocator.dupe(u8, list.items);
        }

        // This function deserializes a byte array into a Value of type T.
        // It takes in
        // - `bytes`: a byte array containing the serialized data.
        // - `allocator`: an allocator to manage memory for complex types, slices, structs, pointers.
        // It returns a Value containing the deserialized data or an error if deserialization fails.
        pub fn fromBytes(bytes: []const u8, allocator: std.mem.Allocator) !Self {
            var stream = std.io.fixedBufferStream(bytes);
            var result: T = undefined;
            try recursiveDeserialize(stream.reader(), T, allocator, &result);
            return Self{ .data = result, .allocator = allocator };
        }

        // function to recursively deinitialize the value of type T. this is for pointers, slices, structs.
        // it takes in
        // - `allocator`: an allocator to manage memory for complex types, slices, structs, pointers.
        // - `comptime T2`: the type of the value to be deinitialized.
        // - `value`: a pointer to the value of type T2 to be deinitialized.
        // it deinitializes the value by freeing memory for slices, pointers, and recursively deinitializing fields of structs.
        // it does not free the memory for primitive types, as they are stack allocated.
        fn deinitRecursive(allocator: std.mem.Allocator, comptime T2: type, value: *const T2) void {
            switch (@typeInfo(T2)) {
                .pointer => |ptr| {
                    switch (ptr.size) {
                        .slice => {
                            allocator.free(value.*);
                        },
                        .one => {
                            deinitRecursive(allocator, ptr.child, value.*);
                            allocator.destroy(value.*);
                        },
                        else => {},
                    }
                },
                .@"struct" => |str| {
                    inline for (str.fields) |fld| {
                        deinitRecursive(allocator, fld.type, &@field(value.*, fld.name));
                    }
                },
                else => {},
            }
        }

        // this function deinitializes the value of type T.
        // it frees memory for complex types, slices, structs, pointers if an allocator is provided.
        pub fn deinit(self: *const Self) void {
            if (self.allocator) |allocator| {
                deinitRecursive(allocator, T, &self.data);
            }
        }
    };
}
