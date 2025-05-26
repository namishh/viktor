const std = @import("std");
const DatabaseError = @import("errors.zig").DatabaseError;

// serialization code taken from https://github.com/ziglibs/s2s/

fn AlignedInt(comptime T: type) type {
    return std.math.ByteAlignedInt(T);
}

pub fn Value(comptime T: type) type {
    return struct {
        data: T,
        allocator: ?std.mem.Allocator,
        const Self = @This();

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

        pub fn convertToBytes(self: *const Self, allocator: *const std.mem.Allocator) ![]u8 {
            var list = std.ArrayList(u8).init(allocator.*);
            defer list.deinit();

            try serializeRecursive(list.writer(), T, self.data);
            return try allocator.dupe(u8, list.items);
        }

        pub fn fromBytes(bytes: []const u8, allocator: std.mem.Allocator) !Self {
            var stream = std.io.fixedBufferStream(bytes);
            var result: T = undefined;
            try recursiveDeserialize(stream.reader(), T, allocator, &result);
            return Self{ .data = result, .allocator = allocator };
        }

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

        pub fn deinit(self: *const Self) void {
            if (self.allocator) |allocator| {
                deinitRecursive(allocator, T, &self.data);
            }
        }
    };
}
