// basic server, will be used later
const std = @import("std");
const net = std.net;

pub const Server = struct {
    allocator: std.mem.Allocator,
    addr: net.Address,
    listener: ?net.Server,
    port: u16,

    pub fn init(allocator: std.mem.Allocator, port: u16) !Server {
        const addr = try net.Address.parseIp("127.0.0.1", port);
        return Server{
            .allocator = allocator,
            .addr = addr,
            .listener = null,
            .port = port,
        };
    }

    pub fn deinit(self: *Server) void {
        if (self.listener) |*listener| {
            listener.deinit();
        }
    }

    fn handleConnection(self: *Server, connection: net.Server.Connection) !void {
        _ = self;

        var buffer: [1024]u8 = undefined;
        const bytes_read = try connection.stream.read(&buffer);

        if (bytes_read > 0) {
            std.debug.print("received {} bytes: {s}\n", .{ bytes_read, buffer[0..bytes_read] });

            const response = "OK\r\n";
            _ = try connection.stream.write(response);
        }
    }

    pub fn start(self: *Server) !void {
        self.listener = try self.addr.listen(.{ .reuse_address = true });
        std.debug.print("viktor server listening on port {}\n", .{self.port});

        while (true) {
            const connection = try self.listener.?.accept();
            defer connection.stream.close();

            try self.handleConnection(connection);
        }
    }
};
