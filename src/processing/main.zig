// processing is not related to the database at all, this is just a helper module to seperate out text processing logic when feeding the database

const std = @import("std");
pub const BagOfWords = @import("bagofwords.zig").BagOfWords;
