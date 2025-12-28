//! ULID (Universally Unique Lexicographically Sortable Identifier) generator.
//!
//! ULIDs are 128-bit identifiers consisting of a 48-bit timestamp (milliseconds)
//! and an 80-bit random component. They are encoded as 26-character strings using
//! Crockford's Base32 alphabet. This implementation ensures monotonicity: if multiple
//! IDs are generated within the same millisecond, the random component is incremented
//! rather than regenerated, preserving correct lexicographic sort order.

const std = @import("std");

/// Generates ULIDs while maintaining monotonic order for IDs generated
/// within the same millisecond.
pub const Generator = struct {
    prng: std.Random.DefaultPrng,
    last_ms: u64,
    last_rand: u128,

    /// Initializes a new ULID generator with the given PRNG seed.
    pub fn init(seed: u64) Generator {
        return .{
            .prng = std.Random.DefaultPrng.init(seed),
            .last_ms = 0,
            .last_rand = 0,
        };
    }

    /// Generates a new ULID for a specific timestamp.
    ///
    /// Returns a 26-character Crockford Base32 encoded string. The caller owns
    /// the returned memory and must free it using the provided allocator.
    pub fn next(self: *Generator, allocator: std.mem.Allocator, timestamp_ms: u64) ![]u8 {
        const value = self.nextValue(timestamp_ms);
        const out = try allocator.alloc(u8, 26);
        const buf = encodeUlid(value);
        std.mem.copyForwards(u8, out, buf[0..]);
        return out;
    }

    /// Generates a new ULID using the current system time.
    ///
    /// Returns a 26-character Crockford Base32 encoded string. The caller owns
    /// the returned memory and must free it using the provided allocator.
    pub fn nextNow(self: *Generator, allocator: std.mem.Allocator) ![]u8 {
        const now_ms = @as(u64, @intCast(std.time.milliTimestamp()));
        return self.next(allocator, now_ms);
    }

    fn nextValue(self: *Generator, timestamp_ms: u64) u128 {
        // Mask to 48 bits for the timestamp portion
        const ts = timestamp_ms & 0xFFFFFFFFFFFF;
        const max_rand: u128 = @as(u128, 1) << 80;

        var rand80: u128 = 0;
        if (ts > self.last_ms) {
            // New millisecond: generate fresh random bits
            rand80 = self.random80();
        } else {
            // Same millisecond: increment to preserve monotonic ordering
            rand80 = self.last_rand + 1;
            if (rand80 >= max_rand) {
                rand80 = 0;
            }
        }
        self.last_ms = ts;
        self.last_rand = rand80;

        // Combine 48-bit timestamp (high) with 80-bit random (low)
        return (@as(u128, ts) << 80) | rand80;
    }

    fn random80(self: *Generator) u128 {
        const hi = self.prng.random().int(u64);
        const lo = self.prng.random().int(u16);
        return (@as(u128, hi) << 16) | @as(u128, lo);
    }
};

/// Encodes a 128-bit ULID value as a 26-character Crockford Base32 string.
fn encodeUlid(value: u128) [26]u8 {
    const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    var out: [26]u8 = undefined;
    var v = value;
    var i: usize = 26;
    while (i > 0) {
        i -= 1;
        const idx = @as(usize, @intCast(v & 0x1F));
        out[i] = alphabet[idx];
        v >>= 5;
    }
    return out;
}
