//! Thin wrapper around the SQLite3 C library.
//!
//! Provides a safe Zig interface for common SQLite operations, handling
//! error codes and providing proper slice-based text binding.

const std = @import("std");

pub const c = @cImport({
    @cInclude("sqlite3.h");
});

/// SQLite operation errors.
pub const Error = error{
    /// General SQLite error.
    SqliteError,
    /// Error during statement execution.
    SqliteStepError,
    /// Query completed with no more rows.
    SqliteDone,
    /// Database is locked or busy.
    SqliteBusy,
};

/// Opens a SQLite database at the given path.
/// The path must be null-terminated for the C API.
pub fn open(path: [:0]const u8) !*c.sqlite3 {
    var db: ?*c.sqlite3 = null;
    const rc = c.sqlite3_open_v2(path.ptr, &db, c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE, null);
    if (rc != c.SQLITE_OK) {
        if (db) |handle| {
            _ = c.sqlite3_close_v2(handle);
        }
        return Error.SqliteError;
    }
    return db.?;
}

pub fn close(db: *c.sqlite3) void {
    _ = c.sqlite3_close_v2(db);
}

/// Executes a SQL statement directly without result handling.
/// The SQL must be null-terminated for the C API.
pub fn exec(db: *c.sqlite3, sql: [:0]const u8) !void {
    const rc = c.sqlite3_exec(db, sql.ptr, null, null, null);
    if (rc != c.SQLITE_OK) {
        if (isBusyCode(rc)) return Error.SqliteBusy;
        return Error.SqliteError;
    }
}

/// Prepares a SQL statement for execution.
/// Uses the length parameter so the SQL does not need to be null-terminated.
pub fn prepare(db: *c.sqlite3, sql: []const u8) !*c.sqlite3_stmt {
    var stmt: ?*c.sqlite3_stmt = null;
    const rc = c.sqlite3_prepare_v2(db, sql.ptr, @as(c_int, @intCast(sql.len)), &stmt, null);
    if (rc != c.SQLITE_OK) {
        if (isBusyCode(rc)) return Error.SqliteBusy;
        return Error.SqliteError;
    }
    return stmt.?;
}

/// Steps through a prepared statement.
/// Returns true if a row is available, false if done.
pub fn step(stmt: *c.sqlite3_stmt) !bool {
    const rc = c.sqlite3_step(stmt);
    if (rc == c.SQLITE_ROW) return true;
    if (rc == c.SQLITE_DONE) return false;
    if (isBusyCode(rc)) return Error.SqliteBusy;
    return Error.SqliteStepError;
}

/// Finalizes and frees a prepared statement.
pub fn finalize(stmt: *c.sqlite3_stmt) void {
    _ = c.sqlite3_finalize(stmt);
}

/// Binds a text value to a prepared statement parameter.
/// SAFETY: The caller must ensure the text slice remains valid until sqlite3_step()
/// completes or the statement is finalized. The explicit length parameter ensures
/// SQLite doesn't read beyond the slice bounds (no null terminator needed).
pub fn bindText(stmt: *c.sqlite3_stmt, idx: c_int, text: []const u8) !void {
    // Using null as destructor means SQLite treats the string as static.
    // This is safe because all callers in this codebase ensure the string
    // remains valid for the duration of the statement execution.
    const rc = c.sqlite3_bind_text(stmt, idx, text.ptr, @as(c_int, @intCast(text.len)), null);
    if (rc != c.SQLITE_OK) return Error.SqliteError;
}

/// Binds a 64-bit integer value to a prepared statement parameter.
pub fn bindInt64(stmt: *c.sqlite3_stmt, idx: c_int, value: i64) !void {
    const rc = c.sqlite3_bind_int64(stmt, idx, value);
    if (rc != c.SQLITE_OK) return Error.SqliteError;
}

/// Binds a 32-bit integer value to a prepared statement parameter.
pub fn bindInt(stmt: *c.sqlite3_stmt, idx: c_int, value: i32) !void {
    const rc = c.sqlite3_bind_int(stmt, idx, value);
    if (rc != c.SQLITE_OK) return Error.SqliteError;
}

/// Binds a NULL value to a prepared statement parameter.
pub fn bindNull(stmt: *c.sqlite3_stmt, idx: c_int) !void {
    const rc = c.sqlite3_bind_null(stmt, idx);
    if (rc != c.SQLITE_OK) return Error.SqliteError;
}

/// Retrieves a text column value from the current result row.
/// Returns an empty string if the column is NULL.
pub fn columnText(stmt: *c.sqlite3_stmt, idx: c_int) []const u8 {
    const ptr = c.sqlite3_column_text(stmt, idx);
    if (ptr == null) return "";
    const len = c.sqlite3_column_bytes(stmt, idx);
    return @as([*]const u8, @ptrCast(ptr))[0..@as(usize, @intCast(len))];
}

/// Retrieves a 64-bit integer column value from the current result row.
pub fn columnInt64(stmt: *c.sqlite3_stmt, idx: c_int) i64 {
    return c.sqlite3_column_int64(stmt, idx);
}

/// Retrieves a 32-bit integer column value from the current result row.
pub fn columnInt(stmt: *c.sqlite3_stmt, idx: c_int) i32 {
    return c.sqlite3_column_int(stmt, idx);
}

/// Returns the rowid of the most recent successful INSERT.
pub fn lastInsertRowId(db: *c.sqlite3) i64 {
    return c.sqlite3_last_insert_rowid(db);
}

/// Returns the error message for the most recent failed SQLite operation.
pub fn errmsg(db: *c.sqlite3) []const u8 {
    const ptr = c.sqlite3_errmsg(db);
    return std.mem.span(ptr);
}

/// Checks if an SQLite result code indicates a busy/locked condition.
fn isBusyCode(rc: c_int) bool {
    const primary: c_int = rc & @as(c_int, 0xff);
    return primary == c.SQLITE_BUSY or primary == c.SQLITE_LOCKED;
}
