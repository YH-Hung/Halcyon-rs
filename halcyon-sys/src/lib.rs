//! Raw FFI bindings to the IBM Db2 CLI (`libdb2` from clidriver).
//!
//! Hand-written, deliberately minimal: only the entry points, types, and
//! constants that Halcyon's driver seam uses. Type widths and constant values
//! are taken from the clidriver headers (`sqlcli.h` / `sqlext.h`) as compiled
//! on 64-bit Linux/macOS: `SQLLEN`/`SQLULEN` are 64-bit and handles are
//! pointers.
//!
//! Everything here is `unsafe` to call; the only consumer is the `driver::db2`
//! module in the `halcyon` crate.

#![allow(non_camel_case_types, non_snake_case, clippy::missing_safety_doc)]

use std::ffi::c_void;

// --- Types (sqlcli.h) ---

pub type SQLCHAR = u8;
pub type SQLSMALLINT = i16;
pub type SQLUSMALLINT = u16;
pub type SQLINTEGER = i32;
pub type SQLUINTEGER = u32;
pub type SQLRETURN = SQLSMALLINT;
pub type SQLLEN = i64;
pub type SQLULEN = u64;
pub type SQLPOINTER = *mut c_void;
pub type SQLHANDLE = *mut c_void;
pub type SQLHENV = SQLHANDLE;
pub type SQLHDBC = SQLHANDLE;
pub type SQLHSTMT = SQLHANDLE;
pub type SQLHWND = SQLPOINTER;

pub const SQL_NULL_HANDLE: SQLHANDLE = std::ptr::null_mut();

// --- Return codes ---

pub const SQL_SUCCESS: SQLRETURN = 0;
pub const SQL_SUCCESS_WITH_INFO: SQLRETURN = 1;
pub const SQL_NO_DATA: SQLRETURN = 100;
pub const SQL_ERROR: SQLRETURN = -1;
pub const SQL_INVALID_HANDLE: SQLRETURN = -2;

// --- Handle types ---

pub const SQL_HANDLE_ENV: SQLSMALLINT = 1;
pub const SQL_HANDLE_DBC: SQLSMALLINT = 2;
pub const SQL_HANDLE_STMT: SQLSMALLINT = 3;

// --- Length / indicator sentinels ---

pub const SQL_NTS: SQLINTEGER = -3;
pub const SQL_NULL_DATA: SQLLEN = -1;
pub const SQL_NO_TOTAL: SQLLEN = -4;

// --- SQL data types ---

pub const SQL_CHAR: SQLSMALLINT = 1;
pub const SQL_INTEGER: SQLSMALLINT = 4;
pub const SQL_SMALLINT: SQLSMALLINT = 5;
pub const SQL_FLOAT: SQLSMALLINT = 6;
pub const SQL_REAL: SQLSMALLINT = 7;
pub const SQL_DOUBLE: SQLSMALLINT = 8;
pub const SQL_VARCHAR: SQLSMALLINT = 12;
pub const SQL_BINARY: SQLSMALLINT = -2;
pub const SQL_VARBINARY: SQLSMALLINT = -3;
pub const SQL_LONGVARBINARY: SQLSMALLINT = -4;
pub const SQL_BIGINT: SQLSMALLINT = -5;
pub const SQL_BIT: SQLSMALLINT = -7;
/// IBM extension (sqlcli.h), not the ODBC value.
pub const SQL_BLOB: SQLSMALLINT = -98;

// --- C data types (sqlext.h) ---

pub const SQL_C_CHAR: SQLSMALLINT = SQL_CHAR;
pub const SQL_C_DOUBLE: SQLSMALLINT = SQL_DOUBLE;
pub const SQL_C_BINARY: SQLSMALLINT = SQL_BINARY;
pub const SQL_C_BIT: SQLSMALLINT = SQL_BIT;
pub const SQL_SIGNED_OFFSET: SQLSMALLINT = -20;
pub const SQL_C_SBIGINT: SQLSMALLINT = SQL_BIGINT + SQL_SIGNED_OFFSET; // -25

// --- SQLDriverConnect ---

pub const SQL_DRIVER_NOPROMPT: SQLUSMALLINT = 0;

// --- SQLBindParameter ---

pub const SQL_PARAM_INPUT: SQLSMALLINT = 1;

// --- Statement attributes (array binding) ---

pub const SQL_ATTR_PARAM_BIND_TYPE: SQLINTEGER = 18;
pub const SQL_ATTR_PARAMSET_SIZE: SQLINTEGER = 22;
pub const SQL_PARAM_BIND_BY_COLUMN: SQLULEN = 0;

// --- Connection attributes ---

pub const SQL_ATTR_AUTOCOMMIT: SQLINTEGER = 102;
pub const SQL_AUTOCOMMIT_OFF: SQLULEN = 0;
pub const SQL_AUTOCOMMIT_ON: SQLULEN = 1;
pub const SQL_IS_INTEGER: SQLINTEGER = -6;

// --- SQLEndTran ---

pub const SQL_COMMIT: SQLSMALLINT = 0;
pub const SQL_ROLLBACK: SQLSMALLINT = 1;

// --- SQLFreeStmt ---

pub const SQL_CLOSE: SQLUSMALLINT = 0;

extern "C" {
    pub fn SQLAllocHandle(
        handle_type: SQLSMALLINT,
        input_handle: SQLHANDLE,
        output_handle: *mut SQLHANDLE,
    ) -> SQLRETURN;

    pub fn SQLFreeHandle(handle_type: SQLSMALLINT, handle: SQLHANDLE) -> SQLRETURN;

    pub fn SQLDriverConnect(
        hdbc: SQLHDBC,
        hwnd: SQLHWND,
        in_connection_string: *mut SQLCHAR,
        string_length: SQLSMALLINT,
        out_connection_string: *mut SQLCHAR,
        buffer_length: SQLSMALLINT,
        out_length: *mut SQLSMALLINT,
        driver_completion: SQLUSMALLINT,
    ) -> SQLRETURN;

    pub fn SQLDisconnect(hdbc: SQLHDBC) -> SQLRETURN;

    pub fn SQLExecDirect(
        hstmt: SQLHSTMT,
        statement_text: *mut SQLCHAR,
        text_length: SQLINTEGER,
    ) -> SQLRETURN;

    pub fn SQLPrepare(
        hstmt: SQLHSTMT,
        statement_text: *mut SQLCHAR,
        text_length: SQLINTEGER,
    ) -> SQLRETURN;

    pub fn SQLBindParameter(
        hstmt: SQLHSTMT,
        parameter_number: SQLUSMALLINT,
        input_output_type: SQLSMALLINT,
        value_type: SQLSMALLINT,
        parameter_type: SQLSMALLINT,
        column_size: SQLULEN,
        decimal_digits: SQLSMALLINT,
        parameter_value: SQLPOINTER,
        buffer_length: SQLLEN,
        str_len_or_ind: *mut SQLLEN,
    ) -> SQLRETURN;

    pub fn SQLExecute(hstmt: SQLHSTMT) -> SQLRETURN;

    pub fn SQLRowCount(hstmt: SQLHSTMT, row_count: *mut SQLLEN) -> SQLRETURN;

    pub fn SQLNumResultCols(hstmt: SQLHSTMT, column_count: *mut SQLSMALLINT) -> SQLRETURN;

    pub fn SQLDescribeCol(
        hstmt: SQLHSTMT,
        column_number: SQLUSMALLINT,
        column_name: *mut SQLCHAR,
        buffer_length: SQLSMALLINT,
        name_length: *mut SQLSMALLINT,
        data_type: *mut SQLSMALLINT,
        column_size: *mut SQLULEN,
        decimal_digits: *mut SQLSMALLINT,
        nullable: *mut SQLSMALLINT,
    ) -> SQLRETURN;

    pub fn SQLFetch(hstmt: SQLHSTMT) -> SQLRETURN;

    pub fn SQLGetData(
        hstmt: SQLHSTMT,
        column_number: SQLUSMALLINT,
        target_type: SQLSMALLINT,
        target_value: SQLPOINTER,
        buffer_length: SQLLEN,
        str_len_or_ind: *mut SQLLEN,
    ) -> SQLRETURN;

    pub fn SQLFreeStmt(hstmt: SQLHSTMT, option: SQLUSMALLINT) -> SQLRETURN;

    pub fn SQLSetStmtAttr(
        hstmt: SQLHSTMT,
        attribute: SQLINTEGER,
        value: SQLPOINTER,
        string_length: SQLINTEGER,
    ) -> SQLRETURN;

    pub fn SQLSetConnectAttr(
        hdbc: SQLHDBC,
        attribute: SQLINTEGER,
        value: SQLPOINTER,
        string_length: SQLINTEGER,
    ) -> SQLRETURN;

    pub fn SQLEndTran(
        handle_type: SQLSMALLINT,
        handle: SQLHANDLE,
        completion_type: SQLSMALLINT,
    ) -> SQLRETURN;

    pub fn SQLGetDiagRec(
        handle_type: SQLSMALLINT,
        handle: SQLHANDLE,
        rec_number: SQLSMALLINT,
        sqlstate: *mut SQLCHAR,
        native_error: *mut SQLINTEGER,
        message_text: *mut SQLCHAR,
        buffer_length: SQLSMALLINT,
        text_length: *mut SQLSMALLINT,
    ) -> SQLRETURN;
}
