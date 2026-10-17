//! SQL binding contracts and materialization budgets.
pub mod wire;
pub use cellar_native::sql::{
    shape, BindValue, Error, ExecResult, ResultSet, SqlMeta, SqlPreparedRequest, SqlPreparedResult,
    SqlRequest, SqlResponse, SqlRows, Value, PRIMARY_BOOKMARK,
};

/// Default maximum rows in one materialized SQL result set.
pub const SQL_MAX_ROWS: usize = 100_000;

/// Default bytes one materialized SQL result set may carry across a binding.
/// Embedded hosts select the effective budget through `CellLimits.sql`.
pub const SQL_MAX_RESULT_BYTES: usize = 32 * 1024 * 1024;
