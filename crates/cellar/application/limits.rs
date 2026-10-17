//! Domain-bound lookup of application resource limits.
pub use cellar_app::limits::{
    BlobLimits, CellLimits, KvLimits, SqlLimits, SqliteLimits, WorkflowLimits,
};

pub(crate) fn current() -> CellLimits {
    crate::asyncrt::services().cell_limits()
}
