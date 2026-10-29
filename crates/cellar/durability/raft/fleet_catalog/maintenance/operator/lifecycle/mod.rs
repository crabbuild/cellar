// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independent human grants and durable references to actual SDK effects.
mod grants;
mod records;
mod resume;
mod stop;
mod terminal;
pub(super) mod work;
use super::*;
pub use grants::{
    FleetRestartOperatorAction, FleetRestartOperatorActionAdmission,
    FleetRestartOperatorActionRequest,
};
pub(super) use records::{Facts, Resume, Stop};
pub use terminal::FleetRestartOperatorCompleted;
pub(super) use work::{reconcile, step, ResumeSession};

pub(super) fn protected(key: &str) -> bool {
    key.starts_with("restart-controller-action/")
        || key.starts_with("restart-controller-finished/")
        || key.starts_with("restart-controller-cancel-refused/")
}
