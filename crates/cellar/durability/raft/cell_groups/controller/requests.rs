// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! All mutations share the same node queue, admission and physical shutdown owner.
use super::*;

pub(super) enum Request {
    Command(Box<commands::Request>),
    Enrollment(Box<enrollments::Request>),
}

pub(super) enum Completed {
    Command(Box<commands::Completed>),
    Enrollment(Box<enrollments::Completed>),
}

impl Completed {
    pub fn failure(&self) -> Option<CellGroupControllerError> {
        match self {
            Self::Command(done) if done.invocation.unresolved() => Some(done.error.clone().map_or(
                CellGroupControllerError::Invalid("unresolved successful command"),
                CellGroupControllerError::Command,
            )),
            Self::Enrollment(done) if done.invocation.unresolved() => {
                Some(done.error.clone().map_or(
                    CellGroupControllerError::Invalid("unresolved successful enrollment"),
                    CellGroupControllerError::Enrollment,
                ))
            }
            _ => None,
        }
    }
}

pub(super) async fn drive(
    request: Request,
    runtime: CellGroupRuntime,
    sql: CellGroupSqlPool,
    shared: Arc<Shared>,
    progress: tokio::sync::watch::Receiver<()>,
) -> Completed {
    match request {
        Request::Command(request) => Completed::Command(Box::new(
            commands::drive(*request, runtime, sql, shared, progress).await,
        )),
        Request::Enrollment(request) => Completed::Enrollment(Box::new(
            enrollments::drive(*request, runtime, sql, shared, progress).await,
        )),
    }
}
