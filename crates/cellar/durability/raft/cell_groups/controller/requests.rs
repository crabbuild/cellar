// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One node queue and physical shutdown owner. Provisioning has independent
//! admission so command ownership need not recursively acquire its own budget.
use super::*;

pub(super) enum Request {
    Registration(Box<registration::Request>),
    Provision(Box<provision::Request>),
    Command(Box<commands::Request>),
    Enrollment(Box<enrollments::Request>),
}

pub(super) enum Completed {
    Command(Box<commands::Completed>),
    Enrollment(Box<enrollments::Completed>),
}

impl Completed {
    pub fn group(&self) -> Option<cellar_logic::cell_group::GroupHistory> {
        match self {
            Self::Command(done) => done.invocation.unresolved_group(),
            Self::Enrollment(done) => done.invocation.unresolved_group(),
        }
    }

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
        Request::Registration(_) | Request::Provision(_) => {
            unreachable!("registration has its own physical join owner")
        }
        Request::Command(request) => Completed::Command(Box::new(
            commands::drive(*request, runtime, sql, shared, progress).await,
        )),
        Request::Enrollment(request) => Completed::Enrollment(Box::new(
            enrollments::drive(*request, runtime, sql, shared, progress).await,
        )),
    }
}
