// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One node queue and physical shutdown owner. Provisioning has independent
//! admission so command ownership need not recursively acquire its own budget.
use super::*;

pub(super) enum Request {
    Archive(Box<archive::Request>),
    S3(Box<archive::settlement::Request>),
    Registration(Box<registration::Request>),
    Provision(Box<provision::Request>),
    Command(Box<commands::Request>),
    Query(Box<queries::Request>),
    Enrollment(Box<enrollments::Request>),
}

pub(super) enum Completed {
    Command(Box<commands::Completed>),
    Enrollment(Box<enrollments::Completed>),
    Query,
}

impl Completed {
    pub fn group(&self) -> Option<cellar_logic::cell_group::GroupHistory> {
        match self {
            Self::Command(done) => done.invocation.as_ref().and_then(|i| i.unresolved_group()),
            Self::Enrollment(done) => done.invocation.unresolved_group(),
            Self::Query => None,
        }
    }

    pub fn failure(&self) -> Option<CellGroupControllerError> {
        match self {
            Self::Command(done) if done.invocation.as_ref().is_some_and(|i| i.unresolved()) => {
                Some(done.error.clone().map_or(
                    CellGroupControllerError::Invalid("unresolved successful command"),
                    CellGroupControllerError::Command,
                ))
            }
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
    transport: RaftTransport,
) -> Completed {
    match request {
        Request::Registration(_) | Request::Provision(_) | Request::Archive(_) | Request::S3(_) => {
            unreachable!("registration has its own physical join owner")
        }
        Request::Command(request) => Completed::Command(Box::new(
            commands::drive(*request, runtime, sql, shared, progress).await,
        )),
        Request::Enrollment(request) => Completed::Enrollment(Box::new(
            enrollments::drive(*request, runtime, sql, shared, progress).await,
        )),
        Request::Query(request) => {
            queries::drive(*request, runtime, sql, shared, transport).await;
            Completed::Query
        }
    }
}
