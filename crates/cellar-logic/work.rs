// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Persisted framework work shares discovery with application alarms, while
//! retaining an independent identity and deadline for each obligation.

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Reason {
    Outbox,
    StreamMaintenance,
    ResourceReconciliation,
    FilesystemMaintenance,
}

impl Reason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Outbox => "outbox",
            Self::StreamMaintenance => "stream-maintenance",
            Self::ResourceReconciliation => "resource-reconciliation",
            Self::FilesystemMaintenance => "filesystem-maintenance",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "outbox" => Some(Self::Outbox),
            "stream-maintenance" => Some(Self::StreamMaintenance),
            "resource-reconciliation" => Some(Self::ResourceReconciliation),
            "filesystem-maintenance" => Some(Self::FilesystemMaintenance),
            _ => None,
        }
    }
}

/// A completion may settle only the installation it originally observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Installation {
    pub reason: Reason,
    pub generation: u64,
    pub at_ms: i64,
}

/// Supply deadlines from committed storage; absence never removes another
/// reason's deadline. This decision is independent of clocks and I/O.
pub fn effective_deadline(
    application_alarm: Option<i64>,
    internal: impl IntoIterator<Item = i64>,
) -> Option<i64> {
    application_alarm.into_iter().chain(internal).min()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Materialization {
    Application,
    InspectRetainedSource,
    Reject,
}

/// Removing application code does not erase a retained delivery obligation.
/// Unknown fresh scopes cannot use the recovery executor to create new state.
pub fn materialization(class_deployed: bool, fresh: bool) -> Materialization {
    match (class_deployed, fresh) {
        (true, _) => Materialization::Application,
        (false, false) => Materialization::InspectRetainedSource,
        (false, true) => Materialization::Reject,
    }
}

/// A source recovery executor has no application callback to prefer or consume.
pub fn internal_first(recovery_only: bool, internal_ms: i64, application_ms: Option<i64>) -> bool {
    recovery_only || application_ms.is_none_or(|at| internal_ms < at)
}
