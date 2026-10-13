//! Persisted framework work shares discovery with application alarms, while
//! retaining an independent identity and deadline for each obligation.

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Reason {
    Outbox,
    StreamMaintenance,
    ResourceReconciliation,
}

impl Reason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Outbox => "outbox",
            Self::StreamMaintenance => "stream-maintenance",
            Self::ResourceReconciliation => "resource-reconciliation",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "outbox" => Some(Self::Outbox),
            "stream-maintenance" => Some(Self::StreamMaintenance),
            "resource-reconciliation" => Some(Self::ResourceReconciliation),
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
