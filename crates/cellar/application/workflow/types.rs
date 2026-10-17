//! Workflow values, duration parsing, binding configuration and definitions.
use super::{error, property};
use anyhow::{ensure, Result};
use cellar_native::clone::{StructuredValue, Value as CloneValue};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const VALUE_LIMIT: usize = 1 << 20;
pub const RETENTION_MAX_MS: f64 = 30.0 * 86400000.0;

/// The same numeric-millisecond and textual duration forms used by Workflows.
/// Fractional milliseconds remain intact until the host installs an alarm.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Duration {
    Milliseconds(f64),
    Text(String),
}

impl From<std::time::Duration> for Duration {
    fn from(duration: std::time::Duration) -> Self {
        Self::Milliseconds(duration.as_secs_f64() * 1000.0)
    }
}

impl From<&str> for Duration {
    fn from(value: &str) -> Self {
        Self::Text(value.into())
    }
}

impl From<String> for Duration {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<f64> for Duration {
    fn from(milliseconds: f64) -> Self {
        Self::Milliseconds(milliseconds)
    }
}

fn duration_space(character: char) -> bool {
    matches!(character as u32, 0x9..=0xd | 0x20 | 0xa0 | 0x1680 | 0x2000..=0x200a | 0x2028 | 0x2029 | 0x202f | 0x205f | 0x3000 | 0xfeff)
}

impl Duration {
    pub fn milliseconds(&self) -> Result<f64> {
        match self {
            Self::Milliseconds(value) if value.is_finite() && *value >= 0.0 => Ok(*value),
            Self::Text(value) => {
                let value = value.trim_matches(duration_space);
                let split = value.find(duration_space).ok_or_else(|| {
                    error("invalid duration: use milliseconds or \"<n> <unit>[s]\"")
                })?;
                let (number, unit) = value.split_at(split);
                let unit = unit.trim_matches(duration_space);
                let unit = unit.strip_suffix('s').unwrap_or(unit);
                let mut parts = number.split('.');
                let whole = parts.next().unwrap();
                let fraction = parts.next();
                ensure!(
                    !whole.is_empty()
                        && whole.bytes().all(|byte| byte.is_ascii_digit())
                        && fraction.is_none_or(|fraction| !fraction.is_empty()
                            && fraction.bytes().all(|byte| byte.is_ascii_digit()))
                        && parts.next().is_none(),
                    "WORKFLOW_ERROR: invalid duration number"
                );
                let multiplier = match unit {
                    "second" => 1000.0,
                    "minute" => 60000.0,
                    "hour" => 3600000.0,
                    "day" => 86400000.0,
                    "week" => 604800000.0,
                    "month" => 30.0 * 86400000.0,
                    "year" => 365.0 * 86400000.0,
                    _ => return Err(error("invalid duration unit")),
                };
                Ok(number.parse::<f64>().map_err(error)? * multiplier)
            }
            _ => Err(error(
                "invalid duration: use finite non-negative milliseconds",
            )),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Retention {
    #[serde(
        default,
        rename = "success_retention",
        alias = "successRetention",
        alias = "success"
    )]
    pub success: Option<Duration>,
    #[serde(
        default,
        rename = "error_retention",
        alias = "errorRetention",
        alias = "error"
    )]
    pub error: Option<Duration>,
}

impl Retention {
    /// Encode retention using the host's captured budget and Unix milliseconds.
    pub(super) fn wire(
        &self,
        limits: crate::application::limits::WorkflowLimits,
        now: i64,
    ) -> Result<serde_json::Value> {
        let normalize = |duration: &Option<Duration>| -> Result<f64> {
            let ms = duration
                .as_ref()
                .map(Duration::milliseconds)
                .transpose()?
                .unwrap_or(RETENTION_MAX_MS);
            ensure!(
                ms.is_finite()
                    && ms >= 0.0
                    && ms < i64::MAX.saturating_sub(now) as f64
                    && limits
                        .max_retention_ms
                        .is_none_or(|maximum| ms <= maximum as f64),
                "WORKFLOW_ERROR: retention exceeds the host limit or timestamp range"
            );
            Ok(ms)
        };
        Ok(json!({"successMs":normalize(&self.success)?,"errorMs":normalize(&self.error)?}))
    }
}

#[derive(Clone, Debug, Default)]
pub struct CreateOptions {
    pub id: Option<String>,
    pub params: Option<StructuredValue>,
    pub retention: Retention,
    pub location_hint: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StepKind {
    #[default]
    Do,
    Sleep,
    WaitForEvent,
}

impl StepKind {
    pub(super) fn wire(self) -> &'static str {
        match self {
            Self::Do => "do",
            Self::Sleep => "sleep",
            Self::WaitForEvent => "waitForEvent",
        }
    }
}

#[derive(Clone, Debug)]
pub struct RestartFrom {
    pub name: Vec<u16>,
    pub count: u64,
    pub kind: StepKind,
}

impl RestartFrom {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.encode_utf16().collect(),
            count: 1,
            kind: StepKind::Do,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RestartOptions {
    pub from: Option<RestartFrom>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TerminateOptions {
    pub rollback: bool,
}

#[derive(Clone, Debug)]
pub struct SendEvent {
    pub event_type: String,
    pub payload: Option<StructuredValue>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorRecord {
    pub name: Vec<u16>,
    pub message: Vec<u16>,
}

impl ErrorRecord {
    pub fn message_lossy(&self) -> String {
        String::from_utf16_lossy(&self.message)
    }

    pub(super) fn from_value(value: StructuredValue) -> Result<Self> {
        let text = |name: &str| -> Result<Vec<u16>> {
            match property(&value, name)?.map(|value| value.root) {
                Some(CloneValue::String(text)) => Ok(text),
                _ => Err(error("invalid Workflow error record")),
            }
        };
        Ok(Self {
            name: text("name")?,
            message: text("message")?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum State {
    Queued,
    Running,
    WaitingForPause,
    Waiting,
    Paused,
    Complete,
    Errored,
    Terminated,
    Unknown,
}

#[derive(Clone, Debug)]
pub struct Status {
    pub state: State,
    pub output: Option<StructuredValue>,
    pub error: Option<ErrorRecord>,
    pub rollback: Option<StructuredValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Deleted {
    pub id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DeleteError {
    pub id: String,
    pub code: u32,
    pub message: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DeleteBatchResult {
    pub deleted: Vec<Deleted>,
    pub errors: Vec<DeleteError>,
}

#[cfg(test)]
mod tests {
    use crate::application::{limits, workflow};
    use serde_json::json;

    #[test]
    fn retention_uses_the_supplied_clock_and_budget() -> anyhow::Result<()> {
        let mut limits = limits::WorkflowLimits {
            max_retention_ms: Some(2_000),
            ..Default::default()
        };
        let retention = workflow::Retention {
            success: Some(workflow::Duration::from("1.5 seconds")),
            error: Some(workflow::Duration::from(2_000.0)),
        };
        assert_eq!(
            retention.wire(limits, 10_000)?,
            json!({"successMs":1500.0,"errorMs":2000.0})
        );
        assert!(retention.wire(limits, i64::MAX - 1).is_err());
        limits.max_retention_ms = Some(1_000);
        assert!(retention.wire(limits, 10_000).is_err());
        Ok(())
    }
}
