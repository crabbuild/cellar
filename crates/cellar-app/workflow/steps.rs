//! Workflow invocation values, retry configuration and typed step failures.
use super::{CronSchedule, Duration, ErrorRecord};
use cellar_native::clone::StructuredValue;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Event {
    pub payload: StructuredValue,
    pub timestamp_ms: i64,
    pub instance_id: String,
    pub workflow_name: String,
    pub schedule: Option<CronSchedule>,
}

#[derive(Clone, Debug)]
pub struct StepName(pub Vec<u16>);

impl From<&str> for StepName {
    fn from(name: &str) -> Self {
        Self(name.encode_utf16().collect())
    }
}

impl From<String> for StepName {
    fn from(name: String) -> Self {
        Self::from(name.as_str())
    }
}

impl From<Vec<u16>> for StepName {
    fn from(name: Vec<u16>) -> Self {
        Self(name)
    }
}

/// Options for a durable external-event wait.
#[derive(Clone, Debug)]
pub struct WaitOptions {
    pub event_type: String,
    pub timeout: Option<Duration>,
}

impl WaitOptions {
    pub fn new(event_type: impl Into<String>) -> Self {
        Self {
            event_type: event_type.into(),
            timeout: None,
        }
    }

    pub fn timeout(mut self, timeout: impl Into<Duration>) -> Self {
        self.timeout = Some(timeout.into());
        self
    }
}

impl From<&str> for WaitOptions {
    fn from(event_type: &str) -> Self {
        Self::new(event_type)
    }
}

impl From<String> for WaitOptions {
    fn from(event_type: String) -> Self {
        Self::new(event_type)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Backoff {
    Constant,
    Linear,
    Exponential,
}

pub type DelayCallback =
    Arc<dyn Fn(Attempt, ErrorRecord) -> crate::HandlerFuture<Duration> + Send + Sync>;

#[derive(Clone)]
pub enum Delay {
    Fixed(Duration),
    Dynamic(DelayCallback),
}

#[derive(Clone)]
pub struct Retries {
    pub limit: f64,
    pub delay: Delay,
    pub backoff: Option<Backoff>,
}

impl Retries {
    /// Configure retries with a fixed initial delay and exponential backoff.
    pub fn new(limit: u32, delay: impl Into<Duration>) -> Self {
        Self {
            limit: f64::from(limit),
            delay: Delay::Fixed(delay.into()),
            backoff: Some(Backoff::Exponential),
        }
    }

    pub fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = Some(backoff);
        self
    }
}

#[derive(Clone, Default)]
pub struct StepConfig {
    pub retries: Option<Retries>,
    pub timeout: Option<Duration>,
    pub sensitive: bool,
    pub rollback: bool,
    /// Explicit inspection input; closures are not introspected. Sensitive steps redact it.
    pub input: Option<StructuredValue>,
}

impl StepConfig {
    pub fn input(mut self, input: StructuredValue) -> Self {
        self.input = Some(input);
        self
    }

    pub fn retries(mut self, retries: Retries) -> Self {
        self.retries = Some(retries);
        self
    }

    pub fn timeout(mut self, timeout: impl Into<Duration>) -> Self {
        self.timeout = Some(timeout.into());
        self
    }
}

#[derive(Clone, Debug)]
pub struct Attempt {
    pub name: Vec<u16>,
    pub count: u64,
    pub attempt: u64,
    pub retry_limit: f64,
    pub retry_delay: Option<Duration>,
    pub backoff: Option<Backoff>,
    pub timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct StepFailure {
    pub record: ErrorRecord,
    pub non_retryable: bool,
}

impl StepFailure {
    pub fn non_retryable(message: &str) -> Self {
        Self {
            record: ErrorRecord {
                name: "NonRetryableError".encode_utf16().collect(),
                message: message.encode_utf16().collect(),
            },
            non_retryable: true,
        }
    }
}

impl std::fmt::Display for StepFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.record.message_lossy())
    }
}

impl std::error::Error for StepFailure {}
