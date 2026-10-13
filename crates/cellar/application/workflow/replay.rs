use super::cell::*;
use super::*;
use crate::application::{Context, HandlerFuture, Storage};
use futures_util::task::AtomicWaker;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context as TaskContext, Poll};

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

pub type DelayCallback = Arc<dyn Fn(Attempt, ErrorRecord) -> HandlerFuture<Duration> + Send + Sync>;

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
}

impl StepConfig {
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

pub(super) fn failure(error: &anyhow::Error) -> StepFailure {
    let mut failure = error
        .downcast_ref::<StepFailure>()
        .cloned()
        .unwrap_or_else(|| StepFailure {
            record: ErrorRecord {
                name: "Error".encode_utf16().collect(),
                message: error.to_string().encode_utf16().collect(),
            },
            non_retryable: false,
        });
    failure.non_retryable |= failure
        .record
        .name
        .iter()
        .copied()
        .eq("NonRetryableError".encode_utf16());
    failure
}

pub(super) fn error_value(error: &anyhow::Error) -> Result<StructuredValue> {
    let record = failure(error).record;
    let mut value = data(json!({}))?;
    for (key, text) in [("name", record.name), ("message", record.message)] {
        set(
            &mut value,
            key,
            &StructuredValue {
                root: CloneValue::String(text),
                objects: Vec::new(),
            },
        )?;
    }
    Ok(value)
}

pub(super) fn revive(record: &StructuredValue) -> Result<anyhow::Error> {
    let record = ErrorRecord::from_value(
        property(record, "error")?.ok_or_else(|| error("missing step error"))?,
    )?;
    Ok(StepFailure {
        non_retryable: record
            .name
            .iter()
            .copied()
            .eq("NonRetryableError".encode_utf16()),
        record,
    }
    .into())
}
thread_local! {static ENCLOSING:RefCell<Option<Vec<u16>>>=const {RefCell::new(None)};}

struct StepGuard(Option<Vec<u16>>);

impl StepGuard {
    fn enter(name: &[u16]) -> Self {
        Self(ENCLOSING.with(|current| current.replace(Some(name.to_vec()))))
    }
}

impl Drop for StepGuard {
    fn drop(&mut self) {
        ENCLOSING.with(|current| current.replace(self.0.take()));
    }
}

struct Scoped<T> {
    name: Vec<u16>,
    future: HandlerFuture<T>,
}

impl<T> Future for Scoped<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let _frame = StepGuard::enter(&this.name);
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            this.future.as_mut().poll(cx)
        }))
        .unwrap_or_else(|panic| Poll::Ready(Err(crate::native_worker::panic_error(panic))))
    }
}

pub(super) fn scoped<T: Send + 'static>(
    name: &[u16],
    callback: impl FnOnce() -> HandlerFuture<T>,
) -> HandlerFuture<T> {
    let _frame = StepGuard::enter(name);
    let future =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)).unwrap_or_else(|panic| {
            let error = crate::native_worker::panic_error(panic);
            Box::pin(async move { Err(error) })
        });
    Box::pin(Scoped {
        name: name.to_vec(),
        future,
    })
}

enum StepResult {
    Value(StructuredValue),
    Block(f64),
    Pause,
}

type Task = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Default)]
struct State {
    counts: HashMap<Vec<u16>, u64>,
    ordinal: u64,
    running: usize,
    blocked: Vec<f64>,
    pause: bool,
    finished: bool,
    activity: u64,
    queued: Vec<Task>,
    held: Vec<tokio::sync::oneshot::Sender<Result<StructuredValue>>>,
    storage_failure: Option<String>,
    rollbacks: HashMap<String, Registration>,
}

pub(super) struct Shared {
    pub(super) storage: Storage,
    pub(super) context: Context,
    pub(super) generation: String,
    changed: Arc<tokio::sync::Notify>,
    rollback_mode: bool,
    state: Mutex<State>,
    wake: AtomicWaker,
}

#[derive(Clone)]
pub(super) struct Registration {
    pub name: Vec<u16>,
    pub count: u64,
    pub options: RollbackOptions,
}

impl Shared {
    pub(super) fn registration(&self, key: &str) -> Option<Registration> {
        self.state.lock().unwrap().rollbacks.get(key).cloned()
    }

    pub(super) fn track<T>(&self, result: Result<T>) -> Result<T> {
        if let Err(error) = &result {
            if error.downcast_ref::<StepFailure>().is_some() {
                return result;
            }
            self.state
                .lock()
                .unwrap()
                .storage_failure
                .get_or_insert_with(|| error.to_string());
        }
        result
    }

    pub(super) fn get(&self, key: &str) -> Result<Option<StructuredValue>> {
        self.track(read(&self.storage, key))
    }

    pub(super) fn put(&self, key: &str, value: &StructuredValue) -> Result<()> {
        let result = self.track(self.storage.transaction_sync(|transaction| {
            let Some(mut meta) = read(transaction, META)? else {
                return Ok(());
            };
            if text(&meta, "generation")? != self.generation || terminal(&meta)? {
                return Ok(());
            }
            let previous = read(transaction, key)?;
            super::history::step_change(transaction, &mut meta, key, previous.as_ref(), value)?;
            transaction.put_clone(key, value)?;
            transaction.put_clone(META, &meta)
        }));
        if result.is_ok() {
            self.changed.notify_waiters();
        }
        result
    }

    fn kind(&self, record: Option<StructuredValue>, kind: &str) -> Result<Option<StructuredValue>> {
        if let Some(record) = &record {
            if text(record, "kind")? != kind {
                let error = error("a step keeps one kind across replays");
                return Err(StepFailure {
                    record: failure(&error).record,
                    non_retryable: true,
                }
                .into());
            }
        }
        Ok(record)
    }
}

#[derive(Clone)]
pub struct Step(pub(super) Arc<Shared>);

/// Compatibility name for [`Step`].
pub use self::Step as Steps;

impl Step {
    fn frame(
        &self,
        name: Vec<u16>,
        body: impl FnOnce(Arc<Shared>, String, u64, u64) -> HandlerFuture<StepResult> + Send + 'static,
    ) -> HandlerFuture<StructuredValue> {
        if self.0.state.lock().unwrap().finished {
            return Box::pin(futures_util::future::pending());
        }
        if let Some(enclosing) = ENCLOSING.with(|current| current.borrow().clone()) {
            return Box::pin(async move {
                let error = error(format!(
                    "step {:?} cannot start inside callback of step {:?}",
                    String::from_utf16_lossy(&name),
                    String::from_utf16_lossy(&enclosing)
                ));
                Err(StepFailure {
                    record: failure(&error).record,
                    non_retryable: true,
                }
                .into())
            });
        }
        let mut state = self.0.state.lock().unwrap();
        if state.finished {
            return Box::pin(futures_util::future::pending());
        }
        let count = state.counts.entry(name.clone()).or_default();
        *count += 1;
        let count = *count;
        state.ordinal += 1;
        let ordinal = state.ordinal;
        state.running += 1;
        state.activity += 1;
        let key = format!(
            "{}step.{count}.{}",
            prefix(&self.0.generation),
            String::from_utf16_lossy(&name)
        );
        let (send, receive) = tokio::sync::oneshot::channel();
        let shared = self.0.clone();
        state.queued.push(Box::pin(async move {
            let result = async {
                let Some(meta) = shared.get(META)? else {
                    return Ok(StepResult::Pause);
                };
                if text(&meta, "generation")? != shared.generation
                    || terminal(&meta)?
                    || matches!(
                        text(&meta, "status")?.as_str(),
                        "paused" | "waitingForPause"
                    )
                {
                    return Ok(StepResult::Pause);
                }
                if property(&meta, "rollbackTarget")?.is_some() && !shared.rollback_mode {
                    return Ok(StepResult::Pause);
                }
                body(shared.clone(), key, count, ordinal).await
            }
            .await;
            let mut state = shared.state.lock().unwrap();
            state.running -= 1;
            state.activity += 1;
            match result {
                Ok(StepResult::Block(at)) => {
                    state.blocked.push(at);
                    state.held.push(send);
                }
                Ok(StepResult::Pause) => {
                    state.pause = true;
                    state.held.push(send);
                }
                Ok(StepResult::Value(value)) => {
                    let _ = send.send(Ok(value));
                }
                Err(error) => {
                    let _ = send.send(Err(error));
                }
            }
            drop(state);
            shared.wake.wake();
        }));
        drop(state);
        self.0.wake.wake();
        Box::pin(async move {
            match receive.await {
                Ok(result) => result,
                Err(_) => futures_util::future::pending().await,
            }
        })
    }

    /// Execute a named durable step, reusing its recorded result on replay.
    ///
    /// Rust reserves `do`, so Cellar uses `run` for Cloudflare's `step.do`.
    #[doc(alias = "do")]
    pub fn run<F, Fut>(
        &self,
        name: impl Into<StepName>,
        callback: F,
    ) -> HandlerFuture<StructuredValue>
    where
        F: FnOnce(Attempt) -> Fut + Send + 'static,
        Fut: Future<Output = Result<StructuredValue>> + Send + 'static,
    {
        self.run_with(name, StepConfig::default(), callback)
    }

    /// Execute a durable step with explicit retry and timeout settings.
    pub fn run_with<F, Fut>(
        &self,
        name: impl Into<StepName>,
        config: StepConfig,
        callback: F,
    ) -> HandlerFuture<StructuredValue>
    where
        F: FnOnce(Attempt) -> Fut + Send + 'static,
        Fut: Future<Output = Result<StructuredValue>> + Send + 'static,
    {
        self.run_internal(name, config, callback, None, false)
    }

    pub fn run_with_rollback<F, Fut>(
        &self,
        name: impl Into<StepName>,
        config: StepConfig,
        callback: F,
        rollback: RollbackOptions,
    ) -> HandlerFuture<StructuredValue>
    where
        F: FnOnce(Attempt) -> Fut + Send + 'static,
        Fut: Future<Output = Result<StructuredValue>> + Send + 'static,
    {
        if let Err(error) = validate_config(&rollback.config) {
            return Box::pin(async move { Err(error) });
        }
        self.run_internal(name, config, callback, Some(rollback), false)
    }

    pub(super) fn run_internal<F, Fut>(
        &self,
        name: impl Into<StepName>,
        config: StepConfig,
        callback: F,
        rollback: Option<RollbackOptions>,
        stream: bool,
    ) -> HandlerFuture<StructuredValue>
    where
        F: FnOnce(Attempt) -> Fut + Send + 'static,
        Fut: Future<Output = Result<StructuredValue>> + Send + 'static,
    {
        let name = name.into().0;
        if let Some(rollback) = &rollback {
            if let Err(error) = validate_config(&rollback.config) {
                return Box::pin(async move { Err(error) });
            }
        }
        let checked = check_step_name(&name).and_then(|_| validate_config(&config));
        let (timeout, timeout_ms) = match checked {
            Ok(value) => value,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        self.frame(name.clone(), move |shared, key, count, ordinal| {
            Box::pin(async move {
                let record = shared.kind(shared.get(&key)?, "do")?;
                if let Some(options) = &rollback {
                    shared.state.lock().unwrap().rollbacks.insert(
                        key.clone(),
                        Registration {
                            name: name.clone(),
                            count,
                            options: options.clone(),
                        },
                    );
                }
                if let Some(record) = &record {
                    match text(record, "status")?.as_str() {
                        "completed" => {
                            return Ok(StepResult::Value(
                                property(record, "value")?
                                    .unwrap_or_else(StructuredValue::undefined),
                            ))
                        }
                        "failed" => return Err(revive(record)?),
                        "retrying"
                            if !shared.rollback_mode
                                && number(record, "nextAt")? > now() as f64 =>
                        {
                            return Ok(StepResult::Block(number(record, "nextAt")?));
                        }
                        _ => {}
                    }
                }
                if shared.rollback_mode {
                    return Err(StepFailure::non_retryable("Workflow is rolling back").into());
                }
                let attempt = record
                    .as_ref()
                    .map(|record| number(record, "attempt"))
                    .transpose()?
                    .unwrap_or(0.0) as u64
                    + 1;
                let ordinal = record
                    .as_ref()
                    .map(|record| number(record, "ordinal"))
                    .transpose()?
                    .unwrap_or(ordinal as f64);
                let retries = config.retries.clone().unwrap_or_else(default_retries);
                let context = Attempt {
                    name: name.clone(),
                    count,
                    attempt,
                    retry_limit: retries.limit,
                    retry_delay: match &retries.delay {
                        Delay::Fixed(delay) => Some(delay.clone()),
                        Delay::Dynamic(_) => None,
                    },
                    backoff: retries.backoff,
                    timeout,
                };
                // A selected restart can name this occurrence while its
                // callback awaits I/O. The generation fences its final write.
                let mut running = data(json!({
                    "kind": "do",
                    "status": "running",
                    "attempt": attempt - 1,
                    "lastAttempt": attempt,
                    "ordinal": ordinal,
                    "rollbackEligible": rollback.is_some(),
                    "sensitive": config.sensitive,
                    "config": config_value(&config, &context)?,
                }))?;
                if let Some(pending) = record
                    .as_ref()
                    .map(|r| property(r, "pendingStream"))
                    .transpose()?
                    .flatten()
                {
                    set(&mut running, "pendingStream", &pending)?;
                }
                shared.put(&key, &running)?;
                let mut future = scoped(&name, || Box::pin(callback(context.clone())));
                let result = match crate::asyncrt::timeout(
                    std::time::Duration::from_millis(timeout_ms.min(u64::MAX as f64) as u64),
                    &mut future,
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        // The timeout discards the result; the callback keeps
                        // its application I/O under the original event.
                        shared.context.wait_until(async move {
                            let _ = future.await;
                            Ok(())
                        })?;
                        Err(error(format!(
                            "step {:?} timed out after {timeout_ms} ms (attempt {attempt})",
                            String::from_utf16_lossy(&name),
                        )))
                    }
                };
                let result = match result {
                    Ok(value) => match check_value(&value, "the step return value") {
                        Ok(()) => Ok(value),
                        Err(error) => Err(StepFailure {
                            record: failure(&error).record,
                            non_retryable: true,
                        }
                        .into()),
                    },
                    Err(error) => Err(error),
                };
                match result {
                    Ok(value) => {
                        let mut record = data(json!({
                            "kind": "do",
                            "status": "completed",
                            "ordinal": ordinal,
                            "lastAttempt": attempt,
                            "rollbackEligible": rollback.is_some(),
                            "sensitive": config.sensitive,
                            "stream": stream,
                        }))?;
                        set(&mut record, "value", &value)?;
                        shared.put(&key, &record)?;
                        Ok(StepResult::Value(value))
                    }
                    Err(error) => {
                        let pending = shared
                            .get(&key)?
                            .map(|r| property(&r, "pendingStream"))
                            .transpose()?
                            .flatten();
                        if failure(&error).non_retryable || attempt as f64 > retries.limit {
                            let mut record = data(json!({
                                "kind": "do",
                                "status": "failed",
                                "ordinal": ordinal,
                                "lastAttempt": attempt,
                                "rollbackEligible": rollback.is_some(),
                            }))?;
                            set(&mut record, "error", &error_value(&error)?)?;
                            if let Some(pending) = &pending {
                                set(&mut record, "pendingStream", pending)?;
                            }
                            shared.put(&key, &record)?;
                            return Err(error);
                        }
                        let mut retrying = data(json!({
                            "kind": "do",
                            "status": "failed",
                            "attempt": attempt,
                            "lastAttempt": attempt,
                            "ordinal": ordinal,
                            "rollbackEligible": rollback.is_some(),
                        }))?;
                        set(&mut retrying, "error", &error_value(&error)?)?;
                        if let Some(pending) = &pending {
                            set(&mut retrying, "pendingStream", pending)?;
                        }
                        let scheduled = async {
                            let delay = match retries.delay {
                                Delay::Fixed(delay) => delay,
                                Delay::Dynamic(callback) => {
                                    scoped(&name, || callback(context, failure(&error).record))
                                        .await?
                                }
                            };
                            retry_at(&delay, retries.backoff, attempt)
                        }
                        .await;
                        let at = match scheduled {
                            Ok(at) => at,
                            Err(error) => {
                                set(&mut retrying, "error", &error_value(&error)?)?;
                                shared.put(&key, &retrying)?;
                                return Err(error);
                            }
                        };
                        put_field(&mut retrying, "status", json!("retrying"))?;
                        put_number(&mut retrying, "nextAt", at)?;
                        shared.put(&key, &retrying)?;
                        Ok(StepResult::Block(at))
                    }
                }
            })
        })
    }

    /// Compatibility spelling for [`Self::run`].
    pub fn do_step<F, Fut>(
        &self,
        name: impl Into<StepName>,
        callback: F,
    ) -> HandlerFuture<StructuredValue>
    where
        F: FnOnce(Attempt) -> Fut + Send + 'static,
        Fut: Future<Output = Result<StructuredValue>> + Send + 'static,
    {
        self.run(name, callback)
    }

    /// Compatibility spelling for [`Self::run_with`].
    pub fn do_with_config<F, Fut>(
        &self,
        name: impl Into<StepName>,
        config: StepConfig,
        callback: F,
    ) -> HandlerFuture<StructuredValue>
    where
        F: FnOnce(Attempt) -> Fut + Send + 'static,
        Fut: Future<Output = Result<StructuredValue>> + Send + 'static,
    {
        self.run_with(name, config, callback)
    }

    /// Sleep durably using a duration string, milliseconds, or a Rust duration.
    pub fn sleep(
        &self,
        name: impl Into<StepName>,
        duration: impl Into<Duration>,
    ) -> HandlerFuture<()> {
        let checked = duration.into().milliseconds().and_then(|ms| {
            check_horizon(ms, "sleep")?;
            Ok(ms)
        });
        self.sleep_deadline(name, checked.map(|ms| (ms, false)))
    }

    pub fn sleep_until(&self, name: impl Into<StepName>, timestamp_ms: f64) -> HandlerFuture<()> {
        let checked = (|| {
            ensure!(
                timestamp_ms.is_finite() && timestamp_ms < i64::MAX as f64,
                "WORKFLOW_ERROR: invalid sleepUntil timestamp"
            );
            check_horizon((timestamp_ms - now() as f64).max(0.0), "sleepUntil")?;
            Ok((timestamp_ms, true))
        })();
        self.sleep_deadline(name, checked)
    }

    fn sleep_deadline(
        &self,
        name: impl Into<StepName>,
        checked: Result<(f64, bool)>,
    ) -> HandlerFuture<()> {
        let name = name.into().0;
        let checked = check_step_name(&name).and(checked);
        let (deadline, absolute) = match checked {
            Ok(value) => value,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let result = self.frame(name, move |shared, key, _, ordinal| {
            Box::pin(async move {
                let record = shared.kind(shared.get(&key)?, "sleep")?;
                if record.as_ref().is_some_and(|record| {
                    text(record, "status").is_ok_and(|status| status == "completed")
                }) {
                    return Ok(StepResult::Value(StructuredValue::undefined()));
                }
                if shared.rollback_mode { return Err(StepFailure::non_retryable("Workflow is rolling back").into()); }
                let (at, ordinal) = match &record {
                    Some(record) => (number(record, "deadline")?, number(record, "ordinal")?),
                    None => {
                        let at = if absolute { deadline } else { now() as f64 + deadline };
                        ensure!(!absolute || at >= now() as f64, "WORKFLOW_ERROR: sleepUntil needs a future timestamp");
                        shared.put(&key, &data(json!({
                            "kind": "sleep", "status": "sleeping", "deadline": at, "ordinal": ordinal,
                        }))?)?;
                        (at, ordinal as f64)
                    }
                };
                if at <= now() as f64 {
                    shared.put(&key, &data(json!({
                        "kind": "sleep", "status": "completed", "ordinal": ordinal,
                    }))?)?;
                    Ok(StepResult::Value(StructuredValue::undefined()))
                } else {
                    Ok(StepResult::Block(at))
                }
            })
        });
        Box::pin(async move {
            result.await?;
            Ok(())
        })
    }

    /// Wait for an event type, optionally configuring its durable timeout.
    ///
    /// A string uses the default 24-hour timeout. Use [`WaitOptions`] to
    /// specify a timeout, for example `WaitOptions::new("approve").timeout("1 hour")`.
    pub fn wait_for_event(
        &self,
        name: impl Into<StepName>,
        options: impl Into<WaitOptions>,
    ) -> HandlerFuture<StructuredValue> {
        let options = options.into();
        self.wait_for_event_with_timeout(name, options.event_type, options.timeout)
    }

    /// Explicit-argument form of [`Self::wait_for_event`].
    pub fn wait_for_event_with_timeout(
        &self,
        name: impl Into<StepName>,
        event_type: String,
        timeout: Option<Duration>,
    ) -> HandlerFuture<StructuredValue> {
        let name = name.into().0;
        let checked = (|| {
            check_step_name(&name)?;
            check_event_type(&event_type)?;
            let ms = timeout
                .unwrap_or(Duration::Milliseconds(86400000.0))
                .milliseconds()?;
            ensure!(
                ms >= 1000.0,
                "WORKFLOW_ERROR: event timeout must be at least 1 second"
            );
            check_horizon(ms, "event timeout")?;
            Ok(ms)
        })();
        let ms = match checked {
            Ok(ms) => ms,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        self.frame(name.clone(), move |shared, key, _, ordinal| {
            Box::pin(async move {
                let mut record = shared.kind(shared.get(&key)?, "event")?;
                if let Some(record) = &record {
                    match text(record, "status")?.as_str() {
                        "completed" => return Ok(StepResult::Value(
                            property(record, "value")?.unwrap_or_else(StructuredValue::undefined),
                        )),
                        "failed" => return Err(revive(record)?),
                        _ => {}
                    }
                }
                if shared.rollback_mode { return Err(StepFailure::non_retryable("Workflow is rolling back").into()); }
                if record.is_none() {
                    let new = data(json!({
                        "kind": "event", "status": "waiting", "type": event_type,
                        "deadline": now() as f64 + ms, "ordinal": ordinal,
                    }))?;
                    shared.put(&key, &new)?;
                    record = Some(new);
                }
                let record = record.unwrap();
                let deadline = number(&record, "deadline")?;
                // A late event cannot satisfy an already-expired wait even
                // if the cell only reopens after both deadlines have passed.
                if deadline <= now() as f64 {
                    let error = error(format!(
                        "waitForEvent {:?} timed out waiting for event of type {:?}",
                        String::from_utf16_lossy(&name), text(&record, "type")?,
                    ));
                    let mut failed = data(json!({
                        "kind": "event", "status": "failed", "ordinal": number(&record, "ordinal")?,
                    }))?;
                    set(&mut failed, "error", &error_value(&error)?)?;
                    shared.put(&key, &failed)?;
                    return Err(error);
                }
                let consumed = shared.track(shared.storage.transaction_sync(|transaction| {
                    let current = read(transaction, &key)?.ok_or_else(|| error("event ledger disappeared"))?;
                    if text(&current, "status")? == "completed" { return property(&current, "value"); }
                    if text(&current, "status")? == "failed" { return Err(revive(&current)?); }
                    for (event_key, event) in list(transaction, &format!("{}event.", prefix(&shared.generation)))? {
                        if text(&event, "type")? != text(&current, "type")? { continue; }
                        let mut value = data(json!({"type": text(&event, "type")?}))?;
                        if let Some(payload) = property(&event, "payload")? { set(&mut value, "payload", &payload)?; }
                        set(&mut value, "timestamp", &StructuredValue {
                            root: CloneValue::Reference(0),
                            objects: vec![cellar_native::clone::Object::Date(number(&event, "timestampMs")?)],
                        })?;
                        transaction.delete(&event_key)?;
                        let mut completed = data(json!({
                            "kind": "event", "status": "completed", "ordinal": number(&current, "ordinal")?,
                        }))?;
                        set(&mut completed, "value", &value)?;
                        let mut meta = required(transaction)?;
                        ensure!(text(&meta, "generation")? == shared.generation && !terminal(&meta)?, "WORKFLOW_ERROR: workflow generation retired");
                        super::history::step_change(transaction, &mut meta, &key, Some(&current), &completed)?;
                        transaction.put_clone(&key, &completed)?;
                        transaction.put_clone(META, &meta)?;
                        return Ok(Some(value));
                    }
                    Ok(None)
                }))?;
                Ok(match consumed {
                    Some(value) => StepResult::Value(value),
                    None => StepResult::Block(deadline),
                })
            })
        })
    }
}

pub(super) fn validate_config(config: &StepConfig) -> Result<(Duration, f64)> {
    let checked = (|| {
        ensure!(
            !config.rollback,
            "WORKFLOW_ERROR: supply a RollbackOptions handler with run_with_rollback"
        );
        if let Some(retries) = &config.retries {
            ensure!(
                retries.limit.is_finite()
                    && (0.0..=limits().max_retries as f64).contains(&retries.limit),
                "WORKFLOW_ERROR: retry limit must be from zero to {}",
                limits().max_retries
            );
            if let Delay::Fixed(delay) = &retries.delay {
                delay.milliseconds()?;
            }
        }
        let timeout = config
            .timeout
            .clone()
            .unwrap_or(Duration::Milliseconds(600000.0));
        let ms = timeout.milliseconds()?;
        ensure!(
            ms > 0.0 && ms.is_finite(),
            "WORKFLOW_ERROR: step timeout must be finite and above zero"
        );
        Ok((timeout, ms))
    })();
    checked
}

pub(super) fn default_retries() -> Retries {
    Retries::new(5, 10000.0)
}

pub(super) fn retry_at(delay: &Duration, backoff: Option<Backoff>, attempt: u64) -> Result<f64> {
    let factor = match backoff {
        Some(Backoff::Exponential) => 2.0_f64.powf(attempt as f64 - 1.0),
        Some(Backoff::Linear) => attempt as f64,
        _ => 1.0,
    };
    let ms = delay.milliseconds()? * factor;
    check_horizon(ms, "retry delay")?;
    Ok(now() as f64 + ms)
}

pub(super) fn config_value(config: &StepConfig, ctx: &Attempt) -> Result<serde_json::Value> {
    let delay = match &ctx.retry_delay {
        Some(delay) => serde_json::to_value(delay)?,
        None => json!("[dynamic]"),
    };
    let mut value =
        json!({"retries":{"limit":ctx.retry_limit,"delay":delay},"timeout":ctx.timeout});
    if let Some(backoff) = ctx.backoff {
        value["retries"]["backoff"] = json!(match backoff {
            Backoff::Constant => "constant",
            Backoff::Linear => "linear",
            Backoff::Exponential => "exponential",
        });
    }
    if config.sensitive {
        value["sensitive"] = json!("output");
    }
    Ok(value)
}

enum Outcome {
    Complete(StructuredValue),
    Error(anyhow::Error),
    Suspend(f64),
    Pause,
}

struct Run {
    shared: Arc<Shared>,
    context: Context,
    handler: Option<HandlerFuture<StructuredValue>>,
    tasks: Vec<Task>,
    activity: u64,
    quiet: bool,
    stall: Option<crate::asyncrt::Sleep>,
}

impl Future for Run {
    type Output = Result<Outcome>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.shared.wake.register(cx.waker());
        let result = this.handler.as_mut().map(|handler| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler.as_mut().poll(cx)))
                .unwrap_or_else(|panic| Poll::Ready(Err(crate::native_worker::panic_error(panic))))
        });
        this.tasks
            .append(&mut this.shared.state.lock().unwrap().queued);
        this.tasks
            .retain_mut(|task| task.as_mut().poll(cx).is_pending());
        let mut state = this.shared.state.lock().unwrap();
        if let Some(error) = &state.storage_failure {
            return Poll::Ready(Err(anyhow!(error.clone())));
        }
        let mut outcome = match result {
            Some(Poll::Ready(Ok(value))) => Some(Outcome::Complete(value)),
            Some(Poll::Ready(Err(error))) => Some(Outcome::Error(error)),
            _ => None,
        };
        if outcome.is_none() {
            if state.running > 0 {
                this.stall = None;
                this.quiet = false;
                return Poll::Pending;
            }
            if state.activity != this.activity || !this.quiet {
                this.stall = None;
                this.activity = state.activity;
                this.quiet = true;
                drop(state);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            if state.pause {
                outcome = Some(Outcome::Pause)
            } else if let Some(at) = state.blocked.iter().copied().min_by(f64::total_cmp) {
                outcome = Some(Outcome::Suspend(at))
            } else {
                let timer = this.stall.get_or_insert_with(|| {
                    crate::asyncrt::sleep(std::time::Duration::from_secs(60))
                });
                if timer.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                outcome=Some(Outcome::Error(error("run() has awaited non-step work for 60 seconds with no running or blocked step; a replay cannot resume this wait")));
            }
        }
        state.finished = true;
        drop(state);
        if !this.tasks.is_empty() {
            let mut tasks = std::mem::take(&mut this.tasks);
            this.context.wait_until(async move {
                std::future::poll_fn(|cx| {
                    tasks.retain_mut(|task| task.as_mut().poll(cx).is_pending());
                    if tasks.is_empty() {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                })
                .await;
                Ok(())
            })?;
        }
        Poll::Ready(Ok(outcome.unwrap()))
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().unwrap();
        state.finished = true;
        state.queued.clear();
    }
}

struct DriveGuard(Arc<InstanceCell>);

impl Drop for DriveGuard {
    fn drop(&mut self) {
        let mut drive = self.0.drive.lock().unwrap();
        drive.active = false;
        for waiter in std::mem::take(&mut drive.waiters) {
            let _ = waiter.send(());
        }
    }
}

pub(super) async fn alarm(cell: Arc<InstanceCell>, context: Context) -> Result<()> {
    let wait = {
        let mut drive = cell.drive.lock().unwrap();
        if drive.active {
            drive.nudge = Some(now());
            let (send, receive) = tokio::sync::oneshot::channel();
            drive.waiters.push(send);
            Some(receive)
        } else {
            drive.active = true;
            None
        }
    };
    if let Some(wait) = wait {
        let _ = wait.await;
        let nudge = cell.drive.lock().unwrap().nudge.take();
        if nudge.is_some() {
            if let Some(meta) = read(&cell.storage, META)? {
                if !terminal(&meta)?
                    && !matches!(
                        text(&meta, "status")?.as_str(),
                        "paused" | "waitingForPause"
                    )
                {
                    cell.storage.set_alarm(now())?;
                }
            }
        }
        return Ok(());
    }
    let _driving = DriveGuard(cell.clone());
    let Some(meta) = cell.prepare_alarm()? else {
        return Ok(());
    };
    let generation = text(&meta, "generation")?;
    let rollback_mode = property(&meta, "rollbackTarget")?.is_some();
    let shared = Arc::new(Shared {
        storage: cell.storage.clone(),
        context: context.clone(),
        generation: generation.clone(),
        changed: cell.changed.clone(),
        rollback_mode,
        state: Mutex::new(State::default()),
        wake: AtomicWaker::new(),
    });
    if rollback_mode && super::rollback::required_steps(&cell.storage, &generation)?.is_empty() {
        super::rollback::finish(&shared, None)?;
        return Ok(());
    }
    let class = cell.classes.get(&text(&meta, "workflowName")?).cloned();
    let result = if let Some(class) = class {
        if !settle(&cell.storage, &generation, |meta, _| {
            if rollback_mode {
                put_field(meta, "status", json!("rollingBack"))
            } else {
                put_field(meta, "status", json!("running"))
            }
        })? {
            return Ok(());
        }
        let event = Event {
            payload: property(&meta, "params")?.unwrap_or_else(StructuredValue::undefined),
            timestamp_ms: number(&meta, "createdMs")? as i64,
            instance_id: text(&meta, "instanceId")?,
            workflow_name: text(&meta, "workflowName")?,
            schedule: property(&meta, "schedule")?
                .map(|s| -> Result<CronSchedule> { Ok(serde_json::from_value(s.to_json()?)?) })
                .transpose()?,
        };
        let mut worker_context = context.clone();
        worker_context.identity = None;
        let handler = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            cell.application.clone().run_workflow(
                class,
                worker_context,
                event,
                Step(shared.clone()),
            )
        }))
        .unwrap_or_else(|panic| {
            let error = crate::native_worker::panic_error(panic);
            Box::pin(async move { Err(error) })
        });
        Run {
            shared: shared.clone(),
            context: context.clone(),
            handler: Some(handler),
            tasks: Vec::new(),
            activity: 0,
            quiet: false,
            stall: None,
        }
        .await?
    } else {
        Outcome::Error(error(format!(
            "workflow {:?} is not declared in this deployment",
            text(&meta, "workflowName")?
        )))
    };
    let current = read(&cell.storage, META)?;
    let Some(current_meta) = &current else {
        return Ok(());
    };
    if text(current_meta, "generation")? != generation || terminal(current_meta)? {
        return Ok(());
    }
    if rollback_mode {
        match super::rollback::drive(shared.clone()).await {
            Ok(Some(at)) => {
                settle(&cell.storage, &generation, |_, transaction| {
                    transaction.set_alarm(at as i64)
                })?;
            }
            Ok(None) => {}
            Err(error) => super::rollback::finish(&shared, Some(&error))?,
        }
        return Ok(());
    }
    if property(current_meta, "rollbackTarget")?.is_some() {
        settle(&cell.storage, &generation, |_, transaction| {
            transaction.set_alarm(now())
        })?;
        return Ok(());
    }
    if matches!(result, Outcome::Pause)
        || current.as_ref().is_some_and(|meta| {
            text(meta, "generation").is_ok_and(|current| current == generation)
                && text(meta, "status").is_ok_and(|status| status == "waitingForPause")
        })
    {
        settle(&cell.storage, &generation, |meta, transaction| {
            if text(meta, "status")? == "waitingForPause" {
                put_field(meta, "status", json!("paused"))?;
                put_field(meta, "pausedMs", json!(now()))?;
                transaction.delete_alarm()?;
            }
            Ok(())
        })?;
        return Ok(());
    }
    let result = match result {
        Outcome::Complete(value) => match check_value(&value, "the run() return value") {
            Ok(()) => Outcome::Complete(value),
            Err(error) => Outcome::Error(error),
        },
        result => result,
    };
    match result {
        Outcome::Suspend(at) => {
            let mut at = at as i64;
            if let Some(nudge) = cell.storage.get_alarm()? {
                at = at.min(nudge)
            }
            if let Some(nudge) = cell.drive.lock().unwrap().nudge.take() {
                at = at.min(nudge)
            }
            settle(&cell.storage, &generation, |meta, transaction| {
                put_field(meta, "status", json!("waiting"))?;
                transaction.set_alarm(at)
            })?;
        }
        Outcome::Complete(value) => {
            settle(&cell.storage, &generation, |meta, transaction| {
                put_field(meta, "status", json!("complete"))?;
                if !matches!(value.root, CloneValue::Undefined) {
                    set(meta, "output", &value)?;
                }
                finish_retention(meta, transaction)
            })?;
        }
        Outcome::Error(error) => {
            settle(&cell.storage, &generation, |meta, transaction| {
                set(meta, "error", &error_value(&error)?)?;
                if !super::rollback::required_steps(transaction, &generation)?.is_empty() {
                    put_field(meta, "status", json!("rollingBack"))?;
                    put_field(meta, "rollbackTarget", json!("errored"))?;
                    set(meta, "rollbackError", &error_value(&error)?)?;
                    transaction.set_alarm(now())
                } else {
                    put_field(meta, "status", json!("errored"))?;
                    finish_retention(meta, transaction)
                }
            })?;
        }
        Outcome::Pause => {}
    }
    Ok(())
}
