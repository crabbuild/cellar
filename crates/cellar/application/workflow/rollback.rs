// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::cell::*;
use super::replay::{Registration, Shared};
use super::*;

#[derive(Clone)]
pub struct RollbackContext {
    pub ctx: Attempt,
    pub error: ErrorRecord,
    pub output: Option<StructuredValue>,
    origin: Arc<Shared>,
    stream: bool,
}

impl RollbackContext {
    pub fn stream_output(&self, context: &Context) -> Result<Option<StreamOutput>> {
        context.check_turn()?;
        ensure!(
            Arc::ptr_eq(&context.io, &self.origin.context.io),
            "WORKFLOW_ERROR: rollback output belongs to another invocation"
        );
        if !self.stream {
            return Ok(None);
        }
        self.output
            .as_ref()
            .map(|output| StreamOutput::local(&self.origin, output))
            .transpose()
    }
}

type Callback = Arc<dyn Fn(RollbackContext) -> crate::application::HandlerFuture<()> + Send + Sync>;

#[derive(Clone)]
pub struct RollbackOptions {
    pub config: StepConfig,
    pub(super) callback: Callback,
}

impl RollbackOptions {
    pub fn new<F, Fut>(callback: F) -> Self
    where
        F: Fn(RollbackContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        Self {
            config: StepConfig::default(),
            callback: Arc::new(move |context| Box::pin(callback(context))),
        }
    }

    pub fn config(mut self, config: StepConfig) -> Self {
        self.config = config;
        self
    }
}

pub(super) fn required_steps(
    storage: &crate::application::Storage,
    generation: &str,
) -> Result<Vec<(String, StructuredValue)>> {
    let mut records = Vec::new();
    for (key, record) in list(storage, &format!("{}step.", prefix(generation)))? {
        if property(&record, "rollbackEligible")?
            .is_some_and(|v| v.root == CloneValue::Boolean(true))
        {
            let ordinal = number(&record, "ordinal")?;
            ensure!(
                ordinal.is_finite() && ordinal >= 1.0 && ordinal.fract() == 0.0,
                "WORKFLOW_ERROR: invalid rollback ordinal"
            );
            records.push((ordinal, key, record));
        }
    }
    records.sort_by(|left, right| right.0.total_cmp(&left.0));
    Ok(records
        .into_iter()
        .map(|(_, key, record)| (key, record))
        .collect())
}

pub(super) async fn drive(shared: Arc<Shared>) -> Result<Option<f64>> {
    let meta = required(&shared.storage)?;
    let cause = ErrorRecord::from_value(
        property(&meta, "rollbackError")?.ok_or_else(|| error("missing rollback cause"))?,
    )?;
    let records = required_steps(&shared.storage, &shared.generation)?;
    for (forward_key, forward) in records {
        if !admitted(&shared)? {
            return Ok(None);
        }
        let registration = shared.registration(&forward_key).ok_or_else(|| {
            error("a recorded rollback handler is missing from the current workflow")
        })?;
        if let Some(at) = step(
            shared.clone(),
            forward_key,
            forward,
            registration,
            cause.clone(),
        )
        .await?
        {
            return Ok(if admitted(&shared)? { Some(at) } else { None });
        }
    }
    if admitted(&shared)? {
        finish(&shared, None)?;
    }
    Ok(None)
}

fn admitted(shared: &Shared) -> Result<bool> {
    let Some(meta) = read(&shared.storage, META)? else {
        return Ok(false);
    };
    if text(&meta, "generation")? != shared.generation
        || terminal(&meta)?
        || property(&meta, "rollbackTarget")?.is_none()
    {
        return Ok(false);
    }
    if matches!(
        text(&meta, "status")?.as_str(),
        "paused" | "waitingForPause"
    ) {
        settle(&shared.storage, &shared.generation, |meta, transaction| {
            if text(meta, "status")? == "waitingForPause" {
                put_field(meta, "status", json!("paused"))?;
                put_field(meta, "pausedMs", json!(now()))?;
                transaction.delete_alarm()?;
            }
            Ok(())
        })?;
        return Ok(false);
    }
    Ok(true)
}

async fn step(
    shared: Arc<Shared>,
    forward_key: String,
    forward: StructuredValue,
    registration: Registration,
    cause: ErrorRecord,
) -> Result<Option<f64>> {
    let key = forward_key.replacen("step.", "rollback.", 1);
    let previous = shared.get(&key)?;
    if previous
        .as_ref()
        .is_some_and(|r| text(r, "status").is_ok_and(|s| s == "completed"))
    {
        return Ok(None);
    }
    if let Some(record) = &previous {
        if text(record, "status")? == "retrying" && number(record, "nextAt")? > now() as f64 {
            return Ok(Some(number(record, "nextAt")?));
        }
        if text(record, "status")? == "failed" {
            return Err(super::replay::revive(record)?);
        }
    }
    let config = registration.options.config;
    let (timeout, timeout_ms) = super::replay::validate_config(&config)?;
    let retries = config
        .retries
        .clone()
        .unwrap_or_else(super::replay::default_retries);
    let attempt = previous
        .as_ref()
        .map(|r| number(r, "attempt"))
        .transpose()?
        .unwrap_or(0.0) as u64
        + 1;
    let ctx = Attempt {
        name: registration.name.clone(),
        count: registration.count,
        attempt,
        retry_limit: retries.limit,
        retry_delay: match &retries.delay {
            Delay::Fixed(d) => Some(d.clone()),
            Delay::Dynamic(_) => None,
        },
        backoff: retries.backoff,
        timeout,
    };
    shared.put(
        &key,
        &data(
            json!({"kind":"rollback", "status":"running", "attempt":attempt-1,"lastAttempt":attempt,
        "config":super::replay::config_value(&config, &ctx)?}),
        )?,
    )?;
    let output = property(&forward, "value")?;
    let is_stream =
        property(&forward, "stream")?.is_some_and(|v| v.root == CloneValue::Boolean(true));
    let mut future = super::replay::scoped(&registration.name, || {
        (registration.options.callback)(RollbackContext {
            ctx: ctx.clone(),
            error: cause,
            output,
            origin: shared.clone(),
            stream: is_stream,
        })
    });
    let result = match crate::asyncrt::timeout(
        std::time::Duration::from_millis(timeout_ms.min(u64::MAX as f64) as u64),
        &mut future,
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            shared.context.wait_until(async move {
                let _ = future.await;
                Ok(())
            })?;
            Err(error("rollback attempt timed out"))
        }
    };
    match result {
        Ok(()) => {
            shared.put(
                &key,
                &data(json!({"kind":"rollback","status":"completed","lastAttempt":attempt}))?,
            )?;
            Ok(None)
        }
        Err(error) => {
            let failure = super::replay::failure(&error);
            let mut record = data(
                json!({"kind":"rollback","status":"failed","attempt":attempt,"lastAttempt":attempt}),
            )?;
            set(&mut record, "error", &super::replay::error_value(&error)?)?;
            if !failure.non_retryable && attempt as f64 <= retries.limit {
                let scheduled = async {
                    let delay = match retries.delay {
                        Delay::Fixed(delay) => delay,
                        Delay::Dynamic(callback) => {
                            super::replay::scoped(&registration.name, || {
                                callback(ctx, failure.record)
                            })
                            .await?
                        }
                    };
                    super::replay::retry_at(&delay, retries.backoff, attempt)
                }
                .await;
                let at = match scheduled {
                    Ok(at) => at,
                    Err(error) => {
                        set(&mut record, "error", &super::replay::error_value(&error)?)?;
                        shared.put(&key, &record)?;
                        return Err(error);
                    }
                };
                put_field(&mut record, "status", json!("retrying"))?;
                put_number(&mut record, "nextAt", at)?;
                shared.put(&key, &record)?;
                Ok(Some(at))
            } else {
                shared.put(&key, &record)?;
                Err(error)
            }
        }
    }
}

pub(super) fn finish(shared: &Shared, failure: Option<&anyhow::Error>) -> Result<()> {
    settle(&shared.storage, &shared.generation, |meta, transaction| {
        if property(meta, "rollbackTarget")?.is_none() {
            return Ok(());
        }
        let target = text(meta, "rollbackTarget")?;
        let mut report = data(
            json!({"outcome":if failure.is_some() {"failed"} else {"complete"}, "error":null}),
        )?;
        if let Some(error) = failure {
            set(&mut report, "error", &super::replay::error_value(error)?)?;
        }
        set(meta, "rollback", &report)?;
        super::history::append(
            transaction,
            meta,
            if failure.is_some() {
                InstanceEventType::RollbackErrored
            } else {
                InstanceEventType::RollbackCompleted
            },
            data(json!({}))?,
        )?;
        put_field(meta, "status", json!(target))?;
        remove(meta, "rollbackTarget")?;
        remove(meta, "rollbackError")?;
        finish_retention(meta, transaction)
    })?;
    Ok(())
}
