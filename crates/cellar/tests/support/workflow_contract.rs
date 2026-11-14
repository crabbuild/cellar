// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared Workflow behavior and assertions for Fleet and shared-group execution.

use anyhow::{anyhow, Result};
use cellar::application::workflow::{
    Backoff, BatchCreateOptions, CreateOptions, Delay, Duration as WfDuration, Event,
    InstanceEventType, RestartFrom, RestartOptions, Retention, Retries, RollbackOptions, SendEvent,
    StepConfig, StepFailure, StepKind, StepSelector, Steps, SubscribeOptions, TerminateOptions,
    WaitOptions,
};
use cellar::application::{Cell, Context, HandlerFuture, Invocation, Output, Storage};
use cellar::engine_api::{HttpResponse, RpcData};
use cellar::native::clone::{Object, StructuredValue, Value as CloneValue};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::sync::Notify;

pub(crate) const SCRIPT: &str = "native-workflow-cell";

pub(crate) fn sink_scope() -> String {
    cellar_native::cell_id::Namespace::new(
        "Sink",
        format!("cells:v1:{}:{SCRIPT}:Sink", SCRIPT.len()),
    )
    .id_from_name("effects")
    .scope()
}

#[derive(Default)]
pub(crate) struct Signals {
    pub(crate) first: AtomicUsize,
    pub(crate) last: AtomicUsize,
    pub(crate) retries: Mutex<Vec<u64>>,
    pub(crate) delays: Mutex<Vec<u64>>,
    pub(crate) permanent: AtomicUsize,
    pub(crate) parallel: Mutex<Vec<u64>>,
    pub(crate) capped: AtomicUsize,
    pub(crate) timeout_started: Notify,
    pub(crate) release_timeout: Notify,
    pub(crate) timeout_finished: AtomicBool,
    pub(crate) race_started: Notify,
    pub(crate) release_race: Notify,
    pub(crate) race_calls: AtomicUsize,
    pub(crate) hung_started: Notify,
    pub(crate) hung_dropped: AtomicBool,
    pub(crate) v2_forward: AtomicUsize,
    pub(crate) v2_rollback: Mutex<Vec<(String, u64)>>,
    pub(crate) stream_runs: AtomicUsize,
    pub(crate) extra_cells: Mutex<Vec<String>>,
    pub(crate) subscribed: Notify,
    pub(crate) rollback_started: Notify,
    pub(crate) release_rollback: Notify,
    pub(crate) stream_retries: AtomicUsize,
    pub(crate) stream_compensations: AtomicUsize,
    pub(crate) bad_delay_calls: AtomicUsize,
}

struct Dropped(Arc<Signals>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.hung_dropped.store(true, Ordering::SeqCst);
    }
}

pub(crate) struct Sink(pub(crate) Storage);

impl Cell for Sink {
    fn invoke(self: Arc<Self>, _: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc {
                args: RpcData::Json(value),
                ..
            } = invocation
            else {
                return Err(anyhow!("expected effect RPC"));
            };
            let mut effects = self.0.get_json("effects")?.unwrap_or_else(|| json!([]));
            effects
                .as_array_mut()
                .unwrap()
                .push(serde_json::from_str::<Value>(&value)?);
            self.0.put_json("effects", &effects)?;
            Ok(Output::Rpc(RpcData::Json("null".into())))
        })
    }
}

pub(crate) async fn note(context: &Context, value: &str) -> Result<()> {
    context
        .rpc_cell(
            sink_scope(),
            None,
            "note".into(),
            RpcData::Json(serde_json::to_string(value)?),
        )
        .await?;
    Ok(())
}

pub(crate) fn field(value: &StructuredValue, key: &str) -> StructuredValue {
    value
        .property(&key.encode_utf16().collect::<Vec<_>>())
        .unwrap()
        .unwrap()
}

pub(crate) fn complex() -> StructuredValue {
    StructuredValue {
        root: CloneValue::Reference(0),
        objects: vec![Object::Object(vec![
            ("self".into(), CloneValue::Reference(0)),
            ("lone".into(), CloneValue::String(vec![0xd800])),
            ("zero".into(), CloneValue::Number(-0.0)),
        ])],
    }
}

pub(crate) fn check_complex(value: &StructuredValue) {
    assert_eq!(field(value, "self").root, value.root);
    assert_eq!(field(value, "lone").root, CloneValue::String(vec![0xd800]));
    let CloneValue::Number(value) = field(value, "zero").root else {
        panic!("number");
    };
    assert_eq!(value.to_bits(), (-0.0_f64).to_bits());
}

pub(crate) async fn workflow(
    context: Context,
    event: Event,
    steps: Steps,
    signals: Arc<Signals>,
) -> Result<StructuredValue> {
    assert!(context.identity().is_none());
    assert!(context.storage().is_err());
    assert_eq!(context.var("SETTING"), Some("native"));
    assert_eq!(event.workflow_name, "orders");
    if let Some(schedule) = event.schedule {
        return steps
            .run("schedule", move |_| async move {
                StructuredValue::from_json(serde_json::to_value(schedule)?)
            })
            .await;
    }
    match event.instance_id.as_str() {
        "main" => {
            let payload = event.payload;
            let counted = signals.clone();
            let ctx = context.clone();
            let value = steps
                .run_with(
                    "first",
                    StepConfig::default().input(payload.clone()),
                    move |attempt| async move {
                        assert_eq!(
                            (attempt.count, attempt.attempt, attempt.retry_limit),
                            (1, 1, 5.0)
                        );
                        counted.first.fetch_add(1, Ordering::SeqCst);
                        note(&ctx, "first").await?;
                        Ok(payload)
                    },
                )
                .await?;
            steps.sleep("nap", 5.0).await?;
            let received = steps
                .wait_for_event("approval", WaitOptions::new("approve").timeout("1 second"))
                .await?;
            check_complex(&field(&received, "payload"));
            let counted = signals.clone();
            steps
                .run("last", move |_| async move {
                    counted.last.fetch_add(1, Ordering::SeqCst);
                    Ok(value)
                })
                .await
        }
        "retry" => {
            let delay_signals = signals.clone();
            let config = StepConfig {
                retries: Some(Retries {
                    limit: 2.0,
                    delay: Delay::Dynamic(Arc::new(move |attempt, error| {
                        delay_signals.delays.lock().unwrap().push(attempt.attempt);
                        assert_eq!(error.message, vec![0xd800]);
                        assert!(attempt.retry_delay.is_none());
                        Box::pin(async { Ok(WfDuration::Milliseconds(0.0)) })
                    })),
                    backoff: Some(Backoff::Linear),
                }),
                ..Default::default()
            };
            steps
                .run_with("retry", config, move |attempt| async move {
                    signals.retries.lock().unwrap().push(attempt.attempt);
                    if attempt.attempt < 3 {
                        return Err(StepFailure {
                            record: cellar::application::workflow::ErrorRecord {
                                name: "Error".encode_utf16().collect(),
                                message: vec![0xd800],
                            },
                            non_retryable: false,
                        }
                        .into());
                    }
                    StructuredValue::from_json(json!("retried"))
                })
                .await
        }
        "bad-delay" => {
            let failure = steps
                .run_with(
                    "invalid-delay",
                    StepConfig {
                        retries: Some(Retries {
                            limit: 1.0,
                            delay: Delay::Dynamic(Arc::new(|_, _| {
                                Box::pin(async { Ok(WfDuration::Milliseconds(f64::INFINITY)) })
                            })),
                            backoff: Some(Backoff::Constant),
                        }),
                        ..Default::default()
                    },
                    move |_| async move {
                        signals.bad_delay_calls.fetch_add(1, Ordering::SeqCst);
                        Err(anyhow!("forward failure"))
                    },
                )
                .await
                .unwrap_err();
            assert!(failure.to_string().contains("duration"));
            steps.wait_for_event("continue", "approve").await
        }
        "rollback" | "terminate-rollback" | "rollback-failed" | "rollback-held" => {
            let terminating = event.instance_id == "terminate-rollback";
            let failing = event.instance_id == "rollback-failed";
            let held = event.instance_id == "rollback-held";
            for name in ["reserve", "charge"] {
                let forward = signals.clone();
                let reverse = signals.clone();
                let ctx = context.clone();
                steps
                    .run_with_rollback(
                        name,
                        StepConfig::default(),
                        move |_| async move {
                            forward.v2_forward.fetch_add(1, Ordering::SeqCst);
                            StructuredValue::from_json(json!(name))
                        },
                        RollbackOptions::new(move |rollback| {
                            let reverse = reverse.clone();
                            let ctx = ctx.clone();
                            async move {
                                assert_eq!(rollback.output.unwrap().to_json()?, json!(name));
                                reverse
                                    .v2_rollback
                                    .lock()
                                    .unwrap()
                                    .push((name.into(), rollback.ctx.attempt));
                                if failing {
                                    return Err(
                                        StepFailure::non_retryable("refund rejected").into()
                                    );
                                }
                                if held && name == "charge" {
                                    reverse.rollback_started.notify_one();
                                    reverse.release_rollback.notified().await;
                                } else if name == "charge" && rollback.ctx.attempt == 1 {
                                    return Err(anyhow!("retry refund"));
                                }
                                note(&ctx, &format!("undo-{name}")).await?;
                                Ok(())
                            }
                        })
                        .config(StepConfig::default().retries(Retries::new(1, 0.0))),
                    )
                    .await?;
            }
            if terminating {
                steps.wait_for_event("hold", "continue").await
            } else {
                Err(StepFailure::non_retryable("fulfillment failed").into())
            }
        }
        "stream-retry" | "stream-rollback" | "stream-quota" => {
            let retrying = event.instance_id == "stream-retry";
            let compensating = event.instance_id == "stream-rollback";
            let quota = event.instance_id == "stream-quota";
            let capturing = signals.clone();
            let producing = context.clone();
            let compensate_context = context.clone();
            let output = steps
                .run_stream_with_rollback(
                    "bytes",
                    StepConfig::default().retries(Retries::new(1, 0.0)),
                    move |attempt| async move {
                        capturing.stream_retries.fetch_add(1, Ordering::SeqCst);
                        let pipe = cellar::sdk::streams::IdentityTransformStream::new();
                        let writer = pipe.writable.get_writer()?;
                        producing.wait_until_byte_stream(&pipe.readable, async move {
                            let size = if quota {
                                4 * 1024 * 1024
                            } else {
                                2 * 1024 * 1024 + 17
                            };
                            if writer.write(vec![7; size]).await.is_ok() {
                                if retrying && attempt.attempt == 1 {
                                    writer
                                        .abort(Some(
                                            cellar::sdk::primitives::web::AbortReason::error(
                                                "source interrupted",
                                            ),
                                        ))
                                        .await?;
                                } else {
                                    writer.close().await?;
                                }
                            }
                            Ok(())
                        })?;
                        Ok(pipe.readable)
                    },
                    RollbackOptions::new(move |rollback| {
                        let context = compensate_context.clone();
                        let signals = signals.clone();
                        async move {
                            if let Some(mut stream) = rollback.stream_output(&context)? {
                                let mut count = 0;
                                while let Some(bytes) = stream.next(&context).await? {
                                    assert!(bytes.iter().all(|byte| *byte == 7));
                                    count += bytes.len();
                                }
                                assert_eq!(count as u64, stream.size());
                                signals.stream_compensations.fetch_add(1, Ordering::SeqCst);
                            }
                            Ok(())
                        }
                    }),
                )
                .await?;
            if compensating {
                return Err(StepFailure::non_retryable("publish failed").into());
            }
            StructuredValue::from_json(json!({"bytes":output.size()}))
        }
        "stream" => {
            let captured = signals.clone();
            let producer_context = context.clone();
            let mut output = steps
                .run_stream("bytes", move |_| async move {
                    captured.stream_runs.fetch_add(1, Ordering::SeqCst);
                    let pipe = cellar::sdk::streams::IdentityTransformStream::new();
                    let writer = pipe.writable.get_writer()?;
                    producer_context.wait_until_byte_stream(&pipe.readable, async move {
                        writer.write(vec![7; 2 * 1024 * 1024 + 17]).await?;
                        writer.close().await?;
                        Ok(())
                    })?;
                    Ok(pipe.readable)
                })
                .await?;
            let mut total = 0;
            while let Some(bytes) = output.next(&context).await? {
                assert!(bytes.iter().all(|byte| *byte == 7));
                total += bytes.len();
            }
            assert_eq!(total, 2 * 1024 * 1024 + 17);
            steps.wait_for_event("finish", "approve").await?;
            StructuredValue::from_json(json!({"bytes":total}))
        }
        "sensitive" => {
            steps
                .run_with(
                    "secret",
                    StepConfig {
                        sensitive: true,
                        input: Some(StructuredValue::from_json(json!({"token":"input-secret"}))?),
                        ..Default::default()
                    },
                    |_| async { StructuredValue::from_json(json!("secret-token")) },
                )
                .await?;
            StructuredValue::from_json(json!("done"))
        }
        "configured" => {
            let config = StepConfig::default()
                .retries(Retries::new(2, 0.0).backoff(Backoff::Linear))
                .timeout("1 second");
            steps
                .run_with("configured", config, |attempt| async move {
                    assert_eq!(attempt.retry_limit, 2.0);
                    assert_eq!(attempt.timeout.milliseconds()?, 1000.0);
                    assert!(matches!(attempt.backoff, Some(Backoff::Linear)));
                    if attempt.attempt < 3 {
                        return Err(anyhow!("retry configured step"));
                    }
                    assert_eq!(attempt.attempt, 3);
                    StructuredValue::from_json(json!(attempt.attempt))
                })
                .await
        }
        "permanent" => {
            let counted = signals.clone();
            let failed = steps
                .run("failed", move |_| async move {
                    counted.permanent.fetch_add(1, Ordering::SeqCst);
                    Err(StepFailure::non_retryable("declined").into())
                })
                .await
                .unwrap_err();
            assert_eq!(failed.to_string(), "declined");
            steps.wait_for_event("gate", "approve").await
        }
        "nested" => {
            let nested = steps.clone();
            steps
                .run("outer", move |_| async move {
                    nested
                        .do_step("inner", |_| async { Ok(StructuredValue::undefined()) })
                        .await
                })
                .await
        }
        "parallel" => {
            let mut futures = Vec::new();
            for _ in 0..3 {
                let counted = signals.clone();
                let ctx = context.clone();
                futures.push(steps.run("same", move |attempt| async move {
                    if attempt.count == 3 {
                        note(&ctx, "detached").await?;
                    }
                    counted.parallel.lock().unwrap().push(attempt.count);
                    StructuredValue::from_json(json!(attempt.count))
                }));
            }
            drop(futures.pop());
            StructuredValue::array(futures_util::future::try_join_all(futures).await?)
        }
        "capped" => {
            steps
                .run("big", move |_| async move {
                    signals.capped.fetch_add(1, Ordering::SeqCst);
                    Ok(StructuredValue {
                        root: CloneValue::String(vec![b'x' as u16; (1 << 20) + 1]),
                        objects: Vec::new(),
                    })
                })
                .await
        }
        "deadline" => {
            steps
                .wait_for_event_with_timeout("expiry", "approve".into(), None)
                .await
        }
        "timeout" => {
            let config = StepConfig::default()
                .retries(Retries::new(0, "1 second").backoff(Backoff::Constant))
                .timeout(5.0);
            let error = steps
                .do_with_config("timeout", config, move |_| async move {
                    signals.timeout_started.notify_one();
                    signals.release_timeout.notified().await;
                    note(&context, "late timeout effect").await?;
                    signals.timeout_finished.store(true, Ordering::SeqCst);
                    StructuredValue::from_json(json!("late result"))
                })
                .await
                .unwrap_err();
            assert!(error.to_string().contains("timed out"));
            StructuredValue::from_json(json!("timeout caught"))
        }
        "race" => {
            let counted = signals.clone();
            steps
                .run("held", move |_| async move {
                    counted.race_calls.fetch_add(1, Ordering::SeqCst);
                    counted.race_started.notify_one();
                    counted.release_race.notified().await;
                    Ok(StructuredValue::undefined())
                })
                .await?;
            steps.wait_for_event("gate", "approve").await
        }
        "hung" => {
            let _drop = Dropped(signals.clone());
            signals.hung_started.notify_one();
            futures_util::future::pending().await
        }
        "atomic-event" => match steps.wait_for_event("consume", "approve").await {
            Ok(value) => Ok(value),
            Err(_) => StructuredValue::from_json(json!("application caught the storage error")),
        },
        _ => Err(anyhow!("unknown workflow")),
    }
}

pub(crate) async fn worker(
    context: Context,
    invocation: Invocation,
    signals: Arc<Signals>,
) -> Result<Output> {
    let Invocation::Fetch(request) = invocation else {
        return Err(anyhow!("expected fetch"));
    };
    let url = url::Url::parse(&request.url)?;
    let mut paths = url.path().trim_start_matches('/').split('/');
    let action = paths.next().unwrap();
    let id = paths.next().unwrap();
    let binding = context.workflow("WF")?;
    if action == "batch" {
        binding
            .create(
                &context,
                CreateOptions {
                    id: Some("batch-existing".into()),
                    ..Default::default()
                },
            )
            .await?;
        let result = binding
            .create_batch_with(
                &context,
                BatchCreateOptions::instances(vec![
                    CreateOptions {
                        id: Some("batch-existing".into()),
                        ..Default::default()
                    },
                    CreateOptions {
                        id: Some("batch-new".into()),
                        ..Default::default()
                    },
                    CreateOptions {
                        id: Some("batch-new".into()),
                        ..Default::default()
                    },
                ]),
            )
            .await?;
        assert_eq!(result.created.len(), 1);
        assert_eq!(
            result
                .errors
                .iter()
                .map(|e| (e.index, e.code))
                .collect::<Vec<_>>(),
            vec![(0, 10405), (2, 10415)]
        );
        let generated = binding
            .create_batch_with(
                &context,
                BatchCreateOptions::Count {
                    count: 2,
                    params: Some(StructuredValue::from_json(json!({"shared":true}))?),
                    retention: Default::default(),
                },
            )
            .await?;
        assert_eq!(generated.created.len(), 2);
        assert!(generated.errors.is_empty());
        signals
            .extra_cells
            .lock()
            .unwrap()
            .extend(generated.created.iter().map(|i| i.id().to_owned()));
        return Ok(Output::Http(HttpResponse {
            status: 200,
            body: b"true".to_vec(),
            stream: None,
            headers: vec![],
            websocket: None,
            write_position: None,
            observed_position: None,
        }));
    }
    let instance = if action == "create" {
        binding
            .create(
                &context,
                CreateOptions {
                    id: Some(id.into()),
                    params: Some(complex()),
                    retention: if id == "terminate-rollback" {
                        Retention {
                            success: None,
                            error: Some(WfDuration::Milliseconds(1.0)),
                        }
                    } else {
                        Default::default()
                    },
                    ..Default::default()
                },
            )
            .await?
    } else {
        binding.get(&context, id).await?
    };
    let value = match action {
        "create" => json!("created"),
        "status" => {
            let status = instance.status(&context).await?;
            let output = if let Some(output) = status.output {
                if id == "main" {
                    check_complex(&output);
                    None
                } else if id.starts_with("cron-")
                    || matches!(
                        id,
                        "stream" | "stream-retry" | "stream-rollback" | "sensitive"
                    )
                {
                    Some(output.to_json()?)
                } else {
                    None
                }
            } else {
                None
            };
            json!({"state":status.state, "error":status.error.map(|error| error.message_lossy()),"rollback":status.rollback.map(|r|r.to_json()).transpose()?,"output":output})
        }
        "history" | "history-resume" | "filtered" | "live" => {
            let filter = if matches!(action, "filtered" | "live") {
                Some(vec![InstanceEventType::StepCompleted])
            } else {
                None
            };
            let mut subscription = instance
                .subscribe(
                    &context,
                    SubscribeOptions {
                        cursor: if action == "history-resume" {
                            Some(3)
                        } else {
                            None
                        },
                        filter,
                    },
                )
                .await?;
            let mut events = Vec::new();
            if action == "live" {
                signals.subscribed.notify_one();
            }
            while let Some(event) = subscription.next().await? {
                let step = event
                    .data
                    .property(&"stepName".encode_utf16().collect::<Vec<_>>())?
                    .map(|v| v.to_json())
                    .transpose()?;
                let output = if id == "sensitive" {
                    event
                        .data
                        .property(&"output".encode_utf16().collect::<Vec<_>>())?
                        .map(|v| v.to_json())
                        .transpose()?
                } else {
                    None
                };
                events.push(json!({"id":event.event_id,"type":event.event_type,"step":step,"output":output}));
                if action == "live" {
                    subscription.close();
                    break;
                }
            }
            json!(events)
        }
        "close-subscription" => {
            let mut subscription = instance
                .subscribe(&context, SubscribeOptions::default())
                .await?;
            subscription.close();
            assert!(subscription.next().await?.is_none());
            json!(true)
        }
        "read-stream" => {
            let mut stream = instance
                .step_stream(&context, StepSelector::new("bytes"))
                .await?;
            let mut count = 0;
            while let Some(bytes) = stream.next(&context).await? {
                assert!(bytes.iter().all(|b| *b == 7));
                count += bytes.len();
            }
            assert_eq!(stream.size(), count as u64);
            json!(count)
        }
        "terminate-rollback" => {
            instance
                .terminate_with_options(&context, TerminateOptions { rollback: true })
                .await?;
            json!("rolling back")
        }
        "pause" => {
            instance.pause(&context).await?;
            json!("paused")
        }
        "resume" => {
            instance.resume(&context).await?;
            json!("resumed")
        }
        "event" => {
            instance
                .send_event(
                    &context,
                    SendEvent {
                        event_type: "approve".into(),
                        payload: Some(complex()),
                    },
                )
                .await?;
            json!("sent")
        }
        "restart" => {
            instance
                .restart(&context, RestartOptions::default())
                .await?;
            json!("restarted")
        }
        "selected" => {
            instance
                .restart(
                    &context,
                    RestartOptions {
                        from: Some(RestartFrom {
                            name: "last".encode_utf16().collect(),
                            count: 1,
                            kind: StepKind::Do,
                        }),
                    },
                )
                .await?;
            json!("restarted")
        }
        "terminate" => {
            instance.terminate(&context).await?;
            json!("terminated")
        }
        "delete" => {
            instance.delete(&context).await?;
            json!("deleted")
        }
        _ => return Err(anyhow!("unknown action")),
    };
    Ok(Output::Http(HttpResponse {
        status: 200,
        body: serde_json::to_vec(&value)?,
        stream: None,
        headers: Vec::new(),
        websocket: None,
        write_position: None,
        observed_position: None,
    }))
}
