// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real SQLite/worker boundaries distinguish queue, storage and delivery waits.
#![allow(clippy::disallowed_methods)]
use super::*;
use crate::sdk::ExecutionDomain;
use anyhow::{ensure, Result};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Condvar;
use std::task::{Context, Poll};
use tracing::field::{Field, Visit};
use tracing::instrument::WithSubscriber;
use tracing::{Event, Instrument as _, Subscriber};
use tracing_subscriber::{layer::Context as LayerContext, prelude::*, registry::LookupSpan, Layer};

#[derive(Clone, Default)]
struct Capture(Arc<Observations>);

#[derive(Default)]
struct Observations {
    rows: Mutex<Vec<BTreeMap<String, String>>>,
    native_progress: Condvar,
    async_progress: tokio::sync::Notify,
}

impl Capture {
    fn dispatch(&self) -> tracing::Dispatch {
        tracing::Dispatch::new(tracing_subscriber::registry().with(self.clone()))
    }

    fn rows(&self) -> Vec<BTreeMap<String, String>> {
        self.0.rows.lock().unwrap().clone()
    }

    fn contains(&self, phase: &str) -> bool {
        self.rows()
            .iter()
            .any(|row| row.get("phase").is_some_and(|value| value == phase))
    }

    async fn wait(&self, phase: &str) {
        loop {
            let notified = self.0.async_progress.notified();
            if self.contains(phase) {
                return;
            }
            notified.await;
        }
    }

    fn wait_without_polling(&self, phase: &str) -> Result<()> {
        let rows = self.0.rows.lock().unwrap();
        let (rows, _) = self
            .0
            .native_progress
            .wait_timeout_while(rows, Duration::from_secs(5), |rows| {
                !rows
                    .iter()
                    .any(|row| row.get("phase").is_some_and(|value| value == phase))
            })
            .unwrap();
        ensure!(
            rows.iter()
                .any(|row| row.get("phase").is_some_and(|value| value == phase)),
            "native worker did not reach {phase}"
        );
        Ok(())
    }

    fn elapsed(&self, phase: &str) -> u64 {
        self.rows()
            .iter()
            .find(|row| row.get("phase").is_some_and(|value| value == phase))
            .unwrap_or_else(|| panic!("missing {phase}: {:?}", self.rows()))["elapsed_us"]
            .parse()
            .unwrap()
    }

    fn check_identity_and_privacy(&self) {
        let rows = self.rows();
        let operation = &rows[0]["operation"];
        for row in &rows {
            assert_eq!(&row["operation"], operation);
            assert_eq!(row["event"], "local_store_put_phase");
            assert!(row.keys().all(|key| matches!(
                key.as_str(),
                "message" | "event" | "operation" | "phase" | "elapsed_us" | "span"
            )));
        }
    }
}

impl<S: Subscriber + for<'lookup> LookupSpan<'lookup>> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, context: LayerContext<'_, S>) {
        if event.metadata().target() != "store" {
            return;
        }

        struct Fields(BTreeMap<String, String>);

        impl Visit for Fields {
            fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
                self.0.insert(field.name().into(), format!("{value:?}"));
            }

            fn record_str(&mut self, field: &Field, value: &str) {
                self.0.insert(field.name().into(), value.into());
            }
        }
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        if let Some(span) = context.event_span(event) {
            fields.0.insert("span".into(), span.name().into());
        }
        self.0.rows.lock().unwrap().push(fields.0);
        self.0.native_progress.notify_all();
        self.0.async_progress.notify_one();
    }
}

#[tokio::test]
async fn sqlite_wait_survives_caller_timeout_and_records_native_completion() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let capture = Capture::default();
    let result = domain
        .run(async {
            let store = LocalStore::open(directory.path().join("objects.sqlite"))?;
            let blocker = store.connect()?;
            blocker.execute_batch("BEGIN IMMEDIATE")?;
            let key = Path::from("private-node/lease.json");
            let dispatch = capture.dispatch();
            let span = tracing::dispatcher::with_default(
                &dispatch,
                || tracing::debug_span!(target: "store", "node_lease_store", node = "fixture"),
            );
            let mut put = Box::pin(
                store
                    .put_opts(
                        &key,
                        Bytes::from_static(b"private body").into(),
                        PutOptions::default(),
                    )
                    .instrument(span)
                    .with_subscriber(dispatch),
            );
            crate::asyncrt::timeout(Duration::from_secs(5), async {
                crate::asyncrt::select_biased! {
                    "observe native entry before cancelling the caller";
                    _ = capture.wait("writer_admitted") => Ok(()),
                    result = &mut put => anyhow::bail!("write escaped SQLite lock: {result:?}"),
                }
            })
            .await??;
            let expired = crate::asyncrt::timeout(Duration::from_millis(25), put).await;
            // Always release the real SQLite lock before an assertion can fail.
            blocker.execute_batch("ROLLBACK")?;
            ensure!(
                expired.is_err(),
                "caller timer did not expire during SQLite wait"
            );
            crate::asyncrt::timeout(Duration::from_secs(5), capture.wait("native_complete"))
                .await?;
            ensure!(capture.elapsed("caller_dropped") <= capture.elapsed("transaction_started"));
            ensure!(
                capture.elapsed("transaction_started") - capture.elapsed("writer_admitted")
                    >= 20_000
            );
            ensure!(!capture.contains("delivered"));
            ensure!(store.get(&key).await?.bytes().await?.as_ref() == b"private body");
            capture.check_identity_and_privacy();
            ensure!(
                capture.rows().iter().all(|row| {
                    row.get("span")
                        .is_some_and(|name| name == "node_lease_store")
                }),
                "native completion lost the cancelled caller's lease span"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}

#[test]
fn completed_sqlite_work_is_distinct_from_an_unpolled_caller() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let capture = Capture::default();
        let result = domain
            .run(async {
                let directory = tempfile::tempdir()?;
                let store = LocalStore::open(directory.path().join("objects.sqlite"))?;
                let key = Path::from("private/key");
                let mut put = Box::pin(
                    store
                        .put_opts(&key, Bytes::new().into(), PutOptions::default())
                        .with_subscriber(capture.dispatch()),
                );
                let waker = futures_util::task::noop_waker_ref();
                {
                    // A real held writer guard guarantees that the first poll
                    // cannot race an already-completed native operation.
                    let _held = store.writer.lock();
                    assert!(matches!(
                        put.as_mut().poll(&mut Context::from_waker(waker)),
                        Poll::Pending
                    ));
                }
                capture.wait_without_polling("native_complete")?;
                // Deliberately stop this current-thread executor after the native
                // result is ready. No SQLite operation remains in this interval.
                std::thread::sleep(Duration::from_millis(25));
                put.await?;
                ensure!(
                    capture.elapsed("delivered") - capture.elapsed("native_complete") >= 20_000
                );
                ensure!(!capture.contains("caller_dropped"));
                capture.check_identity_and_privacy();
                Ok::<_, anyhow::Error>(())
            })
            .await;
        domain.shutdown().await;
        result??;
        Ok(())
    })
}

#[test]
fn queued_native_work_is_distinct_from_sqlite_entry() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let capture = Capture::default();
        let result = domain
            .run(async {
                let directory = tempfile::tempdir()?;
                let store = LocalStore::open(directory.path().join("objects.sqlite"))?;
                let (entered, entry) = std::sync::mpsc::sync_channel(1);
                let (release, released) = std::sync::mpsc::sync_channel(1);
                let occupied = crate::asyncrt::blocking(move || {
                    entered.send(()).unwrap();
                    released.recv_timeout(Duration::from_secs(5))
                });
                entry.recv_timeout(Duration::from_secs(5))?;
                let key = Path::from("private/key");
                let mut put = Box::pin(
                    store
                        .put_opts(&key, Bytes::new().into(), PutOptions::default())
                        .with_subscriber(capture.dispatch()),
                );
                let early = crate::asyncrt::timeout(Duration::from_millis(25), &mut put).await;
                release.send(())?;
                occupied.await??;
                ensure!(early.is_err(), "write bypassed the occupied blocking pool");
                put.await?;
                ensure!(capture.elapsed("native_entered") - capture.elapsed("queued") >= 20_000);
                capture.check_identity_and_privacy();
                Ok::<_, anyhow::Error>(())
            })
            .await;
        domain.shutdown().await;
        result??;
        Ok(())
    })
}
