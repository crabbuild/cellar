// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]
mod support;
use anyhow::{ensure, Result};
use axum::{
    body::Body,
    extract::State,
    response::{sse::Event, Response, Sse},
    routing::get,
    Router,
};
use cellar_axum::{ApplicationBuilder, ApplicationDefinition, AxumCell, CellContext};
use futures_util::{stream, StreamExt};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};

struct Signals {
    release: Semaphore,
    dropped: Notify,
    polls: AtomicUsize,
}

impl Default for Signals {
    fn default() -> Self {
        Self {
            release: Semaphore::new(0),
            dropped: Notify::new(),
            polls: AtomicUsize::new(0),
        }
    }
}

struct Producer(Arc<Signals>);

impl Drop for Producer {
    fn drop(&mut self) {
        self.0.dropped.notify_one();
    }
}

async fn events(
    CellContext(context): CellContext,
    State(signals): State<Arc<Signals>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event>>> {
    let stream = stream::unfold(
        (context, Producer(signals), 0),
        |(context, producer, index)| async move {
            if index == 2 {
                return None;
            }
            if index == 1 {
                producer.0.release.acquire().await.ok()?.forget();
            }
            let result = context
                .storage()
                .and_then(|storage| storage.put_json("count", &serde_json::json!(index + 1)))
                .map(|_| {
                    Event::default()
                        .event("count")
                        .data((index + 1).to_string())
                });
            Some((result, (context, producer, index + 1)))
        },
    );
    Sse::new(stream)
}

async fn endless(
    CellContext(context): CellContext,
    State(signals): State<Arc<Signals>>,
) -> Response {
    let stream = stream::unfold(
        (context, Producer(signals)),
        |(context, producer)| async move {
            // No transport or ambient executor holds the invocation authority.
            let index = producer.0.polls.fetch_add(1, Ordering::SeqCst);
            let result = context
                .storage()
                .and_then(|storage| storage.put_json("count", &serde_json::json!(index)))
                .map(|_| vec![b'x'; 64 * 1024]);
            Some((result, (context, producer)))
        },
    );
    Response::new(Body::from_stream(stream))
}

async fn broken() -> Response {
    Response::new(Body::from_stream(stream::iter([
        Ok::<_, std::io::Error>("first"),
        Err(std::io::Error::other("body failure")),
    ])))
}

fn definition(signals: Arc<Signals>) -> Result<ApplicationDefinition> {
    ApplicationBuilder::default()
        .cell("Counter", move |_, _| {
            Ok(Arc::new(AxumCell::new(
                Router::new()
                    .route("/events/{name}", get(events))
                    .route("/endless/{name}", get(endless))
                    .route("/broken/{name}", get(broken))
                    .route("/trailers/{name}", get(trailers))
                    .with_state(signals.clone()),
            )))
        })?
        .cell_route("/events/{name}", "COUNTERS", "Counter", "name")?
        .cell_route("/endless/{name}", "COUNTERS", "Counter", "name")?
        .cell_route("/broken/{name}", "COUNTERS", "Counter", "name")?
        .cell_route("/trailers/{name}", "COUNTERS", "Counter", "name")?
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sse_polls_with_event_authority_after_the_response_head() -> Result<()> {
    for external in [false, true] {
        let directory = tempfile::tempdir()?;
        let signals = Arc::new(Signals::default());
        let factory = signals.clone();
        let host = support::start(directory.path(), external, "", move || {
            definition(factory.clone())
        })
        .await?;
        let response = support::client()?
            .get(format!("{}/events/alpha", host.url))
            .send()
            .await?;
        ensure!(
            response.status() == 200 && response.headers()["content-type"] == "text/event-stream"
        );
        let mut chunks = response.bytes_stream();
        let first = chunks.next().await.unwrap()?;
        ensure!(String::from_utf8_lossy(&first).contains("data: 1"));
        ensure!(
            tokio::time::timeout(Duration::from_millis(100), chunks.next())
                .await
                .is_err()
        );
        signals.release.add_permits(1);
        let second = tokio::time::timeout(Duration::from_secs(5), chunks.next())
            .await?
            .unwrap()?;
        ensure!(String::from_utf8_lossy(&second).contains("data: 2"));
        ensure!(chunks.next().await.is_none());
        tokio::time::timeout(Duration::from_secs(5), signals.dropped.notified()).await?;
        host.stop().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumer_drop_cancels_the_producer_and_backpressure_bounds_polling() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let signals = Arc::new(Signals::default());
    let factory = signals.clone();
    let host = support::start(directory.path(), true, "", move || {
        definition(factory.clone())
    })
    .await?;
    let ingress = host.ingress.as_ref().unwrap();
    let response = ingress
        .call(
            http::Request::builder()
                .uri("/endless/alpha")
                .body(Body::empty())?,
        )
        .await;
    ensure!(response.status() == 200);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let before = signals.polls.load(Ordering::SeqCst);
    ensure!(
        before > 0 && before < 16,
        "unbounded producer polling {before}"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    ensure!(
        signals.polls.load(Ordering::SeqCst) == before,
        "producer ignored backpressure"
    );
    drop(response);
    tokio::time::timeout(Duration::from_secs(5), signals.dropped.notified()).await?;
    host.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn body_failure_is_not_a_successful_eof() -> Result<()> {
    for external in [false, true] {
        let directory = tempfile::tempdir()?;
        let host = support::start(directory.path(), external, "", || {
            definition(Arc::new(Signals::default()))
        })
        .await?;
        let result = support::client()?
            .get(format!("{}/broken/alpha", host.url))
            .send()
            .await;
        if let Ok(response) = result {
            ensure!(
                response.bytes().await.is_err(),
                "failed stream became successful EOF"
            );
        }
        host.stop().await?;
    }
    Ok(())
}

#[path = "support/store.rs"]
mod store;

async fn proof_events(
    CellContext(context): CellContext,
    State(signals): State<Arc<Signals>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event>>> {
    Sse::new(stream::unfold(
        (context, Producer(signals), 0),
        |(context, producer, index)| async move {
            let result = match index {
                0 => Ok(Event::default().data("ready")),
                1 => {
                    producer.0.release.acquire().await.ok()?.forget();
                    context
                        .storage()
                        .and_then(|storage| storage.put_json("committed", &serde_json::json!(true)))
                        .map(|_| Event::default().data("committed"))
                }
                _ => return None,
            };
            Some((result, (context, producer, index + 1)))
        },
    ))
}

fn proof_definition(signals: Arc<Signals>) -> Result<ApplicationDefinition> {
    ApplicationBuilder::default()
        .cell("Counter", move |_, _| {
            Ok(Arc::new(AxumCell::new(
                Router::new()
                    .route("/events/{name}", get(proof_events))
                    .with_state(signals.clone()),
            )))
        })?
        .cell_route("/events/{name}", "COUNTERS", "Counter", "name")?
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_head_writes_require_proof_for_each_acknowledging_sse_chunk() -> Result<()> {
    for external in [false, true] {
        for fail in [false, true] {
            let directory = tempfile::tempdir()?;
            let mut bucket =
                cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite"))?;
            let control = Arc::new(store::ProofControl::default());
            bucket.store = Arc::new(store::ProofStore {
                inner: bucket.store.clone(),
                control: control.clone(),
            });
            let signals = Arc::new(Signals::default());
            let factory = signals.clone();
            let host = support::start_with(
                &directory.path().join("node"),
                external,
                "",
                "stream-proof",
                Some(bucket),
                move || proof_definition(factory.clone()),
            )
            .await?;
            let response = support::client()?
                .get(format!("{}/events/alpha", host.url))
                .send()
                .await?;
            ensure!(response.status() == 200);
            let mut chunks = response.bytes_stream();
            ensure!(String::from_utf8_lossy(&chunks.next().await.unwrap()?).contains("ready"));
            control.mode.store(1, Ordering::SeqCst);
            signals.release.add_permits(1);
            ensure!(
                tokio::time::timeout(Duration::from_millis(200), chunks.next())
                    .await
                    .is_err(),
                "unproven post-head write escaped"
            );
            tokio::time::timeout(Duration::from_secs(5), control.entered.notified()).await?;
            control
                .mode
                .store(if fail { 2 } else { 0 }, Ordering::SeqCst);
            control.release.add_permits(8);
            let result = tokio::time::timeout(Duration::from_secs(5), chunks.next()).await?;
            if fail {
                ensure!(
                    matches!(result, Some(Err(_))),
                    "failed chunk proof became success or EOF: {result:?}"
                );
            } else {
                ensure!(String::from_utf8_lossy(&result.unwrap()?).contains("committed"));
                ensure!(chunks.next().await.is_none());
            }
            drop(chunks);
            control.mode.store(0, Ordering::SeqCst);
            host.stop().await?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fencing_retires_an_open_sse_producer_before_it_can_acknowledge_a_write() -> Result<()> {
    for external in [false, true] {
        let directory = tempfile::tempdir()?;
        let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite"))?;
        let signals = Arc::new(Signals::default());
        let factory = signals.clone();
        let mut host = support::start_with(
            &directory.path().join("node"),
            external,
            "",
            "fenced",
            Some(bucket.clone()),
            move || proof_definition(factory.clone()),
        )
        .await?;
        let response = support::client()?
            .get(format!("{}/events/alpha", host.url))
            .send()
            .await?;
        let mut chunks = response.bytes_stream();
        ensure!(String::from_utf8_lossy(&chunks.next().await.unwrap()?).contains("ready"));
        let faults = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
        faults
            .run(async {
                let (bytes, _) = bucket.get("nodes/fenced.json").await?.unwrap();
                let mut lease: serde_json::Value = serde_json::from_slice(&bytes)?;
                lease["generation"] = serde_json::json!("replacement-process");
                bucket
                    .put("nodes/fenced.json", serde_json::to_vec(&lease)?)
                    .await?;
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        faults.shutdown().await;
        let outcome = tokio::time::timeout(Duration::from_secs(10), host.node.finished()).await??;
        ensure!(
            matches!(outcome, cellar::sdk::NodeOutcome::Fenced { .. }),
            "{outcome:?}"
        );
        signals.release.add_permits(1);
        let next = tokio::time::timeout(Duration::from_secs(5), chunks.next()).await?;
        ensure!(
            !matches!(next, Some(Ok(_))),
            "fenced stream acknowledged work"
        );
        tokio::time::timeout(Duration::from_secs(5), signals.dropped.notified()).await?;
        host.cancel().await?;
    }
    Ok(())
}

async fn trailers() -> Response {
    let mut trailers = http::HeaderMap::new();
    trailers.insert("x-trailer", http::HeaderValue::from_static("unsupported"));
    let frames = stream::iter([Ok::<_, std::io::Error>(
        http_body::Frame::<bytes::Bytes>::trailers(trailers),
    )]);
    Response::new(Body::new(http_body_util::StreamBody::new(frames)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_trailers_fail_the_stream_instead_of_disappearing() -> Result<()> {
    for external in [false, true] {
        let directory = tempfile::tempdir()?;
        let host = support::start(directory.path(), external, "", || {
            definition(Arc::new(Signals::default()))
        })
        .await?;
        let response = support::client()?
            .get(format!("{}/trailers/alpha", host.url))
            .send()
            .await;
        if let Ok(response) = response {
            ensure!(
                response.bytes().await.is_err(),
                "trailers disappeared into a successful response"
            );
        }
        host.stop().await?;
    }
    Ok(())
}
