// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Provider response capacity precedes construction and survives pending work.
use anyhow::{ensure, Result};
use cellar::sdk::{rpc, *};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

#[allow(dead_code)]
#[path = "../../../tools/rpc/fixtures/generated/rust/example.counter.v1.rs"]
mod generated;
#[allow(dead_code)]
#[path = "support/actor_lease_trace.rs"]
mod lease_trace;
#[path = "../../cellar-javascript/tests/support/native.rs"]
mod native;
use generated::{
    CounterClient, CounterHandler, CounterRegistration, GetCountRequest, GetCountResponse,
    IncrementRequest, IncrementResponse,
};

#[derive(Default)]
struct Counts {
    factories: AtomicUsize,
    handlers: AtomicUsize,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

struct Counter(Arc<Counts>);

#[derive(Default)]
struct ObservedResponse {
    inner: GetCountResponse,
    response_admitted: bool,
    decoder_admitted: bool,
}

impl rpc::prost::Message for ObservedResponse {
    fn encode_raw(&self, buffer: &mut impl bytes::BufMut) {
        rpc::prost::Message::encode_raw(&self.inner, buffer);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire: rpc::prost::encoding::WireType,
        buffer: &mut impl bytes::Buf,
        context: rpc::prost::encoding::DecodeContext,
    ) -> Result<(), rpc::prost::DecodeError> {
        let services = cellar::asyncrt::services();
        self.response_admitted = services.rpc_response_usage().reserved_response_bytes > 0;
        self.decoder_admitted = services.native_rpc_decode_usage().reserved_decode_bytes > 0;
        rpc::prost::Message::merge_field(&mut self.inner, tag, wire, buffer, context)
    }

    fn encoded_len(&self) -> usize {
        rpc::prost::Message::encoded_len(&self.inner)
    }

    fn clear(&mut self) {
        rpc::prost::Message::clear(&mut self.inner);
    }
}

impl CounterHandler for Counter {
    fn increment(
        self: Arc<Self>,
        _: Context,
        _: IncrementRequest,
    ) -> rpc::HandlerFuture<IncrementResponse> {
        Box::pin(async { Ok(IncrementResponse::default()) })
    }

    fn get_count(
        self: Arc<Self>,
        context: Context,
        _: GetCountRequest,
    ) -> rpc::HandlerFuture<GetCountResponse> {
        Box::pin(async move {
            self.0.handlers.fetch_add(1, Ordering::SeqCst);
            if context.identity().unwrap().name.as_deref() == Some("held") {
                self.0.entered.notify_one();
                self.0.release.notified().await;
            }
            Ok(GetCountResponse {
                count: Some(generated::Count { value: 42 }),
            })
        })
    }
}

fn application(counts: Arc<Counts>) -> Result<Arc<dyn Application>> {
    let contract = Arc::new(rpc::Contract::from_descriptor(
        include_bytes!("../../../tools/rpc/fixtures/counter.binpb"),
        rpc::Limits::default(),
    )?);
    let ingress = contract.clone();
    let app = Registry::new(move |context, invocation| {
        let contract = ingress.clone();
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                anyhow::bail!("expected HTTP");
            };
            let url = url::Url::parse(&request.url)?;
            if url.path() == "/observe-decoder" {
                let client = rpc::Client::bind(&context, "COUNTERS", "decoder", contract)?;
                let reply = client.call::<_, ObservedResponse>(
                    "example.counter.v1.Counter/GetCount", GetCountRequest {}, Default::default(),
                ).await?;
                return Ok(Output::Http(http::Response::from_json(&serde_json::json!({
                    "value":reply.inner.count.unwrap().value,
                    "response_admitted":reply.response_admitted,"decoder_admitted":reply.decoder_admitted,
                }))?.into_http(&context)?));
            }
            let client =
                CounterClient::bind(&context, "COUNTERS", url.path().trim_start_matches('/'))?;
            let result = match client.get_count(GetCountRequest {}).await {
                Ok(reply) => serde_json::json!({"value":reply.count.unwrap().value}),
                Err(failure) => serde_json::to_value(failure)?,
            };
            Ok(Output::Http(
                http::Response::from_json(&result)?.into_http(&context)?,
            ))
        })
    })
    .cell("Counter", move |_, _| {
        counts.factories.fetch_add(1, Ordering::SeqCst);
        let router =
            CounterRegistration::register(rpc::Router::new(), Arc::new(Counter(counts.clone())))?;
        Ok(Arc::new(router) as Arc<dyn Cell>)
    })?
    .rpc_service("Counter", "example.counter.v1.Counter", contract)?;
    Ok(Arc::new(app))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_response_frame_remains_admitted_through_the_response_codec() -> Result<()> {
    let trace = lease_trace::install(None);
    trace.mark("native response handoff host starting");
    let directory = tempfile::tempdir()?;
    let counts = Arc::new(Counts::default());
    let (node, base) = native::start(directory.path(), application(counts.clone())?).await?;
    let services = node.services();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    trace.mark("native response handoff host started");
    let operation = async {
        let mut response = client.get(format!("{base}/observe-decoder")).send().await?;
        trace.mark("native response handoff head received");
        let status = response.status();
        let headers = response.headers().clone();
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(body.len() + chunk.len() <= 16 * 1024, "oversize probe body: {status}; headers={headers:?}");
            body.extend_from_slice(&chunk);
        }
        let diagnostic = String::from_utf8_lossy(&body);
        ensure!(status.is_success(), "response codec probe failed: {status}; headers={headers:?}; body={diagnostic}");
        let reply: serde_json::Value = serde_json::from_slice(&body).map_err(|error| {
            anyhow::anyhow!("response codec probe JSON: {error}; status={status}; headers={headers:?}; body={diagnostic}")
        })?;
        ensure!(
            reply["value"] == 42 && reply["decoder_admitted"] == true,
            "probe did not execute the native response decoder: {reply}"
        );
        ensure!(
            reply["response_admitted"] == true,
            "response frame retired before its codec: {reply}"
        );
        Ok::<_, anyhow::Error>(())
    }.await;
    // Join the host on every probe outcome. Record failures before cancellation
    // changes the evidence; successful runs keep the observer memory-only.
    let diagnostic = operation.as_ref().err().map(|error| {
        format!(
            "{error:#}; factories={}; handlers={}; node_closed={}; work={:?}; \
             source={:?}; response={:?}; decoder={:?}; lease_observations={:?}",
            counts.factories.load(Ordering::SeqCst),
            counts.handlers.load(Ordering::SeqCst),
            node.is_closed(),
            services.application_work(),
            services.outbound_rpc_usage(),
            services.rpc_response_usage(),
            services.native_rpc_decode_usage(),
            trace.snapshot(),
        )
    });
    let outcome = if operation.is_err() {
        node.cancel().await
    } else {
        node.shutdown(ShutdownMode::Preserve).await
    };
    if let Some(diagnostic) = diagnostic {
        anyhow::bail!("native response handoff failed: {diagnostic}; joined node={outcome:?}");
    }
    let outcome = outcome?;
    ensure!(
        matches!(outcome, NodeOutcome::Stopped(_)),
        "response handoff node did not stop: {outcome:?}"
    );
    ensure!(services.rpc_response_usage() == Default::default());
    ensure!(services.native_rpc_decode_usage() == Default::default());
    ensure!(services.outbound_rpc_usage() == Default::default());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provider_saturation_refuses_before_factory_and_cancellation_refunds_capacity() -> Result<()>
{
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let directory = tempfile::tempdir()?;
            let counts = Arc::new(Counts::default());
            // Exactly one default typed provider's encoding allowance, including
            // both envelope controls. This is a new fixture, not a relaxed budget.
            let encoding = 4 * (1024 * 1024 + 2048) + 64 * 1024;
            let (node, base) = native::start_configured(
                directory.path(),
                application(counts.clone())?,
                move |config, _, _| {
                    config.runtime.max_rpc_response_bytes = encoding;
                    config.runtime.handler_budget = Duration::from_secs(10);
                },
            )
            .await?;
            let services = node.services();
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()?;
            let calling = client.clone();
            let url = format!("{base}/held");
            let pending = cellar::asyncrt::spawn(async move { calling.get(url).send().await });
            let operation = async {
                cellar::asyncrt::timeout(Duration::from_secs(2), counts.entered.notified()).await?;
                let response = client.get(format!("{base}/blocked")).send().await?;
                ensure!(response.status().is_success());
                let reply: serde_json::Value = response.json().await?;
                ensure!(
                    reply == serde_json::json!({"code":"ResourceExhausted","outcome":"unknown"}),
                    "saturated provider returned {reply}"
                );
                ensure!(
                    counts.factories.load(Ordering::SeqCst) == 1,
                    "saturation constructed another business cell"
                );
                ensure!(
                    counts.handlers.load(Ordering::SeqCst) == 1,
                    "saturation entered another business handler"
                );
                ensure!(services.rpc_response_usage().active_buffers == 1);
                ensure!(services.rpc_response_usage().reserved_response_bytes == encoding);
                Ok::<_, anyhow::Error>(())
            }
            .await;
            pending.abort();
            let _ = pending.await;
            counts.release.notify_waiters();
            let cleanup = cellar::asyncrt::timeout(Duration::from_secs(2), async {
                while services.rpc_response_usage() != Default::default() {
                    cellar::asyncrt::sleep(Duration::from_millis(1)).await;
                }
            })
            .await;
            let fresh = client
                .get(format!("{base}/fresh"))
                .send()
                .await?
                .json::<serde_json::Value>()
                .await?;
            let outcome = node.shutdown(ShutdownMode::Preserve).await?;
            ensure!(
                matches!(outcome, NodeOutcome::Stopped(_)),
                "response quota node failed: {outcome:?}"
            );
            ensure!(
                services.rpc_response_usage() == Default::default(),
                "response admission survived join"
            );
            operation?;
            cleanup?;
            ensure!(
                fresh == serde_json::json!({"value":42}),
                "fresh work did not resume: {fresh}"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}
