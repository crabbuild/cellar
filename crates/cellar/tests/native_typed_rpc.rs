// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Generated business methods must use the real SDK, host output gates and
//! recovered SQLite state. This test is not a language-bridge qualification.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar::sdk::{rpc, *};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

// Reuse the production-node fixture shared by the existing guest SDK tests.
#[path = "../../cellar-javascript/tests/support/native.rs"]
mod native;

#[path = "support/rpc_cold_factory.rs"]
mod cold_factory;

#[allow(dead_code)]
#[path = "../../../tools/rpc/fixtures/generated/rust/example.counter.v1.rs"]
mod generated;
use generated::{
    increment_response, CounterClient, CounterRegistration, GetCountRequest, GetCountResponse,
    IncrementRequest,
};

#[path = "../../../tools/rpc/native_counter.rs"]
mod counter;
use counter::Counter;

static SLOW_DECODE_HIT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static LIMIT_PROBE_ENCODED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
static EXPIRED_PARENT_PROBE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

// This message fits generated defaults but exceeds the application catalog's
// eight slots. Sizing/writing must never run on either side of a native call.
#[derive(Default)]
struct LimitProbe;

impl rpc::prost::Message for LimitProbe {
    fn encode_raw(&self, _: &mut impl bytes::BufMut) {
        LIMIT_PROBE_ENCODED.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn encoded_len(&self) -> usize {
        LIMIT_PROBE_ENCODED.store(true, std::sync::atomic::Ordering::SeqCst);
        0
    }

    fn merge_field(
        &mut self,
        _: u32,
        _: rpc::prost::encoding::WireType,
        _: &mut impl bytes::Buf,
        _: rpc::prost::encoding::DecodeContext,
    ) -> Result<(), rpc::prost::DecodeError> {
        unreachable!("limit probe is never decoded")
    }

    fn clear(&mut self) {}
}

impl rpc::MessageGraph for LimitProbe {
    fn layout_slots(&self) -> usize {
        512
    }

    fn visit(&self, budget: &mut rpc::GraphBudget, depth: usize) -> Result<(), rpc::Failure> {
        budget.message(depth, self.layout_slots())
    }

    fn discard(self: Box<Self>, _: &mut rpc::GraphDiscard) {}
}

// A slow but valid message implementation verifies that synchronous response
// decoding cannot bypass a deadline merely by returning Poll::Ready.
#[derive(Default)]
struct SlowResponse(GetCountResponse);

impl rpc::prost::Message for SlowResponse {
    fn encode_raw(&self, buffer: &mut impl bytes::BufMut) {
        self.0.encode_raw(buffer);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire: rpc::prost::encoding::WireType,
        buffer: &mut impl bytes::Buf,
        context: rpc::prost::encoding::DecodeContext,
    ) -> Result<(), rpc::prost::DecodeError> {
        SLOW_DECODE_HIT.store(true, std::sync::atomic::Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(100));
        self.0.merge_field(tag, wire, buffer, context)
    }

    fn encoded_len(&self) -> usize {
        self.0.encoded_len()
    }

    fn clear(&mut self) {
        self.0.clear();
    }
}

fn application() -> Result<Arc<dyn Application>> {
    let retained: Arc<Mutex<Option<CounterClient>>> = Arc::new(Mutex::new(None));
    let constructors = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let factory_count = constructors.clone();
    let contract = Arc::new(rpc::Contract::from_descriptor(
        include_bytes!("../../../tools/rpc/fixtures/counter.binpb"),
        rpc::Limits {
            max_decoded_slots: 8,
            ..Default::default()
        },
    )?);
    let declared = contract.clone();
    let deadline_contract = contract.clone();
    let app = Registry::new(move |context, invocation| {
        let retained = retained.clone();
        let constructors = constructors.clone();
        let contract = contract.clone();
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else { anyhow::bail!("expected HTTP ingress"); };
            let url = url::Url::parse(&request.url)?;
            let counter = CounterClient::bind(&context, "COUNTERS", "alice")?;
            let value = match url.path() {
                "/client-limits" => {
                    LIMIT_PROBE_ENCODED.store(false, std::sync::atomic::Ordering::SeqCst);
                    let generated_defaults = Arc::new(rpc::Contract::from_descriptor(
                        include_bytes!("../../../tools/rpc/fixtures/counter.binpb"), rpc::Limits::default(),
                    )?);
                    let target = rpc::Client::bind(&context,"COUNTERS","unconstructed-client-limits",generated_defaults)?;
                    let before = constructors.load(std::sync::atomic::Ordering::SeqCst);
                    let failure = target.call::<_,GetCountResponse>("example.counter.v1.Counter/GetCount",LimitProbe,rpc::CallOptions::default()).await.unwrap_err();
                    ensure!(!LIMIT_PROBE_ENCODED.load(std::sync::atomic::Ordering::SeqCst),"client used generated default limits before encoding");
                    ensure!(constructors.load(std::sync::atomic::Ordering::SeqCst) == before,"local codec refusal submitted a call");
                    serde_json::json!({"code":failure.code(),"outcome":failure.outcome()})
                }
                "/handler-limits" => {
                    LIMIT_PROBE_ENCODED.store(false, std::sync::atomic::Ordering::SeqCst);
                    let target = CounterClient::bind(&context,"COUNTERS","handler-limit-probe")?;
                    let failure = target.get_count(GetCountRequest{}).await.unwrap_err();
                    ensure!(!LIMIT_PROBE_ENCODED.load(std::sync::atomic::Ordering::SeqCst),"handler used generated default limits before encoding");
                    serde_json::json!({"code":failure.code(),"outcome":failure.outcome()})
                }
                "/layout-request" => {
                    let before = constructors.load(std::sync::atomic::Ordering::SeqCst);
                    let target = context.namespace("COUNTERS")?.get_by_name("unconstructed-layout");
                    let args = rpc::encode_envelope(contract.digest(),[18,0].repeat(6),contract.limits().max_message_bytes)?;
                    let response = target.rpc(&context,"example.counter.v1.Counter/Increment",args).await?;
                    let failure = rpc::decode_reply(contract.digest(),&response,contract.limits()).unwrap_err();
                    ensure!(constructors.load(std::sync::atomic::Ordering::SeqCst) == before,"layout refusal constructed a business cell");
                    serde_json::json!({"code":failure.code(),"outcome":failure.outcome()})
                }
                "/oversized-request" => {
                    let before = constructors.load(std::sync::atomic::Ordering::SeqCst);
                    let target = CounterClient::bind(&context, "COUNTERS", "unconstructed-oversized")?;
                    let failure = target.increment(IncrementRequest {delta:Some(1), operation_id:"x".repeat(1024 * 1024 + 1)}).await.unwrap_err();
                    ensure!(constructors.load(std::sync::atomic::Ordering::SeqCst) == before);
                    serde_json::json!({"code":failure.code(),"outcome":failure.outcome()})
                }
                "/oversized-reply" | "/oversized-retry" => {
                    let target = CounterClient::bind(&context, "COUNTERS", "oversized-response")?;
                    match target.increment(IncrementRequest {delta:Some(1), operation_id:"oversized-reply".into()}).await {
                        Err(failure) => serde_json::json!({"code":failure.code(),"outcome":failure.outcome()}),
                        Ok(reply) => match reply.result {Some(increment_response::Result::Count(value)) => serde_json::json!({"value":value.value}), _ => anyhow::bail!("missing receipt")},
                    }
                }
                "/pre-factory" => {
                    let before = constructors.load(std::sync::atomic::Ordering::SeqCst);
                    let target = context.namespace("COUNTERS")?.get_by_name("unconstructed");
                    for (method, digest, bytes, expected) in [
                        ("example.counter.v1.Counter/PrivateHelper", contract.digest().to_string(), vec![], rpc::Code::MethodNotFound),
                        ("example.counter.v1.Counter/GetCount", "f".repeat(64), vec![], rpc::Code::ContractMismatch),
                        ("example.counter.v1.Counter/Increment", contract.digest().to_string(), vec![8], rpc::Code::InvalidArgument),
                    ] {
                        let args = rpc::encode_envelope(&digest, bytes, contract.limits().max_message_bytes)?;
                        let reply = target.rpc(&context, method, args).await?;
                        let failure = rpc::decode_reply(&digest, &reply, contract.limits()).unwrap_err();
                        ensure!(failure.code() == expected && failure.outcome() == rpc::EffectOutcome::NotCommitted);
                    }
                    ensure!(constructors.load(std::sync::atomic::Ordering::SeqCst) == before, "refused RPC executed a business constructor");
                    serde_json::json!({"refused_before_factory": true})
                }
                "/read" => serde_json::json!({"value": counter.get_count(GetCountRequest {}).await?.count.unwrap().value.to_string()}),
                "/capture" => { *retained.lock().unwrap() = Some(counter); serde_json::json!({"captured": true}) }
                "/retained" => {
                    let previous = retained.lock().unwrap().clone().unwrap();
                    let failure = previous.get_count(GetCountRequest {}).await.unwrap_err();
                    serde_json::json!({"code": failure.code(), "outcome": failure.outcome()})
                }
                "/cancelled" => {
                    let controller = cellar::sdk::web::AbortController::new();
                    controller.abort(None)?;
                    let failure = counter.get_count_with_options(GetCountRequest {}, rpc::CallOptions { signal: Some(controller.signal()), ..Default::default() }).await.unwrap_err();
                    serde_json::json!({"code": failure.code(), "outcome": failure.outcome()})
                }
                "/cancelled-after-create" => {
                    let controller = cellar::sdk::web::AbortController::new();
                    let call = counter.get_count_with_options(GetCountRequest {}, rpc::CallOptions {
                        signal: Some(controller.signal()), ..Default::default()
                    });
                    controller.abort(None)?;
                    let failure = call.await.unwrap_err();
                    serde_json::json!({"code": failure.code(), "outcome": failure.outcome()})
                }
                "/slow-decode" => {
                    SLOW_DECODE_HIT.store(false, std::sync::atomic::Ordering::SeqCst);
                    let contract = Arc::new(rpc::Contract::from_descriptor(include_bytes!("../../../tools/rpc/fixtures/counter.binpb"), rpc::Limits::default())?);
                    let target = rpc::Client::bind(&context, "COUNTERS", "alice", contract)?;
                    let failure = target.call::<_, SlowResponse>("example.counter.v1.Counter/GetCount", GetCountRequest {}, rpc::CallOptions { timeout: Some(Duration::from_millis(20)), ..Default::default() }).await.err().ok_or_else(|| anyhow::anyhow!("slow response bypassed deadline"))?;
                    serde_json::json!({"code": failure.code(), "outcome": failure.outcome(), "decoded": SLOW_DECODE_HIT.load(std::sync::atomic::Ordering::SeqCst)})
                }
                "/expired" => {
                    let failure = counter.get_count_with_options(GetCountRequest {}, rpc::CallOptions { timeout: Some(Duration::ZERO), ..Default::default() }).await.unwrap_err();
                    serde_json::json!({"code": failure.code(), "outcome": failure.outcome()})
                }
                "/parent-deadline" => {
                    EXPIRED_PARENT_PROBE.store(0, std::sync::atomic::Ordering::SeqCst);
                    let target = CounterClient::bind(&context, "COUNTERS", "deadline-parent")?;
                    let failure = target.get_count_with_options(GetCountRequest {}, rpc::CallOptions {
                        timeout: Some(Duration::from_millis(50)), ..Default::default()
                    }).await.unwrap_err();
                    // The target deliberately blocks a native poll. Its SDK
                    // authority must expire even before its driver can poll
                    // cancellation again; the ingress owns a different turn.
                    for _ in 0..200 {
                        if EXPIRED_PARENT_PROBE.load(std::sync::atomic::Ordering::SeqCst) != 0 {
                            break;
                        }
                        cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                    }
                    let recovered = target.get_count(GetCountRequest {}).await?;
                    serde_json::json!({
                        "code": failure.code(), "outcome": failure.outcome(),
                        "probe": EXPIRED_PARENT_PROBE.load(std::sync::atomic::Ordering::SeqCst),
                        "late_writes": recovered.count.unwrap().value,
                    })
                }
                _ => {
                    let query: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
                    let delta = query.get("delta").map(|s| s.parse()).transpose()?;
                    let operation_id = query.get("id").cloned().unwrap_or_default();
                    match counter.increment(IncrementRequest { delta, operation_id }).await?.result {
                        Some(increment_response::Result::Count(count)) => serde_json::json!({"value": count.value.to_string()}),
                        Some(increment_response::Result::OutOfRange(_)) => serde_json::json!({"error": "OutOfRange"}),
                        Some(increment_response::Result::OperationConflict(_)) => serde_json::json!({"error": "OperationConflict"}),
                        None => anyhow::bail!("missing business result"),
                    }
                }
            };
            Ok(Output::Http(http::Response::from_json(&value)?.into_http(&context)?))
        })
    }).cell("Counter", move |_, identity| {
        factory_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if identity.name.as_deref() == Some("deadline-parent") {
            let router = rpc::Router::new().method_with_limits(
                deadline_contract.clone(), "example.counter.v1.Counter/GetCount", |context, _, limits| Box::pin(async move {
                    let cached_storage = context.storage().map_err(|_| rpc::Failure::new(rpc::Code::Internal))?;
                    if EXPIRED_PARENT_PROBE.load(std::sync::atomic::Ordering::SeqCst) != 0 {
                        let mut late_writes = 0;
                        for key in ["expired-write", "expired-transaction"] {
                            late_writes += i64::from(cached_storage.get_json(key)
                                .map_err(|_| rpc::Failure::new(rpc::Code::Internal))?.is_some());
                        }
                        return rpc::encode_message(GetCountResponse {
                            count: Some(generated::Count { value: late_writes }),
                        }, limits);
                    }
                    let commit_refused = cached_storage.transaction_sync(|transaction| {
                        transaction.put_json("expired-transaction", &serde_json::json!(true))?;
                        std::thread::sleep(Duration::from_millis(200));
                        Ok(())
                    }).is_err();
                    let child_refused = CounterClient::bind(&context, "COUNTERS", "deadline-child").is_err();
                    let storage_refused = context.storage().is_err();
                    let cached_write_refused = cached_storage.put_json("expired-write", &serde_json::json!(true)).is_err();
                    EXPIRED_PARENT_PROBE.store(
                        4 | u8::from(child_refused) | (u8::from(storage_refused) << 1)
                            | (u8::from(cached_write_refused) << 3) | (u8::from(commit_refused) << 4),
                        std::sync::atomic::Ordering::SeqCst,
                    );
                    Err(rpc::Failure::new(rpc::Code::Internal))
                }),
            )?;
            return Ok(Arc::new(router));
        }
        if identity.name.as_deref() == Some("handler-limit-probe") {
            // Exercise the same codec-aware router entry point emitted by the
            // generator, using its default contract against a stricter catalog.
            let generated_defaults = Arc::new(rpc::Contract::from_descriptor(
                include_bytes!("../../../tools/rpc/fixtures/counter.binpb"), rpc::Limits::default(),
            )?);
            let mut router = rpc::Router::new();
            for method in ["example.counter.v1.Counter/Increment","example.counter.v1.Counter/GetCount"] {
                router = router.method_with_limits(generated_defaults.clone(),method,|_,_,limits| Box::pin(async move {
                    rpc::encode_message(LimitProbe,limits).map_err(|_| rpc::Failure::new(rpc::Code::InvalidResponse))
                }))?;
            }
            router.validate()?;
            return Ok(Arc::new(router));
        }
        // One business object and method table per native activation. Static
        // descriptor metadata is shared, while application fields stay local.
        Ok(Arc::new(CounterRegistration::register(
            rpc::Router::new(), Arc::new(Counter),
        )?))
    })?.rpc_service("Counter", "example.counter.v1.Counter", declared)?;
    Ok(Arc::new(app))
}

async fn get(client: &reqwest::Client, address: &str, path: &str) -> Result<serde_json::Value> {
    let response = client.get(format!("{address}{path}")).send().await?;
    let status = response.status();
    let text = response.text().await?;
    ensure!(status.is_success(), "typed RPC failed: {status}: {text}");
    Ok(serde_json::from_str(&text)?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_native_rpc_is_typed_scoped_and_recovers_receipts() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let client = reqwest::Client::new();
    for run in 0..2 {
        let (node, address) = native::start(directory.path(), application()?).await?;
        let before = if run == 0 { "0" } else { "9007199254740993" };
        assert_eq!(
            get(&client, &address, "/client-limits").await?,
            serde_json::json!({"code":"ResourceExhausted","outcome":"not_committed"})
        );
        assert_eq!(
            get(&client, &address, "/handler-limits").await?,
            serde_json::json!({"code":"InvalidResponse","outcome":"unknown"})
        );
        assert_eq!(
            get(&client, &address, "/layout-request").await?,
            serde_json::json!({"code":"ResourceExhausted","outcome":"not_committed"})
        );
        assert_eq!(
            get(&client, &address, "/oversized-request").await?,
            serde_json::json!({"code":"ResourceExhausted","outcome":"not_committed"})
        );
        if run == 0 {
            assert_eq!(
                get(&client, &address, "/oversized-reply").await?,
                serde_json::json!({"code":"InvalidResponse","outcome":"unknown"})
            );
        }
        assert_eq!(
            get(&client, &address, "/oversized-retry").await?,
            serde_json::json!({"value":1})
        );
        assert_eq!(
            get(&client, &address, "/pre-factory").await?["refused_before_factory"],
            true
        );
        assert_eq!(get(&client, &address, "/read").await?["value"], before);
        // Greater than JavaScript's safe integer range: never pass via JSON numbers.
        let path = "/increment?delta=9007199254740993&id=order-123";
        assert_eq!(
            get(&client, &address, path).await?["value"],
            "9007199254740993"
        );
        assert_eq!(
            get(&client, &address, path).await?["value"],
            "9007199254740993"
        );
        assert_eq!(
            get(&client, &address, "/increment?delta=1&id=order-123").await?["error"],
            "OperationConflict"
        );
        assert_eq!(
            get(&client, &address, "/increment?id=missing-delta").await?["error"],
            "OutOfRange"
        );
        assert_eq!(
            get(
                &client,
                &address,
                "/increment?delta=9223372036854775807&id=overflow"
            )
            .await?["error"],
            "OutOfRange"
        );
        get(&client, &address, "/capture").await?;
        for (path, code) in [
            ("/retained", "ContextExpired"),
            ("/cancelled", "Cancelled"),
            ("/cancelled-after-create", "Cancelled"),
            ("/expired", "DeadlineExceeded"),
        ] {
            let failure = get(&client, &address, path).await?;
            assert_eq!(failure["code"], code);
            assert_eq!(failure["outcome"], "not_committed");
        }
        assert_eq!(
            get(&client, &address, "/read").await?["value"],
            "9007199254740993"
        );
        let slow = get(&client, &address, "/slow-decode").await?;
        assert_eq!(slow["code"], "DeadlineExceeded");
        assert_eq!(slow["outcome"], "unknown");
        assert_eq!(slow["decoded"], true);
        let parent = get(&client, &address, "/parent-deadline").await?;
        assert_eq!(parent["code"], "DeadlineExceeded");
        assert_eq!(parent["outcome"], "unknown");
        assert_eq!(parent["probe"], 31, "expired target retained SDK authority");
        assert_eq!(
            parent["late_writes"], 0,
            "deadline prevented rollback or admitted a late write"
        );
        let NodeOutcome::Stopped(report) = node.shutdown(ShutdownMode::Preserve).await? else {
            anyhow::bail!("typed RPC node failed during shutdown");
        };
        ensure!(
            report.ingress_drained
                && report.application_work_drained
                && report.cells_drained
                && !report.drain_timed_out
        );
    }
    Ok(())
}
