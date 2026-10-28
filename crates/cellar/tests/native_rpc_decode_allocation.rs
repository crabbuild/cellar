// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Generated native decoders reserve their transitive inline layout before allocation.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

#[derive(Clone, Copy, Default)]
struct Observation {
    active: bool,
    live: isize,
    peak: usize,
}

thread_local! {
    // Constant TLS needs no heap allocation inside allocator callbacks. Other
    // harness threads and allocations outside the decode are excluded.
    static USAGE: Cell<Observation> = const { Cell::new(Observation {
        active: false, live: 0, peak: 0,
    }) };
}

fn record(delta: isize) {
    // Allocator callbacks must also tolerate thread-local teardown.
    let _ = USAGE.try_with(|usage| {
        let mut observation = usage.get();
        if observation.active {
            observation.live += delta;
            observation.peak = observation.peak.max(observation.live.max(0) as usize);
            usage.set(observation);
        }
    });
}

struct Measured;

// SAFETY: every allocation is forwarded to System with its original layout;
// thread-local counters do not allocate or change allocation ownership.
unsafe impl GlobalAlloc for Measured {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forward the caller's valid allocation layout unchanged.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record(layout.size() as isize);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        record(-(layout.size() as isize));
        // SAFETY: this pointer was allocated by System with this layout.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: preserve the original allocation's pointer/layout and the
        // caller's new size. Failed reallocations keep their old ownership.
        let next = unsafe { System.realloc(pointer, layout, size) };
        if !next.is_null() {
            record(size as isize - layout.size() as isize);
        }
        next
    }
}

#[global_allocator]
static ALLOCATOR: Measured = Measured;

use anyhow::{ensure, Result};
use cellar::sdk::{rpc, *};
use rpc::prost::Message;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

#[allow(dead_code)]
#[path = "../../../tools/rpc/fixtures/native-layout-generated/rust/cellar.rpc.native_layout.v1.rs"]
mod generated;
#[path = "../../cellar-javascript/tests/support/native.rs"]
mod native;
use generated::{Batch, LayoutClient, LayoutHandler, LayoutRegistration, Outer};

const BATCH: &str = "cellar.rpc.native_layout.v1.Batch";
const SERVICE: &str = "cellar.rpc.native_layout.v1.Layout";
const LOW_BUDGET: usize = 1024 * 1024;

fn contract() -> Arc<rpc::Contract> {
    Arc::new(
        rpc::Contract::from_descriptor(
            include_bytes!("../../../tools/rpc/fixtures/native-layout.binpb"),
            rpc::Limits {
                max_decoded_slots: 386,
                ..Default::default()
            },
        )
        .unwrap(),
    )
}

fn wire() -> Vec<u8> {
    [10, 0].repeat(128)
}

fn recursive_wire() -> Vec<u8> {
    fn message(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut value = vec![tag];
        let mut length = body.len();
        while length >= 128 {
            value.push((length as u8 & 127) | 128);
            length >>= 7;
        }
        value.push(length as u8);
        value.extend_from_slice(body);
        value
    }
    let mut value = Vec::new();
    for _ in 1..32 {
        value = message(18, &message(18, &value));
    }
    message(10, &value)
}

#[test]
// This bounded subprocess driver owns a real OS clock outside node execution.
#[allow(clippy::disallowed_methods)]
fn native_decoder_stack_limits_are_checked_in_a_subprocess() {
    let Some(mode) = std::env::var_os("CELLAR_NATIVE_STACK_PROBE") else {
        // A missing stack guard must fail only this owned process. The prior
        // main-lock diagnostic reproduced SIGABRT under a 256 KiB thread stack.
        for mode in ["small", "large"] {
            let directory = tempfile::tempdir().unwrap();
            let executable = std::env::current_exe().unwrap();
            #[cfg(unix)]
            let mut command = {
                let mut command = std::process::Command::new("/bin/sh");
                command
                    .args([
                        "-c",
                        "ulimit -c 0\nexec \"$@\"",
                        "cellar-native-stack-probe",
                    ])
                    .arg(executable);
                command
            };
            #[cfg(not(unix))]
            let mut command = std::process::Command::new(executable);
            command
                .args([
                    "--exact",
                    "native_decoder_stack_limits_are_checked_in_a_subprocess",
                    "--test-threads=1",
                ])
                .env("CELLAR_NATIVE_STACK_PROBE", mode)
                .current_dir(directory.path())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            let mut child = command.spawn().unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            while child.try_wait().unwrap().is_none() {
                if std::time::Instant::now() >= deadline {
                    child.kill().unwrap();
                    let output = child.wait_with_output().unwrap();
                    panic!("native {mode} stack probe exceeded its bound: {output:?}");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let output = child.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "native {mode} stack probe failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return;
    };
    let small = mode == "small";
    let stack = if small { 512 * 1024 } else { 32 * 1024 * 1024 };
    std::thread::Builder::new()
        .stack_size(stack)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let domain =
                    cellar::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
                domain
                    .services()
                    .configure_runtime(RuntimePolicy {
                        max_native_rpc_stack_bytes: if small {
                            512 * 1024
                        } else {
                            24 * 1024 * 1024
                        },
                        ..Default::default()
                    })
                    .unwrap();
                domain
                    .run(async {
                        let contract = contract();
                        let wire = recursive_wire();
                        let name = "cellar.rpc.native_layout.v1.RecursiveBatch";
                        contract.validate_message(name, &wire).unwrap();
                        // Siblings and one shallow message must not accumulate as
                        // active recursion. The deep wire still fits portable limits.
                        let shallow = [10, 0];
                        let held = rpc::DecodeReservation::new(&contract, name, &shallow).unwrap();
                        rpc::discard_message(
                            generated::RecursiveBatch::decode(shallow.as_slice()).unwrap(),
                        );
                        drop(held);
                        match rpc::DecodeReservation::new(&contract, name, &wire) {
                            Err(failure) => {
                                assert!(small, "adequately configured stack refused the profile");
                                assert_eq!(failure.code(), rpc::Code::ResourceExhausted);
                                assert_eq!(failure.outcome(), rpc::EffectOutcome::Unknown);
                            }
                            Ok(_held) => {
                                let decoded =
                                    generated::RecursiveBatch::decode(wire.as_slice()).unwrap();
                                assert_eq!(decoded.values.len(), 1);
                                rpc::discard_message(decoded);
                                assert!(
                                    !small,
                                    "stack admission did not refuse the unsafe nesting"
                                );
                            }
                        }
                        assert_eq!(
                            domain.services().native_rpc_decode_usage(),
                            Default::default()
                        );
                    })
                    .await
                    .unwrap();
                domain.shutdown().await;
            });
        })
        .unwrap()
        .join()
        .unwrap();
}

fn batch() -> Batch {
    Batch {
        values: (0..128).map(|_| Outer::default()).collect(),
    }
}

fn measured<T>(decode: impl FnOnce() -> T) -> (T, Observation) {
    USAGE.with(|usage| {
        usage.set(Observation {
            active: true,
            ..Default::default()
        })
    });
    let value = decode();
    let observation = USAGE.with(|usage| {
        let value = usage.get();
        usage.set(Observation::default());
        value
    });
    (value, observation)
}

#[tokio::test]
async fn actual_generated_inline_layout_is_larger_than_the_portable_slot_estimate() -> Result<()> {
    let contract = contract();
    let wire = wire();
    contract.validate_message(BATCH, &wire)?;
    assert_eq!(wire.len(), 256);
    assert_eq!(
        std::mem::size_of::<Outer>(),
        512 * std::mem::size_of::<String>()
    );
    let (decoded, observation) = measured(|| Batch::decode(wire.as_slice()).unwrap());
    assert_eq!(decoded.values.len(), 128);
    assert!(decoded.values.iter().all(|value| value.absent.is_none()));
    assert_eq!(
        observation.live,
        (128 * std::mem::size_of::<Outer>()) as isize
    );
    assert!(
        observation.peak > LOW_BUDGET,
        "fixture no longer reproduces hidden inline allocation"
    );

    let domain = cellar::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let held = rpc::DecodeReservation::new(&contract, BATCH, &wire)?;
            assert!(held.reserved_bytes() >= observation.peak);
            assert_eq!(
                domain.services().native_rpc_decode_usage().active_decodes,
                1
            );
            drop(held);
            assert_eq!(
                domain.services().native_rpc_decode_usage(),
                Default::default()
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn oneof_and_default_map_layouts_fit_their_native_allowances() -> Result<()> {
    let domain = cellar::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let contract = contract();
            for message in ["Variants", "Mapped"] {
                let wire = [10, 0].repeat(16);
                let name = format!("cellar.rpc.native_layout.v1.{message}");
                contract.validate_message(&name, &wire)?;
                let held = rpc::DecodeReservation::new(&contract, &name, &wire)?;
                let observation = if message == "Variants" {
                    let (decoded, observed) =
                        measured(|| generated::Variants::decode(wire.as_slice()).unwrap());
                    assert_eq!(decoded.values.len(), 16);
                    assert!(decoded.values.iter().all(|value| value.value.is_none()));
                    observed
                } else {
                    let (decoded, observed) =
                        measured(|| generated::Mapped::decode(wire.as_slice()).unwrap());
                    assert_eq!(
                        decoded.values.len(),
                        1,
                        "default duplicate map key did not merge"
                    );
                    assert!(decoded.values[""].absent.is_none());
                    observed
                };
                assert!(
                    observation.peak <= held.reserved_bytes(),
                    "{message} exceeded its decoder allowance"
                );
                drop(held);
            }
            assert_eq!(
                domain.services().native_rpc_decode_usage(),
                Default::default()
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[derive(Default)]
struct Counts {
    echo: AtomicUsize,
    read: AtomicUsize,
    hold: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl LayoutHandler for Counts {
    fn echo(self: Arc<Self>, _: Context, request: Batch) -> rpc::HandlerFuture<Batch> {
        self.echo.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            self.entered.notify_one();
            if self.hold.load(Ordering::SeqCst) {
                self.release.notified().await;
            }
            Ok(request)
        })
    }

    fn read(self: Arc<Self>, _: Context, _: ()) -> rpc::HandlerFuture<Batch> {
        self.read.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(batch()) })
    }
}

static RESPONSE_MERGES: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct ObservedBatch(Batch);

impl Message for ObservedBatch {
    fn encode_raw(&self, bytes: &mut impl bytes::BufMut) {
        self.0.encode_raw(bytes);
    }

    fn encoded_len(&self) -> usize {
        self.0.encoded_len()
    }

    fn merge_field(
        &mut self,
        tag: u32,
        kind: rpc::prost::encoding::WireType,
        bytes: &mut impl bytes::Buf,
        context: rpc::prost::encoding::DecodeContext,
    ) -> Result<(), rpc::prost::DecodeError> {
        RESPONSE_MERGES.fetch_add(1, Ordering::SeqCst);
        self.0.merge_field(tag, kind, bytes, context)
    }

    fn clear(&mut self) {
        self.0.clear();
    }
}

fn application(counts: Arc<Counts>) -> Result<Arc<dyn Application>> {
    let declared = contract();
    let ingress = declared.clone();
    let factory = counts.clone();
    let app = Registry::new(move |context, invocation| {
        let contract = ingress.clone();
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                anyhow::bail!("expected HTTP ingress")
            };
            let path = url::Url::parse(&request.url)?.path().to_string();
            let result = match path.as_str() {
                "/output" => {
                    let client =
                        rpc::Client::bind(&context, "COUNTERS", "budget", contract.clone())?;
                    client
                        .call::<(), ObservedBatch>(
                            &format!("{SERVICE}/Read"),
                            (),
                            Default::default(),
                        )
                        .await
                        .map(|value| value.0.values.len())
                }
                "/generated-output" => LayoutClient::bind(&context, "COUNTERS", "budget")?
                    .read(())
                    .await
                    .map(|value| value.values.len()),
                _ => {
                    let name = if path == "/other-input" {
                        "other"
                    } else {
                        "budget"
                    };
                    let args = rpc::encode_envelope(
                        contract.digest(),
                        wire(),
                        contract.limits().max_message_bytes,
                    )?;
                    let reply = context
                        .namespace("COUNTERS")?
                        .get_by_name(name)
                        .rpc(&context, format!("{SERVICE}/Echo"), args)
                        .await?;
                    rpc::decode_reply(contract.digest(), &reply, contract.limits())
                        .map(|value| value.len())
                }
            };
            let value = match result {
                Ok(length) => serde_json::json!({"ok":true,"length":length}),
                Err(failure) => serde_json::to_value(failure)?,
            };
            Ok(Output::Http(
                http::Response::from_json(&value)?.into_http(&context)?,
            ))
        })
    })
    .cell("Counter", move |_, _| {
        Ok(Arc::new(LayoutRegistration::register(
            rpc::Router::new(),
            factory.clone(),
        )?))
    })?
    .rpc_service("Counter", SERVICE, declared)?;
    Ok(Arc::new(app))
}

async fn start(
    path: &std::path::Path,
    counts: Arc<Counts>,
    maximum: usize,
) -> Result<(Node, String)> {
    native::start_configured(path, application(counts)?, move |config, _, _| {
        config.runtime.max_native_rpc_decode_bytes = maximum;
        // This new resource fixture must distinguish cancellation from the
        // existing five-second call deadline; other fixture budgets are unchanged.
        config.runtime.handler_budget = Duration::from_secs(10);
    })
    .await
}

async fn request(client: &reqwest::Client, base: &str, path: &str) -> Result<serde_json::Value> {
    let response = client.get(format!("{base}{path}")).send().await?;
    let status = response.status();
    let text = response.text().await?;
    ensure!(
        status.is_success(),
        "native decoder request {path}: {status}: {text}"
    );
    Ok(serde_json::from_str(&text)?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_provider_and_native_client_refuse_before_protobuf_decode() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let counts = Arc::new(Counts::default());
    RESPONSE_MERGES.store(0, Ordering::SeqCst);
    let (node, base) = start(directory.path(), counts.clone(), LOW_BUDGET).await?;
    let services = node.services();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let replies = async {
        Ok::<_, anyhow::Error>([
            request(&client, &base, "/input").await?,
            request(&client, &base, "/output").await?,
            request(&client, &base, "/generated-output").await?,
        ])
    }
    .await;
    let stopped = node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(
        matches!(stopped, NodeOutcome::Stopped(_)),
        "native resource node did not stop"
    );
    assert_eq!(services.native_rpc_decode_usage(), Default::default());
    for reply in replies? {
        assert_eq!(
            reply,
            serde_json::json!({"code":"ResourceExhausted","outcome":"unknown"})
        );
    }
    assert_eq!(
        counts.echo.load(Ordering::SeqCst),
        0,
        "decoder refusal entered the generated handler"
    );
    assert_eq!(counts.read.load(Ordering::SeqCst), 2);
    assert_eq!(
        RESPONSE_MERGES.load(Ordering::SeqCst),
        0,
        "response admission happened after Prost decoding"
    );
    Ok(())
}

#[tokio::test]
async fn native_decode_pool_tracks_owned_work_and_refunds_after_domain_close() -> Result<()> {
    let contract = contract();
    let wire = wire();
    let sample = cellar::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let weight = sample
        .run(async {
            rpc::DecodeReservation::new(&contract, BATCH, &wire).map(|held| held.reserved_bytes())
        })
        .await??;
    sample.shutdown().await;
    let domain = cellar::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain.services().configure_runtime(RuntimePolicy {
        max_native_rpc_decode_bytes: weight,
        ..Default::default()
    })?;
    let held = domain
        .run(async {
            let held = rpc::DecodeReservation::new(&contract, BATCH, &wire)?;
            let failure = rpc::DecodeReservation::new(&contract, BATCH, &wire)
                .err()
                .expect("saturated decoder pool admitted another graph");
            assert_eq!(failure.code(), rpc::Code::ResourceExhausted);
            assert_eq!(
                domain
                    .services()
                    .native_rpc_decode_usage()
                    .reserved_decode_bytes,
                weight
            );
            Ok::<_, anyhow::Error>(held)
        })
        .await??;
    domain.shutdown().await;
    assert_eq!(
        domain.services().native_rpc_decode_usage().active_decodes,
        1,
        "shutdown released externally owned admission"
    );
    let failure = domain
        .run(async { rpc::DecodeReservation::new(&contract, BATCH, &wire) })
        .await;
    assert!(failure.is_err(), "closed domain accepted new execution");
    std::thread::spawn(move || drop(held)).join().unwrap();
    assert_eq!(
        domain.services().native_rpc_decode_usage(),
        Default::default()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_pending_handler_keeps_admission_until_cancellation_and_fresh_work() -> Result<()>
{
    let orchestration = cellar::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = orchestration
        .run(async {
            let sample = cellar::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
            let contract = contract();
            let wire = wire();
            let weight = sample
                .run(async {
                    rpc::DecodeReservation::new(&contract, BATCH, &wire)
                        .map(|held| held.reserved_bytes())
                })
                .await??;
            sample.shutdown().await;
            let directory = tempfile::tempdir()?;
            let counts = Arc::new(Counts::default());
            counts.hold.store(true, Ordering::SeqCst);
            let (node, base) = start(directory.path(), counts.clone(), weight).await?;
            let services = node.services();
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()?;
            let operation = async {
                let calling = client.clone();
                let destination = base.clone();
                let started = cellar::asyncrt::mono_ms();
                let pending = cellar::asyncrt::spawn(async move {
                    request(&calling, &destination, "/input").await
                });
                cellar::asyncrt::timeout(Duration::from_secs(1), counts.entered.notified()).await?;
                assert_eq!(services.native_rpc_decode_usage().active_decodes, 1);
                assert_eq!(
                    services.native_rpc_decode_usage().reserved_decode_bytes,
                    weight
                );
                assert_eq!(
                    request(&client, &base, "/other-input").await?,
                    serde_json::json!({"code":"ResourceExhausted","outcome":"unknown"})
                );
                assert_eq!(
                    counts.echo.load(Ordering::SeqCst),
                    1,
                    "saturated decoder entered a second handler"
                );
                ensure!(
                    cellar::asyncrt::mono_ms().saturating_sub(started) < 2_000,
                    "probe cannot distinguish cancellation from its five-second call deadline"
                );
                assert_eq!(services.native_rpc_decode_usage().active_decodes, 1);
                pending.abort();
                assert!(pending.await.is_err());
                cellar::asyncrt::timeout(Duration::from_secs(1), async {
                    while services.native_rpc_decode_usage() != Default::default() {
                        cellar::asyncrt::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await?;
                counts.hold.store(false, Ordering::SeqCst);
                assert_eq!(request(&client, &base, "/input").await?["ok"], true);
                assert_eq!(counts.echo.load(Ordering::SeqCst), 2);
                Ok::<_, anyhow::Error>(())
            }
            .await;
            counts.hold.store(false, Ordering::SeqCst);
            counts.release.notify_waiters();
            let stopped = node.shutdown(ShutdownMode::Preserve).await?;
            ensure!(
                matches!(stopped, NodeOutcome::Stopped(_)),
                "native decoder node did not stop"
            );
            assert_eq!(services.native_rpc_decode_usage(), Default::default());
            operation
        })
        .await?;
    orchestration.shutdown().await;
    result
}
