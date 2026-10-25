// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real host calls in both directions, with one process-owned Python runtime.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{rpc, *};
use cellar_python::{EventKind, Options, PythonApplication, PythonRuntime, Source};
use std::{path::PathBuf, sync::Arc};

#[path = "../../../tools/rpc/native_counter.rs"]
mod counter;
#[allow(dead_code)]
#[path = "../../../tools/rpc/fixtures/generated/rust/example.counter.v1.rs"]
mod generated;
#[path = "../../cellar-javascript/tests/support/native.rs"]
mod native;

struct Mixed {
    python: Arc<PythonApplication>,
    rust: Arc<Registry>,
    catalog: rpc::Catalog,
    contract: Arc<rpc::Contract>,
}

impl Application for Mixed {
    fn rpc_catalog(&self) -> Option<&rpc::Catalog> {
        Some(&self.catalog)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.python.supports_cell(class) || self.rust.supports_cell(class)
    }

    fn validate_configuration(
        &self,
        options: &cellar::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        let mut selected = options.clone();
        selected
            .do_classes
            .retain(|class| self.python.supports_cell(class));
        self.python.validate_configuration(&selected)?;
        self.rust.validate_configuration(options)?;
        ensure!(
            options
                .do_classes
                .iter()
                .all(|class| self.supports_cell(class)),
            "unknown mixed RPC class"
        );
        Ok(())
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        if identity.class == "RustCounter" {
            self.rust.create_cell(context, identity)
        } else {
            self.python.create_cell(context, identity)
        }
    }

    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        let native = matches!(&event, Invocation::Fetch(request) if request.url.contains("/rust/") || request.url.ends_with("/native-preflight"));
        if !native {
            return self.python.clone().invoke(context, event);
        }
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                anyhow::bail!("expected HTTP request");
            };
            let url = url::Url::parse(&request.url)?;
            let query: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
            let binding = if query.get("target").is_some_and(|target| target == "rust") {
                "RUST"
            } else {
                "COUNTERS"
            };
            let value = if url.path() == "/rust/budget" {
                let source = rpc::BudgetSource::capture(&context)?;
                let budget = std::thread::spawn(move || source.start())
                    .join()
                    .map_err(|_| anyhow::anyhow!("captured budget clock panicked"))??;
                ensure!(
                    budget.remaining_ms()? <= 5_000,
                    "budget exceeded host policy"
                );
                let budget = budget.limit(20).limit(u32::MAX);
                std::thread::sleep(std::time::Duration::from_millis(40));
                let failure = budget.remaining_ms().unwrap_err();
                ensure!(failure.code() == rpc::Code::DeadlineExceeded);
                let target = rpc::Client::bind(
                    &context,
                    "RUST",
                    "budget-unconstructed",
                    self.contract.clone(),
                )?;
                let failure = target
                    .call_bytes_in_budget(
                        "example.counter.v1.Counter/GetCount",
                        vec![],
                        rpc::CallOptions::default(),
                        budget,
                    )
                    .await
                    .unwrap_err();
                ensure!(
                    failure.code() == rpc::Code::DeadlineExceeded
                        && failure.outcome() == rpc::EffectOutcome::NotCommitted
                );
                serde_json::json!({"expired":true, "captured_clock":true})
            } else if url.path() == "/rust/budget-parent" {
                let child = generated::CounterClient::bind(&context, "COUNTERS", "codec-parent")?;
                let reply = child
                    .get_count_with_options(
                        generated::GetCountRequest {},
                        rpc::CallOptions {
                            timeout: Some(std::time::Duration::from_millis(1000)),
                            ..Default::default()
                        },
                    )
                    .await?;
                let remaining = reply.count.context("budget parent did not reply")?.value;
                ensure!(
                    remaining > 0 && remaining <= 1000,
                    "guest extended its native parent"
                );
                serde_json::json!({"parent_capped": true})
            } else if url.path() == "/native-preflight" {
                let stub = context.namespace(binding)?.get_by_name("never-construct");
                let args = rpc::encode_envelope(
                    self.contract.digest(),
                    vec![],
                    self.contract.limits().max_message_bytes,
                )?;
                let response = stub
                    .rpc(&context, "example.counter.v1.Counter/private_helper", args)
                    .await?;
                let failure =
                    rpc::decode_reply(self.contract.digest(), &response, self.contract.limits())
                        .unwrap_err();
                ensure!(
                    failure.code() == rpc::Code::MethodNotFound
                        && failure.outcome() == rpc::EffectOutcome::NotCommitted
                );
                serde_json::json!({"refused": true})
            } else {
                let client = generated::CounterClient::bind(&context, binding, "alice")?;
                if url.path().ends_with("/read") {
                    serde_json::json!({"value": client.get_count(generated::GetCountRequest {}).await?.count.unwrap().value.to_string()})
                } else {
                    let result = client
                        .increment(generated::IncrementRequest {
                            delta: query.get("delta").map(|value| value.parse()).transpose()?,
                            operation_id: query.get("id").cloned().unwrap_or_default(),
                        })
                        .await;
                    match result {
                        Ok(response) => match response.result {
                            Some(generated::increment_response::Result::Count(count)) => {
                                serde_json::json!({"value": count.value.to_string()})
                            }
                            Some(generated::increment_response::Result::OperationConflict(_)) => {
                                serde_json::json!({"error":"operation_conflict"})
                            }
                            Some(generated::increment_response::Result::OutOfRange(_)) => {
                                serde_json::json!({"error":"out_of_range"})
                            }
                            None => anyhow::bail!("unset Counter result"),
                        },
                        Err(failure) => {
                            serde_json::json!({"code": failure.code(), "outcome": failure.outcome()})
                        }
                    }
                }
            };
            Ok(Output::Http(
                http::Response::from_json(&value)?.into_http(&context)?,
            ))
        })
    }
}

async fn request(client: &reqwest::Client, base: &str, path: &str) -> Result<serde_json::Value> {
    let response = client.get(format!("{base}{path}")).send().await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(
        status.is_success(),
        "RPC qualification failed: {status}: {body}"
    );
    Ok(serde_json::from_str(&body)?)
}

fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let contract = Arc::new(rpc::Contract::from_descriptor(
        include_bytes!("../../../tools/rpc/fixtures/counter.binpb"),
        rpc::Limits::default(),
    )?);
    let source = Source::new(include_str!("typed_rpc.py"), "typed_rpc.py")
        .events([EventKind::Fetch])
        .cell_rpc("Counter", "example.counter.v1.Counter", contract.clone())?
        .require_distribution("protobuf", "7.34.2");
    let mut options = Options::default();
    options.module_paths.push(
        root.join("tools/rpc/fixtures/generated/python")
            .canonicalize()?,
    );
    let site = std::env::var_os("CELLAR_RPC_PYTHON_SITE")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join(".cellar/rpc/python/lib/python3.14/site-packages"));
    options.module_paths.push(site.canonicalize()?);
    // SAFETY: this executable owns CPython on its main thread and starts the
    // Rust workers only inside the runtime's supervised host callback.
    let report = unsafe {
        PythonRuntime::run(options, source, move |python, _stop| {
            tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build()?.block_on(async move {
            let directory = tempfile::tempdir()?;
            let client = reqwest::Client::new();
            let large = 9_007_199_254_740_993_i64;
            for round in 0..2 {
                let mut catalog = rpc::Catalog::new();
                for class in ["Counter", "RustCounter"] { catalog.register(class, "example.counter.v1.Counter", contract.clone())?; }
                let rust = Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
                    .cell("RustCounter", |_, _| Ok(Arc::new(generated::CounterRegistration::register(rpc::Router::new(), Arc::new(counter::Counter))?)))?
                    .rpc_service("RustCounter", "example.counter.v1.Counter", contract.clone())?;
                let app = Arc::new(Mixed { python: python.clone(), rust: Arc::new(rust), catalog, contract: contract.clone() });
                let (node, base) = native::start_with(directory.path(), app, vec!["Counter".into(), "RustCounter".into()], vec![("COUNTERS".into(), "Counter".into()), ("RUST".into(), "RustCounter".into())]).await?;
                let constructors = request(&client, &base, "/python/constructors").await?["constructed"].clone();
                assert_eq!(request(&client, &base, "/native-preflight").await?["refused"], true);
                assert_eq!(request(&client, &base, "/python/constructors").await?["constructed"], constructors);
                for target in ["python", "rust"] {
                    let current = if round == 0 { 0 } else { large * 2 + i64::from(target == "python") };
                    assert_eq!(request(&client, &base, &format!("/python/read?target={target}")).await?, serde_json::json!({"value":current.to_string()}), "round={round}, target={target}");
                    for (index, caller) in ["python", "rust"].into_iter().enumerate() {
                        let path = format!("/{caller}/increment?target={target}&delta={large}&id={caller}-{target}");
                        let saved = (index as i64 + 1) * large;
                        for _ in 0..2 { assert_eq!(request(&client, &base, &path).await?["value"], saved.to_string()); }
                        assert_eq!(request(&client, &base, &format!("/{caller}/increment?target={target}&delta=1&id={caller}-{target}")).await?["error"], "operation_conflict");
                    }
                }
                request(&client, &base, "/python/capture").await?;
                let expired = request(&client, &base, "/python/expired").await?;
                assert_eq!(expired["code"], "ContextExpired");
                assert_eq!(expired["outcome"], "not_committed");
                let bad = request(&client, &base, "/rust/increment?delta=1&id=invalid-reply").await?;
                if round == 0 { assert_eq!(bad["code"], "InvalidResponse"); assert_eq!(bad["outcome"], "unknown"); }
                else { assert_eq!(bad["value"], (large * 2 + 1).to_string()); }
                assert_eq!(request(&client, &base, "/rust/read").await?["value"], (large * 2 + 1).to_string());
                assert_eq!(request(&client, &base, "/rust/budget").await?["captured_clock"], true);
                assert_eq!(request(&client, &base, "/rust/budget-parent").await?["parent_capped"], true);
                request(&client, &base, "/python/budget-capture").await?;
                let foreign = request(&client, &base, "/python/budget-foreign").await?;
                assert_eq!(foreign["code"], "ContextMismatch");
                assert_eq!(foreign["outcome"], "not_committed");
                for target in ["rust", "python"] {
                    for phase in ["verify", "encode", "decode"] {
                        let result = request(&client, &base, &format!("/python/codec-budget?target={target}&phase={phase}")).await?;
                        assert_eq!(result["code"], "DeadlineExceeded", "{target}/{phase}");
                        assert_eq!(result["outcome"], if phase == "decode" { "unknown" } else { "not_committed" });
                        assert_eq!(result["codec_entered"], phase);
                        assert_eq!(result["count"], if phase == "decode" { "1" } else { "0" });
                    }
                }
                let NodeOutcome::Stopped(shutdown) = node.shutdown(ShutdownMode::Preserve).await? else { anyhow::bail!("node failed"); };
                ensure!(shutdown.ingress_drained && shutdown.application_work_drained && shutdown.cells_drained && !shutdown.drain_timed_out);
            }
            Ok(())
        })
        })?
    };
    ensure!(
        report.complete(),
        "Python RPC runtime shutdown was incomplete"
    );
    Ok(())
}
