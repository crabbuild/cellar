// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Generated named methods through real Rust, V8 and Node cells, not an echo
//! transport. Build the closed V8 bundle and Node fixtures with tools/rpc first.
#![cfg(all(feature = "embed", feature = "node"))]
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{rpc, *};
use cellar_javascript::{
    EventKind, JavaScriptApplication, JavaScriptRuntime, NodeOptions, NodeRuntime, Options, Source,
};
use std::{path::PathBuf, sync::Arc, time::Duration};

#[test]
fn both_backends_refuse_stale_generated_codecs_before_readiness() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let contract = Arc::new(rpc::Contract::from_descriptor(
        include_bytes!("../../../tools/rpc/fixtures/counter.binpb"),
        rpc::Limits::default(),
    )?);
    let admitted = AtomicBool::new(false);
    let code = std::fs::read_to_string(root.join("tools/rpc/dist/stale.v8.js"))?;
    let source = Source::new(code, "rpc/stale.ts")
        .events([EventKind::Fetch])
        .cell_rpc("V8Counter", "example.counter.v1.Counter", contract.clone())?;
    let result = JavaScriptRuntime::run(Options::default(), source, |_| {
        admitted.store(true, Ordering::SeqCst);
        Ok(())
    });
    ensure!(
        result.is_err() && !admitted.load(Ordering::SeqCst),
        "stale V8 codec reached readiness"
    );
    let source = Source::module().events([EventKind::Fetch]).cell_rpc(
        "NodeCounter",
        "example.counter.v1.Counter",
        contract,
    )?;
    let result = NodeRuntime::run(
        Options::default(),
        NodeOptions::new(
            root.join("tools/rpc/dist/node/stale-node.mjs")
                .canonicalize()?,
        ),
        source,
        |_| {
            admitted.store(true, Ordering::SeqCst);
            Ok(())
        },
    );
    ensure!(
        result.is_err() && !admitted.load(Ordering::SeqCst),
        "stale Node codec reached readiness"
    );
    Ok(())
}

#[path = "../../../tools/rpc/native_counter.rs"]
mod counter;
#[allow(dead_code)]
#[path = "../../../tools/rpc/fixtures/generated/rust/example.counter.v1.rs"]
mod generated;
#[path = "support/joined_node.rs"]
mod joined_node;
#[path = "support/native.rs"]
mod native;

struct Mixed {
    rust: Arc<Registry>,
    v8: Arc<JavaScriptApplication>,
    node: Arc<JavaScriptApplication>,
    catalog: rpc::Catalog,
    contract: Arc<rpc::Contract>,
}

impl Application for Mixed {
    fn rpc_catalog(&self) -> Option<&rpc::Catalog> {
        Some(&self.catalog)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.rust.supports_cell(class)
            || self.v8.supports_cell(class)
            || self.node.supports_cell(class)
    }

    fn validate_configuration(
        &self,
        options: &cellar::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        for app in [&self.v8, &self.node] {
            let mut selected = options.clone();
            selected.do_classes.retain(|class| app.supports_cell(class));
            app.validate_configuration(&selected)?;
        }
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
        match identity.class.as_str() {
            "V8Counter" => self.v8.create_cell(context, identity),
            "NodeCounter" => self.node.create_cell(context, identity),
            _ => self.rust.create_cell(context, identity),
        }
    }

    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        if matches!(&event, Invocation::Fetch(request) if request.url.contains("/v8/")) {
            return self.v8.clone().invoke(context, event);
        }
        if matches!(&event, Invocation::Fetch(request) if request.url.contains("/node/")) {
            return self.node.clone().invoke(context, event);
        }
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                anyhow::bail!("expected HTTP ingress");
            };
            let url = reqwest::Url::parse(&request.url)?;
            let query: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
            let target = query.get("target").map(String::as_str).unwrap_or("v8");
            let binding = match target {
                "rust" => "RUST",
                "node" => "NODE",
                _ => "V8",
            };
            let value = if url.path().ends_with("/budget-parent") {
                let child = generated::CounterClient::bind(&context, binding, "codec-parent")?;
                let response = child
                    .get_count_with_options(
                        generated::GetCountRequest {},
                        rpc::CallOptions {
                            timeout: Some(Duration::from_millis(1000)),
                            ..Default::default()
                        },
                    )
                    .await?;
                let remaining = response.count.context("missing parent budget")?.value;
                ensure!(
                    remaining > 0 && remaining <= 1000,
                    "guest extended parent deadline"
                );
                serde_json::json!({"parent_capped":true})
            } else if url.path().ends_with("/preflight") {
                let stub = context.namespace(binding)?.get_by_name("never-construct");
                let args = rpc::encode_envelope(
                    self.contract.digest(),
                    vec![],
                    self.contract.limits().max_message_bytes,
                )?;
                let response = stub
                    .rpc(&context, "example.counter.v1.Counter/PrivateHelper", args)
                    .await?;
                let failure =
                    rpc::decode_reply(self.contract.digest(), &response, self.contract.limits())
                        .unwrap_err();
                ensure!(
                    failure.code() == rpc::Code::MethodNotFound
                        && failure.outcome() == rpc::EffectOutcome::NotCommitted
                );
                serde_json::json!({"refused":true})
            } else {
                let client = generated::CounterClient::bind(&context, binding, "alice")?;
                if url.path().ends_with("/read") {
                    serde_json::json!({"value":client.get_count(generated::GetCountRequest {}).await?.count.context("missing count")?.value.to_string()})
                } else {
                    let response = client
                        .increment(generated::IncrementRequest {
                            delta: query.get("delta").map(|value| value.parse()).transpose()?,
                            operation_id: query.get("id").cloned().unwrap_or_default(),
                        })
                        .await;
                    match response {
                        Ok(response) => match response.result {
                            Some(generated::increment_response::Result::Count(count)) => {
                                serde_json::json!({"value":count.value.to_string()})
                            }
                            Some(generated::increment_response::Result::OutOfRange(_)) => {
                                serde_json::json!({"error":"out_of_range"})
                            }
                            Some(generated::increment_response::Result::OperationConflict(_)) => {
                                serde_json::json!({"error":"operation_conflict"})
                            }
                            None => anyhow::bail!("missing business result"),
                        },
                        Err(error) => {
                            serde_json::json!({"code":error.code(), "outcome":error.outcome()})
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
        "named RPC failed at {path}: {status}: {body}"
    );
    Ok(serde_json::from_str(&body)?)
}

#[test]
fn generated_named_methods_work_across_rust_v8_and_node_with_restart_receipts() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let contract = Arc::new(rpc::Contract::from_descriptor(
        include_bytes!("../../../tools/rpc/fixtures/counter.binpb"),
        rpc::Limits::default(),
    )?);
    let code = std::fs::read_to_string(root.join("tools/rpc/dist/app.v8.js"))
        .context("build RPC fixtures with npm run build --prefix tools/rpc")?;
    let source = Source::new(code, "rpc/app.ts")
        .events([EventKind::Fetch])
        .cell_rpc("V8Counter", "example.counter.v1.Counter", contract.clone())?;
    let node_source = Source::module().events([EventKind::Fetch]).cell_rpc(
        "NodeCounter",
        "example.counter.v1.Counter",
        contract.clone(),
    )?;
    let options = Options {
        turn_timeout: Duration::from_secs(2),
        max_pending_operations: 1,
        ..Default::default()
    };
    JavaScriptRuntime::run(options.clone(), source, |v8| {
        joined_node::run(
            options,
            NodeOptions::new(root.join("tools/rpc/dist/node/node.mjs").canonicalize()?),
            node_source,
            |node| {
                tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build()?.block_on(async move {
                let directory = tempfile::tempdir()?;
                let client = reqwest::Client::new();
                let large = 9_007_199_254_740_993_i64;
                for round in 0..2 {
                    let mut catalog = rpc::Catalog::new();
                    for class in ["RustCounter", "V8Counter", "NodeCounter"] {
                        catalog.register(class, "example.counter.v1.Counter", contract.clone())?;
                    }
                    let rust = Registry::new(|_, _| Box::pin(async {Ok(Output::Done)}))
                        .cell("RustCounter", |_, _| Ok(Arc::new(generated::CounterRegistration::register(rpc::Router::new(), Arc::new(counter::Counter))?)))?
                        .rpc_service("RustCounter", "example.counter.v1.Counter", contract.clone())?;
                    let app = Arc::new(Mixed {rust:Arc::new(rust), v8:v8.clone(), node:node.clone(), catalog, contract:contract.clone()});
                    let (host, base) = native::start_with(directory.path(), app,
                        vec!["RustCounter".into(), "V8Counter".into(), "NodeCounter".into()],
                        vec![("RUST".into(), "RustCounter".into()), ("V8".into(), "V8Counter".into()), ("NODE".into(), "NodeCounter".into())]).await?;
                    // Returned request failures must join the native owner
                    // before either JavaScript runtime is torn down.
                    let result = async {
                    for target in ["rust", "v8", "node"] {
                        let before = if target == "rust" {None} else {Some(request(&client, &base, &format!("/{target}/constructors")).await?)};
                        assert_eq!(request(&client, &base, &format!("/rust/preflight?target={target}")).await?, serde_json::json!({"refused":true}));
                        if let Some(before) = before { assert_eq!(request(&client, &base, &format!("/{target}/constructors")).await?, before); }
                        let current = if round == 0 {0} else {large * 3 + 2 + i64::from(target != "rust")};
                        assert_eq!(request(&client, &base, &format!("/rust/read?target={target}")).await?, serde_json::json!({"value":current.to_string()}));
                        for (index, caller) in ["rust", "v8", "node"].into_iter().enumerate() {
                            let saved = (index as i64 + 1) * large;
                            let path = format!("/{caller}/increment?target={target}&delta={large}&id={caller}-{target}");
                            for _ in 0..2 { assert_eq!(request(&client, &base, &path).await?, serde_json::json!({"value":saved.to_string()}), "round={round}, {caller} -> {target}"); }
                            assert_eq!(request(&client, &base, &format!("/{caller}/increment?target={target}&delta=1&id={caller}-{target}")).await?, serde_json::json!({"error":"operation_conflict"}));
                        }
                        // A leading BOM is part of a Protobuf string, not a
                        // signature to strip. These are distinct receipt IDs.
                        for (caller, id, saved) in [
                            ("rust", "%EF%BB%BFunicode%F0%9F%99%82", large * 3 + 1),
                            ("node", "unicode%F0%9F%99%82", large * 3 + 2),
                            ("v8", "%EF%BB%BFunicode%F0%9F%99%82", large * 3 + 1),
                        ] {
                            assert_eq!(request(&client, &base, &format!("/{caller}/increment?target={target}&delta=1&id={id}")).await?, serde_json::json!({"value":saved.to_string()}));
                        }
                        assert_eq!(request(&client, &base, &format!("/node/increment?target={target}&delta={}&id=overflow", i64::MAX)).await?, serde_json::json!({"error":"out_of_range"}));
                        if target != "rust" {
                            let result = request(&client, &base, &format!("/rust/increment?target={target}&delta=1&id=invalid-reply")).await?;
                            if round == 0 { assert_eq!(result, serde_json::json!({"code":"InvalidResponse", "outcome":"unknown"})); }
                            else { assert_eq!(result, serde_json::json!({"value":(large * 3 + 3).to_string()})); }
                            request(&client, &base, &format!("/{target}/capture")).await?;
                            assert_eq!(request(&client, &base, &format!("/{target}/expired")).await?, serde_json::json!({"code":"ContextExpired", "outcome":"not_committed"}));
                        }
                    }
                    for (index, caller) in ["v8", "node"].into_iter().enumerate() {
                        assert_eq!(request(&client, &base, &format!("/{caller}/budget-capacity")).await?, serde_json::json!({"bounded":true,"slot_reused":true}));
                        assert_eq!(request(&client, &base, &format!("/rust/budget-parent?target={caller}")).await?["parent_capped"], true);
                        let (held_client, held_base, held_path) = (client.clone(), base.clone(), format!("/{caller}/budget-hold"));
                        let held = tokio::spawn(async move { request(&held_client, &held_base, &held_path).await });
                        for attempt in 0..100 {
                            if request(&client, &base, &format!("/{caller}/budget-held")).await?["held"] == true { break; }
                            ensure!(attempt < 99, "budget holder did not become ready");
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                        assert_eq!(request(&client, &base, &format!("/{caller}/budget-foreign")).await?["scope_refused"], true);
                        request(&client, &base, &format!("/{caller}/budget-free")).await?;
                        assert_eq!(held.await??["joined"], true);
                        for target in ["rust", "v8", "node"] {
                            for phase in ["verify", "encode", "decode"] {
                                let result = request(&client, &base, &format!("/{caller}/codec-budget?target={target}&phase={phase}&id=codec-budget-{caller}")).await?;
                                assert_eq!(result["code"], "DeadlineExceeded", "{caller}->{target}/{phase}");
                                assert_eq!(result["outcome"], if phase == "decode" {"unknown"} else {"not_committed"});
                                assert_eq!(result["codec_entered"], phase);
                                let expected = if phase != "decode" {0} else if round == 0 {index + 1} else {2};
                                assert_eq!(result["count"], expected.to_string(), "{caller}->{target}/{phase}, round={round}");
                                if phase == "decode" { assert_eq!(result["replay"], (index + 1).to_string()); }
                            }
                        }
                    }
                    Ok::<_, anyhow::Error>(())
                    }.await;
                    let shutdown = host.shutdown(ShutdownMode::Preserve).await;
                    result.with_context(|| format!("named RPC round={round}, shutdown={shutdown:?}"))?;
                    let NodeOutcome::Stopped(report) = shutdown? else {anyhow::bail!("native RPC host failed")};
                    ensure!(report.ingress_drained && report.application_work_drained && report.cells_drained && !report.drain_timed_out);
                }
                Ok(())
            })
            },
        )
    })
}
