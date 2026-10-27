// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Guest control refusals precede routing; fresh generated calls must progress.
use anyhow::{ensure, Context as _, Result};
use cellar::{bucket::Bucket, sdk::*};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{application::Composite, fleet};

const OWNER: &str = "guest-control-owner";
const SOURCE: &str = "guest-control-source";

type Services = Arc<Mutex<BTreeMap<String, Arc<cellar::host_services::HostServices>>>>;

struct Observed {
    inner: Arc<Composite>,
    services: Services,
}

impl Observed {
    fn capture(&self, context: &Context) -> Result<()> {
        let node = context
            .var("DOMAIN_MARKER")
            .context("missing control node marker")?;
        self.services
            .lock()
            .unwrap()
            .entry(node.into())
            .or_insert_with(cellar::asyncrt::services);
        Ok(())
    }
}

impl Application for Observed {
    fn rpc_catalog(&self) -> Option<&rpc::Catalog> {
        self.inner.rpc_catalog()
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.inner.supports_cell(class)
    }

    fn validate_configuration(
        &self,
        options: &cellar::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        self.inner.validate_configuration(options)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        self.capture(&context)?;
        self.inner.create_cell(context, identity)
    }

    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        if let Err(error) = self.capture(&context) {
            return Box::pin(async move { Err(error) });
        }
        self.inner.clone().invoke(context, invocation)
    }
}

async fn request(client: &reqwest::Client, base: &str, path: &str) -> Result<Value> {
    let response = client.get(format!("{base}{path}")).send().await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(
        status.is_success(),
        "control request {path}: {status} {body}"
    );
    Ok(serde_json::from_str(&body)?)
}

async fn cases(
    app: &Composite,
    client: &reqwest::Client,
    base: &str,
    remote: bool,
) -> Result<Vec<Value>> {
    let mut cases = Vec::new();
    let source = if remote { SOURCE } else { OWNER };
    for caller in ["python", "v8", "node"] {
        for (provider, _, class) in fleet::CLASSES {
            for mode in ["unknown", "oversize"] {
                let cursor = app.audit.cursor();
                let factories = app.audit.factories().len();
                let refused = request(
                    client,
                    base,
                    &format!("/{caller}/metadata-control?target={provider}&mode={mode}"),
                )
                .await?;
                let visits = app.audit.since(cursor);
                ensure!(refused == json!({"code":"InvalidArgument","outcome":"not_committed"}), "guest control {caller}->{provider}/{mode} was admitted: {refused}; visits={visits:?}");
                ensure!(
                    visits.len() == 1
                        && visits[0].node == source
                        && visits[0].class.is_none()
                        && visits[0].method == format!("ingress:{caller}"),
                    "invalid control entered business dispatch"
                );
                ensure!(
                    app.audit.factories().len() == factories,
                    "invalid control ran a provider factory"
                );
                let progress = app.audit.cursor();
                ensure!(
                    request(client, base, &format!("/{caller}/read?target={provider}")).await?
                        == json!({"value":"0"}),
                    "control refusal blocked a fresh generated call"
                );
                let fresh = app.audit.since(progress);
                ensure!(
                    fresh.len() == 2 && fresh[0].node == source && fresh[0].class.is_none(),
                    "fresh control progress changed ingress"
                );
                let owner = &fresh[1];
                ensure!(
                    owner.node == OWNER
                        && owner.class.as_deref() == Some(class)
                        && owner.method == "example.counter.v1.Counter/GetCount"
                        && owner.epoch.is_some_and(|epoch| epoch > 0)
                        && owner.scope.is_some(),
                    "fresh generated call missed the real owner"
                );
                cases.push(json!({"route":if remote {"signed-peer"} else {"local"},"caller":caller,"provider":provider,"mode":mode,"refusal":refused,"provider_calls_on_refusal":0,"factories_on_refusal":0,"source":source,"owner":owner.node,"class":owner.class,"scope":owner.scope,"epoch":owner.epoch,"fresh_generated_read":"0"}));
            }
        }
    }
    Ok(cases)
}

pub async fn run(app: Arc<Composite>) -> Result<Value> {
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(20))
        .build()?;
    let services = Services::default();
    let application = Arc::new(Observed {
        inner: app.clone(),
        services: services.clone(),
    });
    let mut nodes = Vec::new();
    let result = async {
        let (owner, owner_url) = fleet::start(
            &directory.path().join("owner"),
            application.clone(),
            OWNER,
            bucket.clone(),
        )
        .await?;
        nodes.push(owner);
        // Seed each normal cell while this is the only owner. Refusals must not
        // create the oversized target; fresh calls must reach the seeded owner.
        for (provider, _, _) in fleet::CLASSES {
            ensure!(
                request(
                    &client,
                    &owner_url,
                    &format!("/rust/read?target={provider}")
                )
                .await?
                    == json!({"value":"0"}),
                "control seed changed state"
            );
        }
        let mut cases = cases(&app, &client, &owner_url, false).await?;
        let (source, source_url) = fleet::start(
            &directory.path().join("source"),
            application,
            SOURCE,
            bucket,
        )
        .await?;
        nodes.push(source);
        cases.extend(self::cases(&app, &client, &source_url, true).await?);
        ensure!(cases.len() == 48, "incomplete guest-control matrix");
        Ok::<_, anyhow::Error>(cases)
    }
    .await;
    let mut cleanup = Vec::new();
    for node in nodes.into_iter().rev() {
        if let Err(error) = fleet::stop(node, ShutdownMode::Preserve).await {
            cleanup.push(format!("{error:#}"));
        }
    }
    let retained = services.lock().unwrap().values().any(|services| {
        services.outbound_rpc_usage() != Default::default()
            || services.inbound_rpc_usage() != Default::default()
    });
    let cases = result.with_context(|| {
        format!("control cleanup={cleanup:?}, retained_native_admission={retained}")
    })?;
    ensure!(
        cleanup.is_empty() && !retained,
        "control cleanup failed: {cleanup:?}; retained={retained}"
    );
    Ok(
        json!({"format":1,"case_count":cases.len(),"cases":cases,"native_nodes_drained":true,"native_reservations_after_join":0,"all_memory_qualified":false,"release_qualified":false}),
    )
}
