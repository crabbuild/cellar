// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Provider catalog limits must govern guest response codecs, before a loose
//! caller can accept a reply which violates the actual owner's declaration.
use anyhow::{ensure, Context as _, Result};
use cellar::{bucket::Bucket, sdk::*};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use crate::{application::Composite, fleet};

const OWNER: &str = "provider-limit-owner";
const SOURCE: &str = "provider-limit-source";
const SLOTS: usize = 7;

struct Declared {
    inner: Arc<Composite>,
    catalog: rpc::Catalog,
}

impl Declared {
    fn new(inner: Arc<Composite>) -> Result<Self> {
        let contract = Arc::new(rpc::Contract::from_descriptor(
            inner.contract.descriptor(),
            rpc::Limits {
                max_decoded_slots: SLOTS,
                ..Default::default()
            },
        )?);
        let mut catalog = rpc::Catalog::new();
        for (_, _, class) in fleet::CLASSES {
            catalog.register(class, "example.counter.v1.Counter", contract.clone())?;
        }
        Ok(Self { inner, catalog })
    }
}

impl Application for Declared {
    fn rpc_catalog(&self) -> Option<&rpc::Catalog> {
        Some(&self.catalog)
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
        self.inner.create_cell(context, identity)
    }

    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        self.inner.clone().invoke(context, invocation)
    }
}

async fn request(client: &reqwest::Client, base: &str, path: &str) -> Result<Value> {
    let response = client.get(format!("{base}{path}")).send().await?;
    let status = response.status();
    let text = response.text().await?;
    ensure!(
        status.is_success(),
        "provider limits ingress {path}: {status}: {text}"
    );
    serde_json::from_str(&text).context("provider limits returned invalid JSON")
}

async fn matrix(
    app: &Composite,
    client: &reqwest::Client,
    base: &str,
    remote: bool,
    counts: &mut BTreeMap<&'static str, i64>,
) -> Result<Vec<Value>> {
    let mut cases = Vec::new();
    for (provider, _, class) in fleet::CLASSES {
        let identity = app
            .audit
            .since(0)
            .into_iter()
            .find(|visit| visit.class.as_deref() == Some(class))
            .context("provider limit owner was not seeded")?;
        ensure!(identity.node == OWNER, "seed used a different owner");
        for (caller, _, _) in fleet::CLASSES {
            let before = *counts.get(provider).unwrap_or(&0);
            let after = before + 1;
            let id = format!("provider-slots-{remote}-{caller}-{provider}");
            let path = format!("/{caller}/increment?target={provider}&delta=1&id={id}");
            let cursor = app.audit.cursor();
            let returned = app.audit.returned_cursor();
            let factories = app.audit.factories().len();
            let first = request(client, base, &path).await?;
            let observation = app.audit.returned_since(returned);
            ensure!(first == json!({"code":"InvalidResponse","outcome":"unknown"}),
                "declared provider slots bypassed by {caller}->{provider}, peer={remote}: {first}; native returns={observation:?}");
            let count = request(client, base, &format!("/{caller}/read?target={provider}")).await?;
            ensure!(
                count == json!({"value":after.to_string()}),
                "response refusal lost committed count: {count}"
            );
            ensure!(
                app.audit
                    .since(cursor)
                    .iter()
                    .filter(|v| v.method.ends_with("/Increment"))
                    .count()
                    == 1,
                "invalid response detached an automatic replay"
            );
            for _ in 0..2 {
                ensure!(
                    request(client, base, &path).await? == first,
                    "explicit receipt retry changed response policy"
                );
            }
            let conflict = request(
                client,
                base,
                &format!("/{caller}/increment?target={provider}&delta=2&id={id}"),
            )
            .await?;
            ensure!(
                conflict == json!({"error":"operation_conflict"}),
                "response refusal lost its atomic receipt: {conflict}"
            );
            ensure!(
                request(client, base, &format!("/{caller}/read?target={provider}")).await? == count,
                "receipt retries duplicated a mutation"
            );
            let visits = app.audit.since(cursor);
            let mut handlers = 0;
            for visit in &visits {
                if visit.class.is_some() {
                    ensure!(
                        visit.node == OWNER
                            && visit.class.as_deref() == Some(class)
                            && visit.scope == identity.scope
                            && visit.epoch == identity.epoch,
                        "provider limits changed native authority: {visit:?}"
                    );
                    handlers += usize::from(visit.method.ends_with("/Increment"));
                } else {
                    ensure!(
                        visit.node == if remote { SOURCE } else { OWNER },
                        "wrong source authority"
                    );
                }
            }
            ensure!(
                handlers == 4 && app.audit.factories().len() == factories,
                "unexpected provider replay or construction"
            );
            let returned = app.audit.returned_since(returned);
            ensure!(
                returned.first().is_some_and(
                    |r| r.transaction_open == Some(false) && r.write_position.is_some()
                ),
                "response refusal did not follow a closed native transaction"
            );
            counts.insert(provider, after);
            cases.push(json!({"route":if remote {"signed-peer"} else {"local"},
                "caller":caller,"provider":provider,"class":class,"owner":OWNER,
                "source":if remote {SOURCE} else {OWNER},"scope":identity.scope,"epoch":identity.epoch,
                "provider_max_decoded_slots":SLOTS,"generated_max_decoded_slots":1_000_000,
                "first_reply":first,"counter_before":before.to_string(),"counter_after":after.to_string(),
                "explicit_retries":2,"conflict":conflict,"handler_count":handlers,
                "factory_count_change":0,"automatic_replay":false,"closed_transaction":true}));
        }
    }
    Ok(cases)
}

pub async fn run(app: Arc<Composite>) -> Result<Value> {
    app.audit.reset_after_node_drain();
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(20))
        .build()?;
    let owner_app = Arc::new(Declared::new(app.clone())?);
    let mut nodes = Vec::new();
    let result = async {
        let (owner, owner_url) = fleet::start(
            &directory.path().join("owner"),
            owner_app,
            OWNER,
            bucket.clone(),
        )
        .await?;
        nodes.push(owner);
        for (provider, _, _) in fleet::CLASSES {
            ensure!(
                request(
                    &client,
                    &owner_url,
                    &format!("/rust/read?target={provider}")
                )
                .await?
                    == json!({"value":"0"}),
                "provider limit seed changed"
            );
        }
        let mut counts = BTreeMap::new();
        let mut cases = matrix(&app, &client, &owner_url, false, &mut counts).await?;
        let (source, source_url) = fleet::start(
            &directory.path().join("source"),
            app.clone(),
            SOURCE,
            bucket,
        )
        .await?;
        nodes.push(source);
        cases.extend(matrix(&app, &client, &source_url, true, &mut counts).await?);
        Ok::<_, anyhow::Error>(cases)
    }
    .await;
    let services: Vec<_> = nodes.iter().map(Node::services).collect();
    let mut cleanup = Vec::new();
    for node in nodes.into_iter().rev() {
        if let Err(error) = fleet::stop(node, ShutdownMode::Preserve).await {
            cleanup.push(format!("{error:#}"));
        }
    }
    for services in services {
        ensure!(
            services.outbound_rpc_usage() == Default::default()
                && services.inbound_rpc_usage() == Default::default()
                && services.native_rpc_decode_usage() == Default::default()
                && services.rpc_response_usage() == Default::default(),
            "provider limits retained native reservations after node join"
        );
    }
    let cases = result.with_context(|| format!("provider limits cleanup={cleanup:?}"))?;
    ensure!(
        cleanup.is_empty(),
        "provider limits did not join: {cleanup:?}"
    );
    ensure!(cases.len() == 32, "provider limits matrix incomplete");
    Ok(json!({"format":1,"case_count":cases.len(),"cases":cases,
        "native_nodes_joined":true,"native_reservations_after_join":0,
        "all_memory_qualified":false,"release_qualified":false}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::*;
    use rpc::prost::Message;

    #[test]
    fn slot_probe_admits_requests_reads_and_conflicts_but_refuses_count_results() -> Result<()> {
        let contract = rpc::Contract::from_descriptor(
            include_bytes!("../fixtures/counter.binpb"),
            rpc::Limits {
                max_decoded_slots: SLOTS,
                ..Default::default()
            },
        )?;
        for (name, bytes) in [
            (
                "IncrementRequest",
                IncrementRequest {
                    delta: Some(1),
                    operation_id: "slots".into(),
                }
                .encode_to_vec(),
            ),
            (
                "GetCountResponse",
                GetCountResponse {
                    count: Some(Count { value: 1 }),
                }
                .encode_to_vec(),
            ),
            (
                "IncrementResponse",
                IncrementResponse {
                    result: Some(increment_response::Result::OperationConflict(
                        OperationConflict {},
                    )),
                }
                .encode_to_vec(),
            ),
        ] {
            contract.validate_message(&format!("example.counter.v1.{name}"), &bytes)?;
        }
        let bytes = IncrementResponse {
            result: Some(increment_response::Result::Count(Count { value: 1 })),
        }
        .encode_to_vec();
        ensure!(
            contract
                .validate_message("example.counter.v1.IncrementResponse", &bytes)
                .unwrap_err()
                .code()
                == rpc::Code::ResourceExhausted
        );
        Ok(())
    }
}
