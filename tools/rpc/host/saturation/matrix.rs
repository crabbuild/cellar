// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Context as _, Result};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use super::{gate::Gate, request, Limit, OWNER, SOURCE};
use crate::{application::Composite, fleet};

async fn drained(gate: &Gate) -> Result<()> {
    let services = gate.services()?;
    tokio::time::timeout(Duration::from_secs(3), async {
        while services.outbound_rpc_usage() != Default::default() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("source RPC reservations did not retire")
}

pub(super) async fn cases(
    app: &Arc<Composite>,
    client: &reqwest::Client,
    base: &str,
    gate: &Arc<Gate>,
    remote: bool,
    limit: Limit,
) -> Result<Value> {
    let services = gate.services()?;
    let policy = services.runtime_policy();
    ensure!(
        policy.max_outbound_rpc_requests == limit.policy().max_outbound_rpc_requests
            && policy.max_outbound_rpc_bytes == limit.policy().max_outbound_rpc_bytes,
        "source policy was not installed"
    );
    let owner = if remote { OWNER } else { SOURCE };
    let mut cases = Vec::new();
    let mut counts = BTreeMap::new();
    for (provider, _, class) in fleet::CLASSES {
        let identity = app
            .audit
            .since(0)
            .into_iter()
            .find(|v| v.class.as_deref() == Some(class))
            .context("seed identity missing")?;
        ensure!(identity.node == owner, "seed ran at wrong owner");
        for (caller, _, _) in fleet::CLASSES {
            drained(gate).await?;
            let before = *counts.get(provider).unwrap_or(&0_i64);
            let before_text = before.to_string();
            let id = format!("saturation-{}-{remote}-{caller}-{provider}", limit.name());
            let held_id = format!("held-{id}");
            let (entered, release) = gate.arm(class, &held_id)?;
            let cursor = app.audit.cursor();
            let returned = app.audit.returned_cursor();
            let factories = app.audit.factories().len();
            let (held_client, held_base, target) = (client.clone(), base.to_owned(), provider);
            let mut pending = tokio::spawn(async move {
                request(
                    &held_client,
                    &held_base,
                    &format!("/rust/increment?target={target}&delta=1&id={held_id}"),
                )
                .await
            });
            let checks = async {
                let evidence = tokio::time::timeout(Duration::from_secs(5), entered).await??;
                ensure!(
                    evidence["node"] == owner
                        && evidence["class"] == class
                        && evidence["scope"].as_str() == identity.scope.as_deref()
                        && evidence["epoch"].as_u64() == identity.epoch
                        && evidence["count"].as_str() == Some(before_text.as_str())
                        && evidence["transaction_open"] == false,
                    "held request entered wrong state/domain: {evidence}"
                );
                let held = services.outbound_rpc_usage();
                ensure!(
                    held.active_requests == 1
                        && held.reserved_request_bytes > 64 * 1024
                        && held.reserved_request_bytes <= policy.max_outbound_rpc_bytes,
                    "source did not retain its exact SDK submission: {held:?}"
                );
                let refused = request(
                    client,
                    base,
                    &format!("/{caller}/increment?target={provider}&delta=1&id={id}"),
                )
                .await?;
                ensure!(
                    refused == json!({"code":"ResourceExhausted","outcome":"not_committed"}),
                    "{} {caller}->{provider} refusal changed: {refused}",
                    limit.name()
                );
                ensure!(
                    services.outbound_rpc_usage() == held,
                    "refusal leaked or released the held reservation"
                );
                ensure!(
                    app.audit.since(cursor).iter().all(|v| v.class.is_none())
                        && app.audit.returned_since(returned).is_empty()
                        && app.audit.factories().len() == factories,
                    "saturated submission entered a factory or business handler"
                );
                Ok::<_, anyhow::Error>((evidence, held, refused))
            }
            .await;
            // Always release and join the owned request, including assertion failure.
            let _ = release.send(());
            let completion = tokio::time::timeout(Duration::from_secs(20), &mut pending).await;
            let completion = match completion {
                Ok(result) => result.context("held ingress task failed"),
                Err(error) => {
                    pending.abort();
                    let _ = pending.await;
                    Err(error.into())
                }
            };
            let (evidence, held, refused) = checks?;
            let first = completion??;
            ensure!(
                first == json!({"value":(before+1).to_string()}),
                "held business result changed: {first}"
            );
            drained(gate).await?;
            ensure!(
                request(client, base, &format!("/{caller}/read?target={provider}")).await?
                    == json!({"value":(before+1).to_string()}),
                "saturated request mutated state"
            );
            ensure!(
                app.audit
                    .since(cursor)
                    .iter()
                    .filter(|v| v.method.ends_with("/Increment"))
                    .count()
                    == 1,
                "saturated request detached an automatic replay"
            );
            let path = format!("/{caller}/increment?target={provider}&delta=1&id={id}");
            let expected = json!({"value":(before+2).to_string()});
            for _ in 0..2 {
                drained(gate).await?;
                ensure!(
                    request(client, base, &path).await? == expected,
                    "explicit receipt retry changed"
                );
            }
            drained(gate).await?;
            ensure!(
                request(
                    client,
                    base,
                    &format!("/{caller}/increment?target={provider}&delta=2&id={id}")
                )
                .await?
                    == json!({"error":"operation_conflict"}),
                "changed command was accepted"
            );
            drained(gate).await?;
            ensure!(
                request(client, base, &format!("/{caller}/read?target={provider}")).await?
                    == expected,
                "explicit receipt retry duplicated a mutation"
            );
            drained(gate).await?;
            let visits = app.audit.since(cursor);
            for visit in visits.iter().filter(|v| v.class.is_some()) {
                ensure!(
                    visit.node == owner
                        && visit.class.as_deref() == Some(class)
                        && visit.scope == identity.scope
                        && visit.epoch == identity.epoch,
                    "saturation changed route/authority: {visit:?}"
                );
            }
            let handlers = visits
                .iter()
                .filter(|v| v.method.ends_with("/Increment"))
                .count();
            ensure!(
                handlers == 4 && app.audit.factories().len() == factories,
                "replay or extra factory observed"
            );
            let results = app.audit.returned_since(returned);
            let first_result = results.first().context("held handler result missing")?;
            ensure!(
                first_result.count_matching_storage.as_deref()
                    == Some((before + 1).to_string().as_str())
                    && first_result.transaction_open == Some(false)
                    && first_result.write_position.is_some(),
                "held result did not match its closed native transaction"
            );
            counts.insert(provider, before + 2);
            cases.push(json!({"route":if remote {"signed-peer"} else {"local"},"limit":limit.name(),
                "caller":caller,"provider":provider,"class":class,"source":SOURCE,"owner":owner,
                "scope":identity.scope,"epoch":identity.epoch,"held":evidence,"usage_while_held":held,
                "max_outbound_rpc_requests":policy.max_outbound_rpc_requests,
                "max_outbound_rpc_bytes":policy.max_outbound_rpc_bytes,
                "first_refusal":refused,"refused_handler_count":0,"factory_count_change":0,
                "counter_before":before.to_string(),"counter_after":(before+2).to_string(),
                "total_handler_count":handlers,"automatic_replay":false,"explicit_receipt_retries":2,
                "changed_command_refused":true,"usage_after_case":services.outbound_rpc_usage()}));
        }
    }
    Ok(json!(cases))
}
