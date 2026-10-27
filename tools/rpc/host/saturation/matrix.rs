// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Context as _, Result};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use super::{gate::Gate, request, Limit, Side, OWNER, SOURCE};
use crate::{application::Composite, fleet};

async fn drained(gate: &Gate) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(3), async {
        while gate.observed_services().iter().any(|services| {
            let receiver = services.inbound_rpc_usage();
            services.outbound_rpc_usage() != Default::default()
                || receiver.active_requests != 0
                || receiver.reserved_request_bytes != 0
        }) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("source or receiver RPC reservations did not retire")
}

pub(super) async fn cases(
    app: &Arc<Composite>,
    client: &reqwest::Client,
    base: &str,
    gate: &Arc<Gate>,
    remote: bool,
    limit: Limit,
    side: Side,
) -> Result<Value> {
    let services = gate.services()?;
    let owner_services = gate.services_for(if remote { OWNER } else { SOURCE })?;
    let policy = if side == Side::Source {
        services.runtime_policy()
    } else {
        owner_services.runtime_policy()
    };
    let (max_requests, max_bytes) = if side == Side::Source {
        (
            policy.max_outbound_rpc_requests,
            policy.max_outbound_rpc_bytes,
        )
    } else {
        (
            policy.max_inbound_rpc_requests,
            policy.max_inbound_rpc_bytes,
        )
    };
    ensure!(
        max_requests == limit.policy().max_outbound_rpc_requests
            && max_bytes == limit.policy().max_outbound_rpc_bytes,
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
            let id = format!(
                "saturation-{}-{}-{remote}-{caller}-{provider}",
                side.name(),
                limit.name()
            );
            let held_id = if side == Side::ReceiverReply {
                format!("held-reply-quota-{id}")
            } else {
                format!("held-{id}")
            };
            let held_retry_id = held_id.clone();
            let holder_caller = if side == Side::ReceiverReply {
                caller
            } else {
                "rust"
            };
            let (entered, release) = gate.arm(class, &held_id)?;
            let cursor = app.audit.cursor();
            let returned = app.audit.returned_cursor();
            let factories = app.audit.factories().len();
            let (held_client, held_base, target) = (client.clone(), base.to_owned(), provider);
            let mut pending = tokio::spawn(async move {
                request(
                    &held_client,
                    &held_base,
                    &format!("/{holder_caller}/increment?target={target}&delta=1&id={held_id}"),
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
                let held = side.usage(&services, &owner_services);
                ensure!(
                    held.active_requests == 1
                        && held.reserved_request_bytes > 64 * 1024
                        && held.reserved_request_bytes <= max_bytes,
                    "source did not retain its exact SDK submission: {held:?}"
                );
                let refused = request(
                    client,
                    base,
                    &format!("/{caller}/increment?target={provider}&delta=1&id={id}"),
                )
                .await?;
                ensure!(
                    refused == json!({"code":"ResourceExhausted","outcome": if side == Side::Source {"not_committed"} else {"unknown"}}),
                    "{} {caller}->{provider} refusal changed: {refused}",
                    limit.name()
                );
                ensure!(
                    side.usage(&services, &owner_services) == held,
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
            let holder_expected = if side == Side::ReceiverReply {
                json!({"code":"ResourceExhausted","outcome":"unknown"})
            } else {
                json!({"value":(before+1).to_string()})
            };
            ensure!(
                first == holder_expected,
                "{} {caller}->{provider} held business result changed: {first}",
                side.name()
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
            if side == Side::ReceiverReply {
                let replay = request(
                    client,
                    base,
                    &format!("/{caller}/increment?target={provider}&delta=1&id={held_retry_id}"),
                )
                .await?;
                ensure!(
                    replay == json!({"value":(before+1).to_string()}),
                    "explicit post-commit reply retry changed the saved receipt: {replay}"
                );
            }
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
            let expected_handlers = if side == Side::ReceiverReply { 5 } else { 4 };
            ensure!(
                handlers == expected_handlers && app.audit.factories().len() == factories,
                "replay or extra factory observed"
            );
            let results = app.audit.returned_since(returned);
            let first_result = results.first().context("held handler result missing")?;
            ensure!(
                first_result.stored_count.as_deref() == Some((before + 1).to_string().as_str())
                    && first_result.transaction_open == Some(false)
                    && first_result.write_position.is_some(),
                "held result did not preserve its closed native transaction"
            );
            if side == Side::ReceiverReply {
                ensure!(
                    first_result
                        .valid_reply_message_bytes
                        .is_some_and(|length| length > 64 * 1024),
                    "post-commit refusal did not observe a valid large generated reply"
                );
            } else {
                ensure!(
                    first_result.count_matching_storage == first_result.stored_count,
                    "held result did not match the stored count"
                );
            }
            counts.insert(provider, before + 2);
            cases.push(json!({"side":side.name(),"route":if remote {"signed-peer"} else {"local"},"limit":limit.name(),
                "caller":caller,"provider":provider,"class":class,"source":SOURCE,"owner":owner,
                "scope":identity.scope,"epoch":identity.epoch,"held":evidence,"usage_while_held":held,
                "max_rpc_requests":max_requests,
                "max_rpc_bytes":max_bytes,
                "first_refusal":refused,"refused_handler_count":0,"factory_count_change":0,
                "counter_before":before.to_string(),"counter_after":(before+2).to_string(),
                "holder_caller":holder_caller,"holder_first_reply":first,
                "holder_stored_count":first_result.stored_count,
                "holder_valid_message_bytes":first_result.valid_reply_message_bytes,
                "holder_transaction_open":first_result.transaction_open,
                "holder_write_position":first_result.write_position,
                "holder_receipt_replayed":side == Side::ReceiverReply,
                "total_handler_count":handlers,"automatic_replay":false,"explicit_receipt_retries":if side == Side::ReceiverReply {3} else {2},
                "changed_command_refused":true,"usage_after_case":side.usage(&services, &owner_services)}));
        }
    }
    Ok(json!(cases))
}
