//! Hold original signed settlement requests before owner submission, then change
//! the durable subscription. No synthetic authorization or proof is supplied.

use super::*;
use serde_json::{json, Value};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub(super) async fn lease_call(context: Context, url: &str) -> Result<Output> {
    let url = url::Url::parse(url)?;
    let fields = url
        .query_pairs()
        .collect::<std::collections::BTreeMap<_, _>>();
    let receipt = Receipt::decode(fields.get("receipt").context("receipt missing")?)?;
    let events = context.stream::<Changed>("EVENTS")?;
    let subscription = events.subscription("projection")?;
    let result: Result<Value> = match fields.get("kind").map(|value| value.as_ref()) {
        Some("ack") => subscription
            .ack(&context, &receipt)
            .await
            .map(|status| json!({"status":format!("{status:?}")})),
        Some("extend") => subscription
            .extend_until(
                &context,
                &receipt,
                fields.get("target").context("target missing")?.parse()?,
            )
            .await
            .map(|expires| json!({"expires_ms":expires.to_string()})),
        _ => anyhow::bail!("unknown settlement action"),
    };
    let value = match result {
        Ok(value) => value,
        Err(error) => json!({"error":error.downcast_ref::<DeliveryError>()
            .context("settlement refusal was untyped")?.code.as_str()}),
    };
    Ok(super::super::response(serde_json::to_string(&value)?))
}

pub(super) async fn status(client: &reqwest::Client, node: &Node) -> Result<Value> {
    Ok(stream_operator(
        client,
        node,
        &binding(true),
        json!({"kind":"subscription","name":"projection"}),
        "settlement-race",
    )
    .await?["result"]
        .clone())
}

pub(super) async fn fetch(
    client: &reqwest::Client,
    node: &Node,
    id: &str,
    worker: &str,
) -> Result<Value> {
    Ok(serde_json::from_str(
        &call(
            client,
            node,
            reqwest::Method::POST,
            &format!("/fetch-save?source={worker}&id={id}"),
        )
        .await?,
    )?)
}

pub(super) async fn settle(
    client: &reqwest::Client,
    node: &Node,
    worker: &str,
    kind: &str,
    receipt: &Receipt,
    target: i64,
) -> Result<Value> {
    Ok(serde_json::from_str(
        &call(
            client,
            node,
            reqwest::Method::POST,
            &format!(
                "/lease-call?source={worker}&kind={kind}&receipt={}&target={target}",
                receipt.encode()?
            ),
        )
        .await?,
    )?)
}

fn number(value: &Value) -> Result<i64> {
    Ok(value.as_str().context("decimal string missing")?.parse()?)
}

fn wall_ms() -> Result<i64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}

#[derive(Clone, Copy, Debug)]
enum Transition {
    Reset,
    Expire,
    Acknowledge,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settlement_race_reset_invalidates_pending_ack_and_extension() -> Result<()> {
    race(Transition::Reset).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settlement_race_expiry_replacement_refuses_old_ack_and_extension() -> Result<()> {
    race(Transition::Expire).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settlement_race_ack_replay_and_extension_preserve_next_lease() -> Result<()> {
    race(Transition::Acknowledge).await
}

async fn race(transition: Transition) -> Result<()> {
    const FIRST: &str = "000000000000000000000000000000c1";
    const COMPETING: &str = "000000000000000000000000000000c2";
    const NEXT: &str = "000000000000000000000000000000c3";
    let directory = tempfile::tempdir()?;
    let destination_core = Executor::new("settlement-destination-core")?;
    let consumer_core = Executor::new("settlement-consumer-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (mut config, services) = inputs(
        "settlement-destination",
        &directory.path().join("destination"),
        &destination_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let proxy = peer_reply_fault::Proxy::start(services.internal_listener.local_addr()?).await?;
    config.identity.advertise = proxy.address.to_string();
    let destination = Node::start(
        config,
        services,
        graph(1, false),
        applications("destination")?,
    )
    .await?;
    super::super::ready(&destination).await?;
    let (config, services) = inputs(
        "settlement-consumer",
        &directory.path().join("consumer"),
        &consumer_core,
        Some(bucket),
        0,
    )
    .await?;
    let budget = Duration::from_millis(
        config
            .actor
            .core
            .operation_deadline_ms
            .context("operation budget missing")?,
    );
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let consumer = Node::start(config, services, external, applications("consumer")?).await?;
    super::super::ready(&consumer).await?;
    let client = client()?;
    // Place separate acknowledgment/extension callers on the consumer node so
    // both signed requests must cross the real destination peer proxy.
    for worker in ["projection", "lease-ack", "lease-extend", "competitor"] {
        ensure!(
            call(
                &client,
                &consumer,
                reqwest::Method::GET,
                &format!("/?source={worker}")
            )
            .await?
                == "consumer:0"
        );
    }
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::POST,
            "/write?source=producer"
        )
        .await?
            == "consumer:1"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while status(&client, &destination).await?["end"] != "2" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("wait for original event")??;
    let original = fetch(&client, &consumer, FIRST, "projection").await?;
    ensure!(original["processed"] == 1);
    let receipt = Receipt::decode(
        original["batch"]["receipt"]
            .as_str()
            .context("original receipt missing")?,
    )?;
    let original_state = status(&client, &destination).await?;
    ensure!(
        original_state["checkpoint"] == "0"
            && original_state["observation"]["lease_active"] == true
    );
    ensure!(
        fetch(&client, &consumer, COMPETING, "competitor").await? == json!({"error":"LeaseBusy"})
    );
    ensure!(status(&client, &destination).await?["lease"] == original_state["lease"]);
    let target = number(&original["batch"]["issued_ms"])? + 60_000;
    if matches!(transition, Transition::Acknowledge) {
        let extension = number(&original["batch"]["expires_ms"])? + 1_000;
        for _ in 0..2 {
            ensure!(
                settle(
                    &client,
                    &consumer,
                    "projection",
                    "extend",
                    &receipt,
                    extension
                )
                .await?
                    == json!({"expires_ms":extension.to_string()})
            );
        }
        ensure!(
            status(&client, &destination).await?["lease"]["expires_ms"].as_str()
                == Some(extension.to_string().as_str())
        );
    }
    if matches!(transition, Transition::Expire) {
        // Preserve the ordinary 30s lease and 3s operation budget. Wait outside
        // the calls, then hold both original requests across actual expiry.
        let remaining = number(&original["batch"]["expires_ms"])? - wall_ms()? - 1_000;
        if remaining > 0 {
            tokio::time::sleep(Duration::from_millis(remaining as u64)).await;
        }
    }
    proxy
        .capture
        .hold_requests(&["__streamAck", "__streamExtend"]);
    let submitted = Instant::now();
    let mut old_calls = Box::pin(async {
        tokio::join!(
            settle(&client, &consumer, "lease-ack", "ack", &receipt, target),
            settle(
                &client,
                &consumer,
                "lease-extend",
                "extend",
                &receipt,
                target
            ),
        )
    });
    cellar::asyncrt::select! {
        outcome = &mut old_calls => anyhow::bail!("old calls finished before submission barrier: {outcome:?}"),
        entered = tokio::time::timeout(Duration::from_secs(5), proxy.capture.request_entered.acquire_many(2)) =>
            entered.context("capture both original settlement requests")??.forget(),
    }
    let held = proxy.capture.held_requests.lock().unwrap().clone();
    ensure!(held.len() == 2);
    for request in &held {
        ensure!(request.scope == stream_scope()?);
        let body = cellar::application::decode_rpc(&cellar::engine_api::RpcData::Structured(
            request.body.clone().into(),
        ))?
        .to_json()?;
        ensure!(body[0]["receipt"] == original["batch"]["receipt"]);
        if request.method == "__streamExtend" {
            ensure!(body[0]["expires_ms"].as_str() == Some(target.to_string().as_str()));
        }
    }
    let at_cut = status(&client, &destination).await?;
    ensure!(
        at_cut["checkpoint"] == "0"
            && at_cut["identity"] == original_state["identity"]
            && at_cut["observation"]["lease_active"] == true,
        "old lease not live at cut: {at_cut}"
    );
    let reset = json!({"kind":"change","change":{"operation":"000000000000000000000000000000c4",
        "expected":original_state["identity"],"action":{"kind":"reset","cursor":{"resource":resource().id,"partition":0,"next":"1"}},
        "reason":"replay original event while old settlement requests are held"}});
    let mut reset_result = None;
    match transition {
        Transition::Reset => {
            let result = stream_operator(
                &client,
                &destination,
                &binding(true),
                reset.clone(),
                "settlement-race",
            )
            .await?;
            ensure!(
                result["result"]["checkpoint"] == "0"
                    && result["result"]["current"]["generation"]
                        != original_state["identity"]["generation"]
            );
            reset_result = Some(result);
        }
        Transition::Expire => {
            tokio::time::timeout(budget, async {
                while status(&client, &destination).await?["observation"]["lease_active"] == true {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await
            .context("observe original lease expiry")??;
            ensure!(status(&client, &destination).await?["checkpoint"] == "0");
        }
        Transition::Acknowledge => {
            ensure!(
                settle(&client, &consumer, "projection", "ack", &receipt, target).await?
                    == json!({"status":"Acknowledged"})
            );
            let acknowledged = status(&client, &destination).await?;
            ensure!(acknowledged["checkpoint"] == "1" && acknowledged["lease"].is_null());
            ensure!(
                call(
                    &client,
                    &consumer,
                    reqwest::Method::POST,
                    "/write?source=producer"
                )
                .await?
                    == "consumer:2"
            );
            tokio::time::timeout(budget, async {
                while status(&client, &destination).await?["end"] != "3" {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await
            .context("wait for second event")??;
        }
    }
    let next = fetch(&client, &consumer, NEXT, "projection").await?;
    let next_receipt = Receipt::decode(
        next["batch"]["receipt"]
            .as_str()
            .context("replacement receipt missing")?,
    )?;
    let old_identity = serde_json::to_value(&receipt)?;
    let next_identity = serde_json::to_value(&next_receipt)?;
    ensure!(
        number(&next_identity["generation"])? > number(&old_identity["generation"])?
            && next_identity["token"] != old_identity["token"]
    );
    let (checkpoint, processed) = if matches!(transition, Transition::Acknowledge) {
        ("1", 3)
    } else {
        ("0", 1)
    };
    ensure!(next["processed"] == processed);
    if !matches!(transition, Transition::Acknowledge) {
        ensure!(
            next["batch"]["records"] == original["batch"]["records"],
            "redelivery changed accepted bytes"
        );
    }
    let replacement = status(&client, &destination).await?;
    ensure!(
        replacement["checkpoint"] == checkpoint
            && replacement["observation"]["lease_active"] == true
    );
    ensure!(
        fetch(&client, &consumer, COMPETING, "competitor").await? == json!({"error":"LeaseBusy"})
    );
    proxy.capture.request_release.add_permits(2);
    let (old_ack, old_extend) = tokio::time::timeout(budget, &mut old_calls)
        .await
        .context("old settlement callers complete")?;
    drop(old_calls);
    ensure!(
        submitted.elapsed() < budget && !consumer.is_closed() && !destination.is_closed(),
        "timeout/closure cannot qualify settlement race: {:?}",
        submitted.elapsed()
    );
    let (old_ack, old_extend) = (old_ack?, old_extend?);
    let expected_ack = match transition {
        Transition::Reset => json!({"error":"SubscriptionReset"}),
        Transition::Expire => json!({"error":"StaleReceipt"}),
        Transition::Acknowledge => json!({"status":"AlreadyAcknowledged"}),
    };
    let expected_extend = json!({"error":if matches!(transition, Transition::Reset) {"SubscriptionReset"} else {"StaleReceipt"}});
    ensure!(
        old_ack == expected_ack && old_extend == expected_extend,
        "obsolete requests changed outcome: ack={old_ack}, extend={old_extend}"
    );
    let after = status(&client, &destination).await?;
    ensure!(
        after["checkpoint"] == replacement["checkpoint"]
            && after["identity"] == replacement["identity"]
            && after["lease"] == replacement["lease"],
        "old request changed replacement: {after}"
    );
    for request in &held {
        ensure!(
            proxy
                .capture
                .exchanges
                .lock()
                .unwrap()
                .iter()
                .any(|exchange| exchange.method == request.method
                    && exchange.request == request.body),
            "held packet was not forwarded unchanged"
        );
    }
    if let Some(result) = &reset_result {
        ensure!(
            stream_operator(
                &client,
                &destination,
                &binding(true),
                reset.clone(),
                "settlement-race"
            )
            .await?
                == *result
        );
        ensure!(status(&client, &destination).await?["lease"] == replacement["lease"]);
    }
    ensure!(
        settle(
            &client,
            &consumer,
            "projection",
            "ack",
            &next_receipt,
            target
        )
        .await?
            == json!({"status":"Acknowledged"})
    );
    let finished = status(&client, &destination).await?;
    let final_checkpoint = if matches!(transition, Transition::Acknowledge) {
        "2"
    } else {
        "1"
    };
    ensure!(finished["checkpoint"] == final_checkpoint && finished["lease"].is_null());
    if let Some(result) = &reset_result {
        ensure!(
            stream_operator(
                &client,
                &destination,
                &binding(true),
                reset,
                "settlement-race"
            )
            .await?
                == *result
        );
        ensure!(status(&client, &destination).await?["checkpoint"] == final_checkpoint);
    }
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::GET,
            "/processed?source=projection"
        )
        .await?
            == processed.to_string()
    );
    // Every settlement and replacement-lease assertion has passed. The nodes
    // now close their peer sockets during drain; only transport closure is
    // expected beyond this cut, while proxy framing errors still fail join.
    proxy.capture.expect_upstream_close();
    stopped(consumer)
        .await
        .context("complete settlement consumer drain")?;
    stopped(destination)
        .await
        .context("complete settlement destination drain")?;
    proxy.stop().await.context("join settlement peer proxy")?;
    ensure!(
        consumer_core.handle().spawn(async { 7 }).await? == 7
            && destination_core.handle().spawn(async { 7 }).await? == 7
    );
    Ok(())
}
