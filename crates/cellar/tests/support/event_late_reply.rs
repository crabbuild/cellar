//! A proven append reply outlives its source owner. No replay can settle the
//! successor until its original obligation and stream acceptance are audited.

use super::*;
use base64::Engine;
use serde_json::{json, Value};

async fn pending(client: &reqwest::Client, node: &Node, scope: &str) -> Result<Value> {
    Ok(operator(
        client,
        node,
        scope,
        &json!({"op":"inspect"}),
        "late-reply-audit",
    )
    .await?["result"]
        .clone())
}

async fn history(client: &reqwest::Client, node: &Node) -> Result<Value> {
    Ok(stream_operator(
        client,
        node,
        &binding(false),
        json!({"kind":"read","cursor":null,"limit":10,"max_bytes":4096,"payload":true}),
        "late-reply-audit",
    )
    .await?["result"]["records"]
        .clone())
}

async fn subscription(client: &reqwest::Client, node: &Node) -> Result<Value> {
    Ok(stream_operator(
        client,
        node,
        &binding(true),
        json!({"kind":"subscription","name":"projection"}),
        "late-ack-audit",
    )
    .await?["result"]
        .clone())
}

async fn partition_owner(
    domain: &ExecutionDomain,
    bucket: &cellar::bucket::Bucket,
) -> Result<Value> {
    domain
        .run(async {
            let key = format!("cells/{}/own.json", stream_scope()?);
            let (bytes, _) = bucket.get(&key).await?.context("partition owner missing")?;
            Ok::<_, anyhow::Error>(serde_json::from_slice(&bytes)?)
        })
        .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_proven_append_reply_cannot_settle_a_fenced_source_successor() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let destination_core = Executor::new("late-reply-destination-core")?;
    let source_core = Executor::new("late-reply-source-core")?;
    let successor_core = Executor::new("late-reply-successor-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (mut config, services) = inputs(
        "late-destination",
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
    let client = client()?;
    ensure!(history(&client, &destination).await? == json!([]));

    let (mut config, services) = inputs(
        "late-source",
        &directory.path().join("source"),
        &source_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    // Match the existing missing-authority fixture: the readback must report
    // the removed lease, rather than letting the expiry timer win the race.
    config.actor.lease_ttl_ms = 5_000;
    config.startup.recovery_retry_backoff = Duration::from_millis(5_000);
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let source = Node::start(config, services, external, applications("source")?).await?;
    super::super::ready(&source).await?;
    proxy.capture.delay_append();
    ensure!(call(&client, &source, reqwest::Method::POST, "/write").await? == "source:1");
    tokio::time::timeout(Duration::from_secs(5), proxy.capture.entered.acquire())
        .await??
        .forget();
    let captured = proxy.capture.exchanges.lock().unwrap().clone();
    ensure!(captured.len() == 1 && captured[0].delayed && !captured[0].dropped);
    let request = cellar::application::decode_rpc(&cellar::engine_api::RpcData::Structured(
        captured[0].request.clone().into(),
    ))?
    .to_json()?;
    let response = cellar::application::decode_rpc_result(
        &cellar::engine_api::RpcData::Structured(captured[0].response.clone().into()),
    )?
    .to_json()?;
    let envelope: Value = serde_json::from_slice(
        &base64::engine::general_purpose::STANDARD.decode(
            request[0]["records"][0]
                .as_str()
                .context("append envelope missing")?,
        )?,
    )?;
    let scope = envelope["source"]
        .as_str()
        .context("source identity missing")?;
    ensure!(
        envelope["payload"] == json!({"value":1})
            && envelope["sequence"] == "1"
            && response["ok"]["protocol"] == 1
            && response["ok"]["resource"] == resource().id
            && response["ok"]["lane"] == envelope["lane"]
            && response["ok"]["accepted_through"] == "1"
    );
    let original = pending(&client, &source, scope).await?;
    ensure!(
        original["pending_rows"] == "1"
            && original["incarnation"] == envelope["source_incarnation"]
            && original["lanes"][0]["revision"]["next_sequence"] == "2"
            && original["lanes"][0]["revision"]["settled_through"] == "0"
    );
    let accepted = history(&client, &destination).await?;
    ensure!(
        accepted
            .as_array()
            .is_some_and(|records| records.len() == 1)
            && accepted[0]["event"] == envelope
            && accepted[0]["offset"] == "1"
    );

    // Remove only this test's node lease through an independent store client.
    // This drives the real authority readback; no shutdown path or fake proof
    // adapter is used to close the source execution domain.
    let faults = ExecutionDomain::new(tokio::runtime::Handle::current());
    faults
        .run(async { bucket.delete("nodes/late-source.json").await })
        .await??;
    faults.shutdown().await;
    let source_services = source.services();
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        source.run_until(std::future::pending()),
    )
    .await??;
    ensure!(
        outcome
            == NodeOutcome::Fenced {
                code: 3,
                reason: FenceReason::NodeLeaseMissing
            },
        "source authority removal returned {outcome:?}"
    );
    ensure!(source_services
        .configure_metrics(Arc::new(HostMetricsSample::default))
        .is_err());
    ensure!(source_core.handle().spawn(async { 7 }).await? == 7);

    let (config, services) = inputs(
        "late-successor",
        &directory.path().join("successor"),
        &successor_core,
        Some(bucket),
        0,
    )
    .await?;
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let successor = Node::start(config, services, external, applications("successor")?).await?;
    super::super::ready(&successor).await?;
    let recovered = pending(&client, &successor, scope).await?;
    ensure!(
        recovered["pending_rows"] == "1"
            && recovered["incarnation"] == original["incarnation"]
            && recovered["lanes"][0]["revision"]["lane"]
                == original["lanes"][0]["revision"]["lane"]
            && recovered["lanes"][0]["revision"]["next_sequence"] == "2"
            && recovered["lanes"][0]["revision"]["settled_through"] == "0",
        "successor lost or settled the original obligation before replay: {recovered}"
    );
    ensure!(call(&client, &successor, reqwest::Method::GET, "/read").await? == "successor:1");
    ensure!(history(&client, &destination).await? == accepted);
    tokio::time::timeout(Duration::from_secs(5), proxy.capture.blocked.acquire())
        .await??
        .forget();
    ensure!(
        proxy.capture.exchanges.lock().unwrap().len() == 1,
        "successor replay reached the stream before its audit"
    );

    // The only released reply belongs to the fenced source. New append
    // submissions remain paused, so they cannot conceal stale settlement.
    proxy.capture.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), proxy.capture.released.acquire())
        .await??
        .forget();
    let after_late_reply = pending(&client, &successor, scope).await?;
    ensure!(
        after_late_reply["pending_rows"] == "1"
            && after_late_reply["incarnation"] == original["incarnation"]
            && after_late_reply["lanes"][0]["revision"]["settled_through"] == "0"
            && after_late_reply["lanes"][0]["revision"]["next_sequence"] == "2",
        "late old-owner reply settled successor state: {after_late_reply}"
    );
    ensure!(proxy.capture.exchanges.lock().unwrap().len() == 1);
    ensure!(history(&client, &destination).await? == accepted);
    proxy.capture.resume_appends();
    tokio::time::timeout(Duration::from_secs(5), async {
        while pending(&client, &successor, scope).await?["pending_rows"] != "0" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let attempts = proxy.capture.exchanges.lock().unwrap().clone();
    ensure!(attempts.len() >= 2 && attempts.iter().filter(|attempt| attempt.delayed).count() == 1);
    for attempt in attempts {
        ensure!(
            attempt.scope == captured[0].scope
                && attempt.request == captured[0].request
                && attempt.response == captured[0].response
                && !attempt.dropped,
            "successor replay changed original bytes or acceptance"
        );
    }
    ensure!(
        history(&client, &destination).await? == accepted,
        "retry duplicated stream acceptance"
    );
    ensure!(
        call(
            &client,
            &successor,
            reqwest::Method::POST,
            "/process-ack?source=projection"
        )
        .await?
            == "1"
    );
    let status = stream_operator(
        &client,
        &successor,
        &binding(true),
        json!({"kind":"subscription","name":"projection"}),
        "late-reply-audit",
    )
    .await?;
    ensure!(status["result"]["checkpoint"] == "1");
    stopped(successor)
        .await
        .context("late-reply successor complete shutdown")?;
    stopped(destination)
        .await
        .context("late-reply destination complete shutdown")?;
    proxy.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_proven_ack_survives_destination_fencing_without_consuming_the_next_lease(
) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let destination_core = Executor::new("late-ack-destination-core")?;
    let consumer_core = Executor::new("late-ack-consumer-core")?;
    let replacement_core = Executor::new("late-ack-replacement-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let observer = ExecutionDomain::new(tokio::runtime::Handle::current());
    let (mut config, services) = inputs(
        "late-ack-destination",
        &directory.path().join("destination"),
        &destination_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    config.actor.lease_ttl_ms = 5_000;
    config.startup.recovery_retry_backoff = Duration::from_millis(5_000);
    let mut proxy =
        peer_reply_fault::Proxy::start(services.internal_listener.local_addr()?).await?;
    config.identity.advertise = proxy.address.to_string();
    let destination = Node::start(
        config,
        services,
        graph(1, false),
        applications("destination")?,
    )
    .await?;
    super::super::ready(&destination).await?;
    let client = client()?;
    let original_owner = partition_owner(&observer, &bucket).await?;
    ensure!(original_owner["node"] == "late-ack-destination");
    let (config, services) = inputs(
        "late-ack-consumer",
        &directory.path().join("consumer"),
        &consumer_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let consumer = Node::start(config, services, external, applications("consumer")?).await?;
    super::super::ready(&consumer).await?;
    ensure!(call(&client, &consumer, reqwest::Method::POST, "/write").await? == "consumer:1");
    tokio::time::timeout(Duration::from_secs(5), async {
        while history(&client, &destination)
            .await?
            .as_array()
            .map(Vec::len)
            != Some(1)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("wait for first event before delayed ack")??;
    let accepted = history(&client, &destination).await?;
    ensure!(accepted[0]["event"]["payload"] == json!({"value":1}) && accepted[0]["offset"] == "1");
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::POST,
            "/process-save?source=projection"
        )
        .await?
            == "1"
    );
    let leased = subscription(&client, &destination).await?;
    ensure!(leased["checkpoint"] == "0" && leased["observation"]["lease_active"] == true);
    proxy.capture.delay_ack();
    let mut acknowledging = Box::pin(call(
        &client,
        &consumer,
        reqwest::Method::POST,
        "/ack-original?source=projection",
    ));
    cellar::asyncrt::select! {
        result = &mut acknowledging => anyhow::bail!("ack finished before its held proof boundary: {result:?}"),
        entered = tokio::time::timeout(Duration::from_secs(5), proxy.capture.entered.acquire()) => entered.context("wait for proven ack capture")??.forget(),
    }
    let ack = proxy
        .capture
        .exchanges
        .lock()
        .unwrap()
        .iter()
        .find(|exchange| exchange.method == "__streamAck")
        .cloned()
        .context("held ack missing")?;
    ensure!(ack.delayed && !ack.dropped && ack.scope == stream_scope()?);
    let request = cellar::application::decode_rpc(&cellar::engine_api::RpcData::Structured(
        ack.request.clone().into(),
    ))?
    .to_json()?;
    let receipt = Receipt::decode(
        request[0]["receipt"]
            .as_str()
            .context("ack receipt missing")?,
    )?;
    let identity = serde_json::to_value(&receipt)?;
    ensure!(
        identity["resource"] == resource().id
            && identity["name"] == "projection"
            && identity["first"] == "1"
            && identity["last"] == "1"
    );
    let reply = cellar::application::decode_rpc_result(&cellar::engine_api::RpcData::Structured(
        ack.response.clone().into(),
    ))?
    .to_json()?;
    ensure!(reply["ok"]["status"] == "acknowledged");
    let committed = subscription(&client, &destination).await?;
    ensure!(
        committed["checkpoint"] == "1"
            && committed["lease"].is_null()
            && committed["identity"] == leased["identity"]
    );

    proxy.capture.expect_upstream_close();
    observer
        .run(async { bucket.delete("nodes/late-ack-destination.json").await })
        .await??;
    let destination_services = destination.services();
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        destination.run_until(std::future::pending()),
    )
    .await
    .context("wait for destination authority fence and join")??;
    ensure!(
        outcome
            == NodeOutcome::Fenced {
                code: 3,
                reason: FenceReason::NodeLeaseMissing
            },
        "destination lease removal returned {outcome:?}"
    );
    ensure!(destination_services
        .configure_metrics(Arc::new(HostMetricsSample::default))
        .is_err());
    ensure!(destination_core.handle().spawn(async { 7 }).await? == 7);
    let (config, services) = inputs(
        "late-ack-replacement",
        &directory.path().join("replacement"),
        &replacement_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let replacement = Node::start(config, services, external, applications("replacement")?).await?;
    super::super::ready(&replacement).await?;
    // Activation/inspection can resolve another healthy host as the actual
    // partition owner. Require its authoritative epoch to advance, without
    // pretending that this test can dictate the placement winner.
    let recovered = subscription(&client, &replacement).await?;
    let new_owner = partition_owner(&observer, &bucket).await?;
    ensure!(
        new_owner["node"] != original_owner["node"]
            && new_owner["epoch"].as_u64().context("new epoch missing")?
                > original_owner["epoch"]
                    .as_u64()
                    .context("old epoch missing")?,
        "partition did not change owner/epoch: {original_owner} -> {new_owner}"
    );
    ensure!(
        recovered["identity"] == committed["identity"]
            && recovered["checkpoint"] == "1"
            && recovered["lease"].is_null(),
        "ack was lost before saved-receipt replay: {recovered}"
    );
    ensure!(history(&client, &replacement).await? == accepted);

    proxy.capture.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), proxy.wait_for_release())
        .await
        .context("wait for held ack release")??;
    let original_outcome = tokio::time::timeout(Duration::from_secs(5), &mut acknowledging)
        .await
        .context("wait for original ack caller after held reply release")??;
    drop(acknowledging);
    ensure!(
        matches!(original_outcome.as_str(), "Acknowledged" | "Ambiguous"),
        "delayed proven ack returned {original_outcome}"
    );
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::GET,
            "/processed?source=projection"
        )
        .await?
            == "1"
    );
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::POST,
            "/ack-original?source=projection"
        )
        .await?
            == "AlreadyAcknowledged"
    );
    ensure!(subscription(&client, &replacement).await?["checkpoint"] == "1");
    ensure!(call(&client, &consumer, reqwest::Method::POST, "/write").await? == "consumer:2");
    tokio::time::timeout(Duration::from_secs(5), async {
        while history(&client, &replacement)
            .await?
            .as_array()
            .map(Vec::len)
            != Some(2)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("wait for second event after destination takeover")??;
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::POST,
            "/process-save?source=projection"
        )
        .await?
            == "3"
    );
    let second_lease = subscription(&client, &replacement).await?;
    ensure!(
        second_lease["checkpoint"] == "1"
            && second_lease["lease"]["first"] == "2"
            && second_lease["lease"]["last"] == "2"
            && second_lease["observation"]["lease_active"] == true
    );
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::POST,
            "/ack-original?source=projection"
        )
        .await?
            == "AlreadyAcknowledged"
    );
    let after_old_receipt = subscription(&client, &replacement).await?;
    ensure!(
        after_old_receipt["checkpoint"] == "1"
            && after_old_receipt["lease"] == second_lease["lease"],
        "old ack consumed the new lease: {after_old_receipt}"
    );
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::POST,
            "/ack-stored?source=projection"
        )
        .await?
            == "Acknowledged"
    );
    ensure!(subscription(&client, &replacement).await?["checkpoint"] == "2");
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::GET,
            "/processed?source=projection"
        )
        .await?
            == "3"
    );
    let attempts = proxy
        .capture
        .exchanges
        .lock()
        .unwrap()
        .iter()
        .filter(|exchange| exchange.method == "__streamAck")
        .cloned()
        .collect::<Vec<_>>();
    ensure!(attempts.iter().filter(|exchange| exchange.delayed).count() == 1);
    // Requests resolved to the new owner may bypass this dead owner's proxy.
    // Any original-epoch duplicate observed here must retain the exact receipt.
    for attempt in attempts {
        ensure!(attempt.request == ack.request && attempt.scope == ack.scope);
    }
    stopped(consumer)
        .await
        .context("late-ack consumer complete shutdown")?;
    stopped(replacement)
        .await
        .context("late-ack replacement complete shutdown")?;
    proxy.stop().await?;
    observer.shutdown().await;
    Ok(())
}
