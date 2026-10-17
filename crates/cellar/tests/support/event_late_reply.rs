//! Delayed event replies cross ownership and source-control transitions.
//! Audit the original obligation before allowing a newer dispatch to settle it.

use super::*;
use base64::Engine;
use serde_json::{json, Value};

// This fixture deliberately removes authority to exercise NodeLeaseMissing.
// Atomically retain the exact deleted root: a separate GET followed by DELETE
// could miss an intervening log/lease CAS and discard its proof metadata.
async fn remove_authority(
    domain: &ExecutionDomain,
    bucket: &cellar::bucket::Bucket,
    node: &str,
) -> Result<Value> {
    ensure!(
        bucket.scheme() == "dev",
        "authority fault requires the private SQLite store"
    );
    let database = std::path::PathBuf::from(&bucket.name);
    let key = format!("{}nodes/{node}.json", bucket.prefix);
    let bytes = domain
        .blocking(move || {
            let connection = rusqlite::Connection::open(database)?;
            connection.busy_timeout(Duration::from_secs(1))?;
            connection.pragma_update(None, "synchronous", "FULL")?;
            let bytes: Vec<u8> = connection.query_row(
                "DELETE FROM __sys_local_objects WHERE key = ?1 RETURNING body",
                [key],
                |row| row.get(0),
            )?;
            Ok::<_, anyhow::Error>(bytes)
        })?
        .await??;
    let saved: Value = serde_json::from_slice(&bytes)?;
    ensure!(
        saved["node"] == node,
        "removed authority belongs to another node"
    );
    Ok(saved)
}

async fn restore_joined_authority(
    domain: &ExecutionDomain,
    bucket: &cellar::bucket::Bucket,
    node: &str,
    saved: &Value,
) -> Result<()> {
    domain
        .run(async {
            let key = format!("nodes/{node}.json");
            ensure!(
                bucket.get(&key).await?.is_none(),
                "removed lease was recreated before join"
            );
            let mut expired = saved.clone();
            expired["expires_ms"] = json!(cellar::ownership_store::now_ms().saturating_sub(1));
            ensure!(
                bucket
                    .put_cas(&key, serde_json::to_vec(&expired)?, None)
                    .await?
                    .is_some(),
                "a concurrent node replaced the removed authority"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?
}

fn dispatch_observation(node: &Node) -> Result<[u64; 5]> {
    let snapshot = node.services().event_metrics();
    let counter = |name| {
        snapshot
            .counters
            .iter()
            .find(|counter| counter.name == name)
            .map(|counter| counter.value)
            .context("dispatch observation missing")
    };
    Ok([
        counter("cellar_event_dispatch_rpc_attempts_total")?,
        counter("cellar_event_dispatch_rpc_succeeded_total")?,
        counter("cellar_event_dispatch_rpc_failed_total")?,
        counter("cellar_event_dispatch_rpc_cancelled_total")?,
        snapshot.inflight_dispatches,
    ])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operator_retry_ignores_prior_generation_append_acceptance() -> Result<()> {
    operator_retry_fences_held_append(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operator_retry_ignores_prior_generation_append_refusal() -> Result<()> {
    operator_retry_fences_held_append(false).await
}

async fn operator_retry_fences_held_append(accepted: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let destination_core = Executor::new("retry-reply-destination")?;
    let source_core = Executor::new("retry-reply-source")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (mut config, services) = inputs(
        "retry-destination",
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

    let (config, services) = inputs(
        "retry-source",
        &directory.path().join("source"),
        &source_core,
        Some(bucket),
        0,
    )
    .await?;
    let operation_budget = Duration::from_millis(config.actor.core.operation_deadline_ms.unwrap());
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let source = Node::start(config, services, external, applications("source")?).await?;
    super::super::ready(&source).await?;
    // Establish the business source here before any administrative call can
    // place it on the destination and turn this into a local dispatch.
    ensure!(call(&client, &source, reqwest::Method::GET, "/read").await? == "source:0");
    if !accepted {
        ensure!(
            call(&client, &source, reqwest::Method::POST, "/revoke-publish").await? == "active"
        );
    }
    proxy.capture.delay_append();
    let submitted = std::time::Instant::now();
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
                .context("held append envelope missing")?,
        )?,
    )?;
    let scope = envelope["source"]
        .as_str()
        .context("held append source missing")?;
    ensure!(envelope["payload"] == json!({"value":1}) && envelope["sequence"] == "1");
    if accepted {
        ensure!(response["ok"]["accepted_through"] == "1");
    } else {
        ensure!(
            response["error"]["code"] == "Forbidden",
            "held append did not return authorization refusal: {response}"
        );
        ensure!(
            call(&client, &source, reqwest::Method::POST, "/restore-publish").await? == "active"
        );
    }
    let original = pending(&client, &source, scope).await?;
    let revision = original["lanes"][0]["revision"].clone();
    let generation: u64 = revision["dispatch_generation"]
        .as_str()
        .context("dispatch generation missing")?
        .parse()?;
    let expected_control_generation = (generation + 1).to_string();
    let expected_fresh_generation = (generation + 2).to_string();
    ensure!(
        original["pending_rows"] == "1"
            && revision["next_sequence"] == "2"
            && revision["settled_through"] == "0"
    );
    let before = history(&client, &destination).await?;
    ensure!(before.as_array().context("stream history missing")?.len() == usize::from(accepted));
    if accepted {
        ensure!(before[0]["event"] == envelope && before[0]["offset"] == "1");
    }
    let change = json!({"op":"retry","change":{"operation":"000000000000000000000000000000a1",
        "reason":"fence a still-pending append reply","expected":revision}});
    let receipt = operator(&client, &source, scope, &change, "retry-reply-audit").await?;
    ensure!(receipt["result"]["rearmed"] == true);
    let rearmed = pending(&client, &source, scope).await?;
    ensure!(
        rearmed["pending_rows"] == "1"
            && rearmed["lanes"][0]["revision"]["dispatch_generation"].as_str()
                == Some(expected_control_generation.as_str())
            && rearmed["lanes"][0]["revision"]["settled_through"] == "0"
    );
    ensure!(
        dispatch_observation(&source)? == [1, 0, 0, 0, 1],
        "old RPC completed before the control cut"
    );
    ensure!(
        proxy.capture.blocked.try_acquire().is_err(),
        "a new batch overlapped the held lane"
    );
    proxy.capture.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), proxy.capture.released.acquire())
        .await??
        .forget();
    // A fresh submission proves the old callback returned and released its
    // lane reservation. Keep this submission paused so it cannot hide stale
    // settlement or a stale failure overwriting the operator's retry.
    let permit =
        match tokio::time::timeout(Duration::from_secs(5), proxy.capture.blocked.acquire()).await {
            Ok(permit) => permit?,
            Err(error) => {
                let state = pending(&client, &source, scope).await?;
                return Err(error).with_context(|| {
                    format!("new dispatch did not follow the obsolete reply; source={state}")
                });
            }
        };
    permit.forget();
    ensure!(
        submitted.elapsed() < operation_budget && !source.is_closed(),
        "old callback crossed its original operation deadline or source closure"
    );
    let after = pending(&client, &source, scope).await?;
    ensure!(
        after["pending_rows"] == "1"
            && after["incarnation"] == original["incarnation"]
            && after["lanes"][0]["revision"]["dispatch_generation"].as_str()
                == Some(expected_fresh_generation.as_str())
            && after["lanes"][0]["revision"]["next_sequence"] == "2"
            && after["lanes"][0]["revision"]["settled_through"] == "0"
            && after["lanes"][0]["status"] == "pending"
            && after["lanes"][0]["blocked_reason"].is_null()
            && after["lanes"][0]["last_error"].is_null(),
        "obsolete reply changed newer work: {after}"
    );
    ensure!(
        dispatch_observation(&source)? == [2, u64::from(accepted), u64::from(!accepted), 0, 1],
        "old reply was not consumed at the intended boundary"
    );
    ensure!(
        operator(&client, &source, scope, &change, "retry-reply-audit").await? == receipt,
        "replaying the operator receipt retried a newer generation"
    );
    let observed = pending(&client, &source, scope).await?;
    ensure!(observed["lanes"][0]["revision"] == after["lanes"][0]["revision"]);
    ensure!(proxy.capture.exchanges.lock().unwrap().len() == 1);
    ensure!(history(&client, &destination).await? == before);
    ensure!(
        dispatch_observation(&source)? == [2, u64::from(accepted), u64::from(!accepted), 0, 1],
        "fresh replay timed out before the measuring gate opened"
    );
    proxy.capture.resume_appends();
    tokio::time::timeout(Duration::from_secs(5), async {
        while pending(&client, &source, scope).await?["pending_rows"] != "0" {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let settled = pending(&client, &source, scope).await?;
    ensure!(
        settled["incarnation"] == original["incarnation"]
            && settled["lanes"][0]["revision"]["lane"] == revision["lane"]
            && settled["lanes"][0]["revision"]["next_sequence"] == "2"
            && settled["lanes"][0]["revision"]["settled_through"] == "1"
            && settled["lanes"][0]["revision"]["dispatch_generation"].as_str()
                == Some(expected_fresh_generation.as_str()),
        "fresh retry changed producer progress: {settled}"
    );
    let exchanges = proxy.capture.exchanges.lock().unwrap().clone();
    ensure!(
        exchanges.len() >= 2 && exchanges.iter().filter(|exchange| exchange.delayed).count() == 1
    );
    for exchange in &exchanges {
        ensure!(
            exchange.scope == captured[0].scope
                && exchange.request == captured[0].request
                && !exchange.dropped,
            "operator retry changed the original append bytes"
        );
        if accepted {
            ensure!(exchange.response == captured[0].response);
        }
    }
    let delivered = history(&client, &destination).await?;
    ensure!(
        delivered
            .as_array()
            .context("delivered history missing")?
            .len()
            == 1
            && delivered[0]["event"] == envelope
            && delivered[0]["offset"] == "1"
    );
    ensure!(
        call(
            &client,
            &source,
            reqwest::Method::POST,
            "/process-retry?source=projection"
        )
        .await?
            == "1"
    );
    ensure!(
        call(
            &client,
            &source,
            reqwest::Method::POST,
            "/process-ack?source=projection"
        )
        .await?
            == "1"
    );
    ensure!(subscription(&client, &destination).await?["checkpoint"] == "1");
    ensure!(operator(&client, &source, scope, &change, "retry-reply-audit").await? == receipt);
    stopped(source)
        .await
        .context("retry-reply source complete shutdown")?;
    stopped(destination)
        .await
        .context("retry-reply destination complete shutdown")?;
    proxy.stop().await?;
    Ok(())
}

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
    let result = Box::pin(late_proven_append_reply_case(directory.path())).await;
    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            let retained = directory.keep();
            Err(error)
                .with_context(|| format!("late append fixture retained at {}", retained.display()))
        }
    }
}

async fn late_proven_append_reply_case(directory: &std::path::Path) -> Result<()> {
    let destination_core = Executor::new("late-reply-destination-core")?;
    let source_core = Executor::new("late-reply-source-core")?;
    let successor_core = Executor::new("late-reply-successor-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.join("fleet.sqlite"))?;
    let (mut config, services) = inputs(
        "late-destination",
        &directory.join("destination"),
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
    .await
    .context("late append destination startup")?;
    super::super::ready(&destination).await?;
    let client = client()?;
    ensure!(history(&client, &destination).await? == json!([]));

    let (mut config, services) = inputs(
        "late-source",
        &directory.join("source"),
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
    let source = Node::start(config, services, external, applications("source")?)
        .await
        .context("late append source startup")?;
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
    let original = pending(&client, &source, scope)
        .await
        .context("inspect original source before fencing")?;
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
    let removed = remove_authority(&faults, &bucket, "late-source").await?;
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
    // Missing authority fenced the old process. Return its exact proof fields
    // only after join, with expiry shortened, before asking for a successor.
    restore_joined_authority(&faults, &bucket, "late-source", &removed).await?;
    faults.shutdown().await;

    let (config, services) = inputs(
        "late-successor",
        &directory.join("successor"),
        &successor_core,
        Some(bucket),
        0,
    )
    .await?;
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let successor = Node::start(config, services, external, applications("successor")?)
        .await
        .context("late append successor startup")?;
    super::super::ready(&successor).await?;
    let recovered = pending(&client, &successor, scope)
        .await
        .context("inspect recovered source before old reply release")?;
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
    let after_late_reply = pending(&client, &successor, scope)
        .await
        .context("inspect successor after old reply release")?;
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
        while pending(&client, &successor, scope)
            .await
            .context("inspect fresh dispatch settlement")?["pending_rows"]
            != "0"
        {
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
    let removed = remove_authority(&observer, &bucket, "late-ack-destination").await?;
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
    restore_joined_authority(&observer, &bucket, "late-ack-destination", &removed).await?;
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
