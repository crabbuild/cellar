//! A lost proven fetch reply retains its lease across owner handoff. Replaying
//! the fetch recovers authority, while inbox identity governs local effects.

use super::*;
use base64::Engine;
use serde_json::{json, Value};

pub(super) async fn fetch_and_save(context: Context, url: &str) -> Result<Output> {
    let url = url::Url::parse(url)?;
    let fetch_id = url
        .query_pairs()
        .find(|(key, _)| key == "id")
        .map(|(_, value)| value.into_owned())
        .context("fetch ID missing")?;
    let events = context.stream::<Changed>("EVENTS")?;
    let fetched = events
        .subscription("projection")?
        .next_with_id(
            &context,
            &fetch_id,
            BatchOptions {
                max_records: 1,
                max_bytes: 4096,
                wait_ms: 0,
            },
        )
        .await;
    let value = match fetched {
        Err(error) => {
            let refusal = error
                .downcast_ref::<DeliveryError>()
                .context("fetch refusal was untyped")?;
            if refusal.code == ErrorCode::Unavailable {
                // The public message can omit a retained native admission or
                // transport cause. Keep that bounded chain in this private
                // fixture's log; response and mutation/replay policy stay intact.
                let detail: String = format!("{error:#}").chars().take(2_048).collect();
                eprintln!("event fetch unavailable: {detail}");
            }
            json!({"error":refusal.code.as_str()})
        }
        Ok(None) => json!({"batch":null}),
        Ok(Some(batch)) => {
            let db = context.storage()?;
            db.tx(|tx| {
                for record in &batch.records {
                    if tx.inbox("projection")?.accept(record)? {
                        let prior = tx
                            .get_json("processed")?
                            .and_then(|value| value.as_i64())
                            .unwrap_or(0);
                        tx.put_json(
                            "processed",
                            &json!(prior + record.event.decode::<Changed>()?.value),
                        )?;
                    }
                }
                tx.put_json("ack_receipt", &serde_json::to_value(batch.receipt())?)?;
                if tx.get_json("original_ack_receipt")?.is_none() {
                    tx.put_json(
                        "original_ack_receipt",
                        &serde_json::to_value(batch.receipt())?,
                    )?;
                }
                Ok(())
            })?;
            db.sync().await?;
            json!({"batch":{
                "receipt":batch.receipt().encode()?,
                "issued_ms":batch.issued_at_ms.to_string(),
                "expires_ms":batch.expires_at_ms.to_string(),
                "records":batch.records.iter().map(|record| json!({
                    "offset":record.offset.to_string(),
                    "envelope":base64::engine::general_purpose::STANDARD.encode(record.encoded()),
                })).collect::<Vec<_>>(),
            }, "processed":db.get_json("processed")?})
        }
    };
    Ok(super::super::response(serde_json::to_string(&value)?))
}

async fn status(client: &reqwest::Client, node: &Node) -> Result<Value> {
    Ok(stream_operator(
        client,
        node,
        &binding(true),
        json!({"kind":"subscription","name":"projection"}),
        "fetch-recovery",
    )
    .await?["result"]
        .clone())
}

async fn owner(domain: &ExecutionDomain, bucket: &cellar::bucket::Bucket) -> Result<Value> {
    domain
        .run(async {
            let (bytes, _) = bucket
                .get(&format!("cells/{}/own.json", stream_scope()?))
                .await?
                .context("partition owner missing")?;
            Ok::<_, anyhow::Error>(serde_json::from_slice(&bytes)?)
        })
        .await?
}

async fn fetch(client: &reqwest::Client, node: &Node, id: &str) -> Result<Value> {
    serde_json::from_str(
        &call(
            client,
            node,
            reqwest::Method::POST,
            &format!("/fetch-save?source=projection&id={id}"),
        )
        .await?,
    )
    .map_err(Into::into)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_proven_fetch_recovers_exact_lease_after_owner_handoff() -> Result<()> {
    const FETCH: &str = "000000000000000000000000000000a1";
    const COMPETING: &str = "000000000000000000000000000000a2";
    let directory = tempfile::tempdir()?;
    let destination_core = Executor::new("fetch-destination-core")?;
    let consumer_core = Executor::new("fetch-consumer-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let observer = ExecutionDomain::new(tokio::runtime::Handle::current());
    let (mut config, services) = inputs(
        "fetch-destination",
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
    let original_owner = owner(&observer, &bucket).await?;
    ensure!(original_owner["node"] == "fetch-destination");
    let (config, services) = inputs(
        "fetch-consumer",
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
    let client = client()?;
    ensure!(call(&client, &consumer, reqwest::Method::POST, "/write").await? == "consumer:1");
    tokio::time::timeout(Duration::from_secs(5), async {
        while status(&client, &destination).await?["end"] != "2" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("wait for first event before fetch loss")??;
    proxy.capture.arm("__streamNext");
    let mut fetching = Box::pin(fetch(&client, &consumer, FETCH));
    cellar::asyncrt::select! {
        result = &mut fetching => anyhow::bail!("fetch finished before lost-reply boundary: {result:?}"),
        entered = tokio::time::timeout(Duration::from_secs(5), proxy.capture.entered.acquire()) =>
            entered.context("capture proven fetch response")??.forget(),
    }
    let captured = proxy
        .capture
        .exchanges
        .lock()
        .unwrap()
        .iter()
        .filter(|exchange| exchange.method == "__streamNext")
        .cloned()
        .collect::<Vec<_>>();
    ensure!(captured.len() == 1 && captured[0].dropped && captured[0].scope == stream_scope()?);
    let request = cellar::application::decode_rpc(&cellar::engine_api::RpcData::Structured(
        captured[0].request.clone().into(),
    ))?
    .to_json()?;
    ensure!(request[0]["fetch_id"] == FETCH);
    let response = cellar::application::decode_rpc_result(
        &cellar::engine_api::RpcData::Structured(captured[0].response.clone().into()),
    )?
    .to_json()?;
    let original = response["ok"].clone();
    let receipt = Receipt::decode(
        original["receipt"]
            .as_str()
            .context("lease receipt missing")?,
    )?;
    let identity = serde_json::to_value(&receipt)?;
    ensure!(
        identity["resource"] == resource().id
            && identity["first"] == "1"
            && identity["last"] == "1"
    );
    ensure!(original["records"]
        .as_array()
        .is_some_and(|records| records.len() == 1));
    let envelope: Value = serde_json::from_slice(
        &base64::engine::general_purpose::STANDARD.decode(
            original["records"][0]["envelope"]
                .as_str()
                .context("record envelope missing")?,
        )?,
    )?;
    ensure!(envelope["payload"] == json!({"value":1}));
    let committed = status(&client, &destination).await?;
    ensure!(
        committed["checkpoint"] == "0"
            && committed["observation"]["lease_active"] == true
            && committed["lease"]["generation"] == identity["generation"]
            && committed["lease"]["expires_ms"] == original["expires_ms"]
    );
    // Inspect the actual proven lease before losing its response. No replay or
    // consumer effect is permitted to hide a missing lease installation.
    proxy.capture.release.add_permits(1);
    let outcome = tokio::time::timeout(Duration::from_secs(5), &mut fetching)
        .await
        .context("lost fetch returns to caller")??;
    drop(fetching);
    ensure!(
        outcome == json!({"error":"Ambiguous"}),
        "lost fetch returned {outcome}"
    );
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::GET,
            "/processed?source=projection"
        )
        .await?
            == "0"
    );
    ensure!(fetch(&client, &consumer, COMPETING).await? == json!({"error":"LeaseBusy"}));
    ensure!(status(&client, &destination).await?["lease"] == committed["lease"]);

    proxy.capture.expect_upstream_close();
    stopped(destination)
        .await
        .context("handoff destination with an unacknowledged lost fetch")?;
    let recovered = status(&client, &consumer).await?;
    let new_owner = owner(&observer, &bucket).await?;
    ensure!(
        new_owner["node"] != original_owner["node"]
            && new_owner["epoch"].as_u64().context("new epoch missing")?
                > original_owner["epoch"]
                    .as_u64()
                    .context("old epoch missing")?
    );
    ensure!(
        recovered["identity"] == committed["identity"]
            && recovered["checkpoint"] == "0"
            && recovered["lease"] == committed["lease"]
            && recovered["observation"]["lease_active"] == true,
        "handoff lost or replaced the original live lease: {recovered}"
    );
    let replayed = fetch(&client, &consumer, FETCH).await?;
    ensure!(
        replayed["batch"] == original && replayed["processed"] == 1,
        "same fetch ID did not recover exact batch: {replayed}"
    );
    ensure!(
        fetch(&client, &consumer, FETCH).await? == replayed,
        "replay repeated inbox effects"
    );
    ensure!(status(&client, &consumer).await?["checkpoint"] == "0");
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
    ensure!(status(&client, &consumer).await?["checkpoint"] == "1");

    ensure!(call(&client, &consumer, reqwest::Method::POST, "/write").await? == "consumer:2");
    tokio::time::timeout(Duration::from_secs(5), async {
        while status(&client, &consumer).await?["end"] != "3" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("wait for second event after recovered lease ack")??;
    let next = fetch(&client, &consumer, FETCH).await?;
    let fresh = Receipt::decode(
        next["batch"]["receipt"]
            .as_str()
            .context("fresh receipt missing")?,
    )?;
    let fresh_identity = serde_json::to_value(fresh)?;
    ensure!(
        next["processed"] == 3
            && fresh_identity["first"] == "2"
            && fresh_identity["last"] == "2"
            && fresh_identity["generation"]
                .as_str()
                .context("fresh generation missing")?
                .parse::<i64>()?
                > identity["generation"]
                    .as_str()
                    .context("original generation missing")?
                    .parse::<i64>()?
            && fresh_identity["token"] != identity["token"]
    );
    let leased = status(&client, &consumer).await?;
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
    ensure!(status(&client, &consumer).await?["lease"] == leased["lease"]);
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
    let finished = status(&client, &consumer).await?;
    ensure!(finished["checkpoint"] == "2" && finished["lease"].is_null());
    stopped(consumer)
        .await
        .context("complete fetch recovery consumer shutdown")?;
    proxy.stop().await?;
    observer.shutdown().await;
    Ok(())
}
