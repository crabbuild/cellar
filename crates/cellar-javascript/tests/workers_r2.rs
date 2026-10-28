// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The unmodified published backup gateway uses real native SQLite Blob storage.
#![cfg(feature = "node")]
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar::{bucket::Bucket, sdk::*};
use cellar_javascript::{EventKind, NodeOptions, Options, Source};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
#[path = "support/joined_node.rs"]
mod joined_node;
#[path = "support/native.rs"]
mod native;

#[test]
fn published_backup_gateway_uses_native_resumable_storage() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tools/sandbox/cloudflare-v1");
    let build = std::process::Command::new("node")
        .arg(root.join("build.mjs"))
        .output()?;
    ensure!(
        build.status.success(),
        "R2 fixture build: {}",
        String::from_utf8_lossy(&build.stderr)
    );
    joined_node::run(
        Options {
            max_body_bytes: 4096,
            ..Default::default()
        },
        NodeOptions::new(root.join("dist/r2.mjs")),
        Source::module()
            .workers_lifetimes()
            .events([EventKind::Fetch, EventKind::WorkerRpc])
            .cell_events("Counter", [EventKind::Fetch]),
        |application| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(exercise(application))
        },
    )
}

async fn start(
    path: &Path,
    database: &Path,
    application: Arc<dyn Application>,
) -> Result<(Node, String)> {
    let bucket = Bucket::open_dev(database)?;
    let storage = ReplicaStorage::new(
        bucket.store.clone(),
        ReplicaStorageOptions {
            prefix: bucket.prefix.clone(),
            ..Default::default()
        },
    )?;
    native::start_configured(path, application, move |config, services, graph| {
        // Dedicated storage qualification includes fsync and cold native fleet
        // bootstrap. Existing 250 ms module/routing fixtures are unchanged.
        config.runtime.handler_budget = Duration::from_secs(5);
        graph.primary.options.blob_bindings = vec![("BACKUPS".into(), "sandbox-backups".into())];
        services.fleet = Some(NodeFleet {
            lease_bucket: bucket.clone(),
            bucket,
            replica_storage: storage,
            replication: ReplicationOptions {
                flush_interval: Duration::ZERO,
                compaction: None,
                ..Default::default()
            },
            log_transport: Arc::new(SignedPeerTransport::from_clients(
                services.peer_http.clone(),
                services.peer_http.clone(),
                services.peer_auth.clone(),
            )),
            log: LogOptions {
                record_publish_timeout: Duration::from_secs(3),
                ..Default::default()
            },
            fleet_durability: false,
        });
    })
    .await
}

async fn stop(node: Node) -> Result<()> {
    let outcome = node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(
        matches!(outcome, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out)
    );
    Ok(())
}

async fn request(client: &reqwest::Client, address: &str, path: &str) -> Result<Value> {
    let response = client.get(format!("{address}{path}")).send().await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status.is_success(), "{path}: {status}: {body}");
    Ok(serde_json::from_str(&body)?)
}

async fn part(
    client: &reqwest::Client,
    address: &str,
    id: &str,
    number: u32,
    bytes: Vec<u8>,
) -> Result<String> {
    let response = client
        .put(format!("{address}/parts/{number}"))
        .query(&[("upload", id)])
        .body(bytes)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status.is_success(), "part: {status}: {body}");
    let value: Value = serde_json::from_str(&body)?;
    Ok(value["etag"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing actual part receipt"))?
        .into())
}

async fn exercise(application: Arc<dyn Application>) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("bucket.sqlite");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let (node, address) = start(
        &directory.path().join("first"),
        &database,
        application.clone(),
    )
    .await?;
    let first = async {
        ensure!(request(&client, &address, "/head").await?.is_null());
        let value = request(&client, &address, "/create").await?;
        let id = value["uploadId"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing upload ID"))?
            .to_string();
        let obsolete = part(&client, &address, &id, 1, b"obsolete".to_vec()).await?;
        ensure!(!obsolete.is_empty());
        ensure!(
            request(&client, &address, "/head").await?.is_null(),
            "incomplete object became public"
        );
        Ok::<_, anyhow::Error>((id, obsolete))
    }
    .await;
    stop(node).await?;
    let (id, obsolete) = first?;
    // Rebuild the real client and node. No in-process upload registry supplies
    // either the bytes or receipts to the new gateway events.
    let (node, address) = start(&directory.path().join("second"), &database, application).await?;
    let result = async {
        ensure!(
            request(&client, &address, "/part-admission").await?
                == json!({"code": "ValidationError", "outcome": "not_committed", "locked": false})
        );
        ensure!(
            request(&client, &address, "/large-stream").await?
                == json!({"bytes": 256 * 1024 + 17, "verified": true})
        );
        let first = part(&client, &address, &id, 1, vec![0, 255, 42, 7]).await?;
        let second = part(&client, &address, &id, 2, b"backup".to_vec()).await?;
        ensure!(first != obsolete);
        let short = client
            .get(format!("{address}/short-part"))
            .query(&[("upload", &id)])
            .send()
            .await?;
        let short_status = short.status();
        let short_body = short.text().await?;
        ensure!(
            short_status.as_u16() == 502 && short_body.contains("too few bytes"),
            "published gateway accepted a short part: {short_status}: {short_body}"
        );
        let oversized = client
            .get(format!("{address}/oversized"))
            .query(&[("upload", &id)])
            .send()
            .await?
            .error_for_status()?;
        ensure!(
            serde_json::from_str::<Value>(&oversized.text().await?)?
                == json!({"cancelled":true,"name":"RangeError"})
        );
        let stale = client
            .post(format!("{address}/complete"))
            .query(&[("upload", &id)])
            .json(&json!([{"partNumber":1,"etag":obsolete},{"partNumber":2,"etag":second}]))
            .send()
            .await?;
        ensure!(
            stale.status().is_server_error(),
            "stale receipt was accepted"
        );
        ensure!(request(&client, &address, "/head").await?.is_null());
        let complete = client
            .post(format!("{address}/complete"))
            .query(&[("upload", &id)])
            .json(&json!([{"partNumber":1,"etag":first},{"partNumber":2,"etag":second}]))
            .send()
            .await?;
        let status = complete.status();
        let body = complete.text().await?;
        ensure!(
            status.is_success() && serde_json::from_str::<Value>(&body)? == json!({"size":10}),
            "complete: {status}: {body}"
        );
        let head = request(&client, &address, "/head").await?;
        ensure!(
            head["size"] == 10
                && head["custom"]["name"] == "backup 雪"
                && head["contentType"] == "application/zstd"
                && head["uploaded"] == true,
            "metadata: {head}"
        );
        ensure!(
            !head["etag"].as_str().unwrap_or_default().is_empty()
                && head["httpEtag"] == json!(format!("\"{}\"", head["etag"].as_str().unwrap()))
        );
        let range = client
            .get(format!("{address}/object"))
            .header("range", "bytes=1-5")
            .send()
            .await?;
        ensure!(
            range.status().as_u16() == 206
                && range
                    .headers()
                    .get("content-range")
                    .is_some_and(|value| value == "bytes 1-5/10")
        );
        ensure!(range.bytes().await?.as_ref() == [255, 42, 7, b'b', b'a']);
        ensure!(
            request(&client, &address, "/get").await?
                == json!({"bytes":[0,255,42,7,98,97,99,107,117,112],"before":false,"after":true})
        );
        let saved = client
            .get(format!("{address}/save"))
            .send()
            .await?
            .error_for_status()?;
        ensure!(saved.text().await? == "saved");
        ensure!(request(&client, &address, "/borrow").await? == json!(["binding", "body"]));
        ensure!(
            request(&client, &address, "/transaction").await?
                == json!({"code":"ValidationError","outcome":"not_committed"})
        );
        let aborted = request(&client, &address, "/create").await?;
        let aborted = aborted["uploadId"].as_str().unwrap();
        part(&client, &address, aborted, 1, b"discard".to_vec()).await?;
        client
            .get(format!("{address}/abort"))
            .query(&[("upload", aborted)])
            .send()
            .await?
            .error_for_status()?;
        ensure!(
            request(&client, &address, "/head").await?["size"] == 10,
            "abort changed public object"
        );
        client
            .get(format!("{address}/delete"))
            .send()
            .await?
            .error_for_status()?;
        ensure!(request(&client, &address, "/head").await?.is_null());
        Ok(())
    }
    .await;
    stop(node).await?;
    result?;
    let connection = rusqlite::Connection::open(&database)?;
    let uploads: u64 =
        connection.query_row("SELECT count(*) FROM __sys_local_uploads", [], |row| {
            row.get(0)
        })?;
    let parts: u64 = connection.query_row("SELECT count(*) FROM __sys_local_parts", [], |row| {
        row.get(0)
    })?;
    ensure!(
        uploads == 0 && parts == 0,
        "gateway leaked uploads/parts: {uploads}/{parts}"
    );
    Ok(())
}
