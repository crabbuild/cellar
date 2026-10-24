// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Streaming object traffic and a separately acknowledged cell reference.
use super::*;
use sha2::{Digest, Sha256};

pub(super) async fn upload(
    client: &Client,
    config: &Config,
    writer: &Mutex<BufWriter<tokio::fs::File>>,
    entry: &Entry,
    key: &str,
    bytes: &[u8],
) -> Result<Value> {
    let checksum = format!("{:x}", Sha256::digest(bytes));
    let path = format!(
        "/attachments/{}/{}/bench-attachments/{key}",
        entry.tenant, entry.id
    );
    let chunks: Vec<_> = bytes
        .chunks(4096)
        .map(|b| Ok::<_, std::io::Error>(bytes::Bytes::copy_from_slice(b)))
        .collect();
    let response = client
        .put(format!(
            "{}{path}",
            config.urls[entry.tenant % config.urls.len()]
        ))
        .bearer_auth(&config.token)
        .header("x-content-sha256", &checksum)
        .body(reqwest::Body::wrap_stream(stream::iter(chunks)))
        .send()
        .await?;
    ensure!(
        response.status().is_success(),
        "attachment write HTTP {}",
        response.status()
    );
    let value: Value = response.json().await?;
    ensure!(
        value["object"]["size"] == bytes.len()
            && value["object"]["checksums"]["sha256"] == checksum,
        "protocol: attachment checksum/size changed"
    );
    let object_key = value["object"]["key"]
        .as_str()
        .context("object key absent")?;
    let version = &value["object"]["version"];
    let receipt: Receipt = serde_json::from_value(value["reference"]["receipt"].clone())?;
    let command = Command {
        command_id: format!(
            "attachment-{:x}",
            Sha256::digest(
                format!(
                    "{object_key}:{}",
                    version.as_str().context("object version absent")?
                )
                .as_bytes()
            )
        ),
        action: "attach".into(),
        payload: json!({"order_id":"bench-attachments","key":object_key,
            "sha256":checksum,"bytes":bytes.len(),"version":version}),
        expected_version: None,
    };
    ensure!(
        receipt.accepted
            && receipt.command_id == command.command_id
            && receipt.digest == command.digest(&entry.scope)?,
        "protocol: invalid attachment reference acknowledgement"
    );
    append(
        writer,
        &Attempt {
            phase: config.phase.clone(),
            index: 0,
            operation: "attachment_reference".into(),
            path: entry.path.clone(),
            ingress: 0,
            scheduled_us: 0,
            sent_us: Some(0),
            finished_us: 0,
            status: "ok".into(),
            bytes: bytes.len(),
            command: Some(command),
            receipt: Some(receipt),
            scope: Some(entry.scope.clone()),
            owner: None,
        },
    )
    .await?;
    Ok(value)
}

pub(super) async fn read(
    client: &Client,
    config: &Config,
    entry: &Entry,
    key: &str,
    bytes: &[u8],
) -> Result<usize> {
    let response = client
        .get(format!(
            "{}/attachments/{}/{}/bench-attachments/{key}",
            config.urls[entry.tenant % config.urls.len()],
            entry.tenant,
            entry.id
        ))
        .bearer_auth(&config.token)
        .send()
        .await?;
    ensure!(
        response.status() == 200,
        "attachment read HTTP {}",
        response.status()
    );
    let actual = response.bytes().await?;
    ensure!(
        actual.as_ref() == bytes,
        "protocol: attachment bytes changed"
    );
    Ok(actual.len())
}

pub(super) async fn prepare(
    client: &Client,
    config: &Config,
    manifest: &Manifest,
) -> Result<Value> {
    let writer = Mutex::new(BufWriter::new(
        tokio::fs::File::create(&config.ledger).await?,
    ));
    let bytes = vec![42; config.bytes];
    let mut count = 0;
    for entry in manifest
        .cells
        .iter()
        .filter(|e| e.kind == "books" && e.id == 0)
    {
        let receipt = super::checks::command(
            client,
            config,
            &writer,
            &entry.path,
            Command {
                command_id: "attachment-fixture-v1".into(),
                action: "create".into(),
                payload: json!({"order_id":"bench-attachments","inventory_id":0,"saga":false}),
                expected_version: None,
            },
        )
        .await?;
        ensure!(receipt.accepted, "attachment order fixture rejected");
        upload(client, config, &writer, entry, "fixture", &bytes).await?;
        read(client, config, entry, "fixture", &bytes).await?;
        count += 1;
    }
    writer.lock().await.flush().await?;
    Ok(json!({"action":"attachment_seed","orders":count,"bytes":bytes.len()}))
}

pub(super) async fn verify(
    client: &Client,
    config: &Config,
    writer: &Mutex<BufWriter<tokio::fs::File>>,
) -> Result<Value> {
    let manifest: Manifest = serde_json::from_slice(&tokio::fs::read(&config.manifest).await?)?;
    let entry = manifest
        .cells
        .iter()
        .find(|e| e.kind == "books" && e.tenant == 0 && e.id == 0)
        .context("attachment book absent")?;
    let bytes = vec![42; config.bytes];
    let first = upload(client, config, writer, entry, "probe", &bytes).await?;
    let second = upload(client, config, writer, entry, "probe", &bytes).await?;
    ensure!(
        first["reference"]["receipt"] == second["reference"]["receipt"]
            && second["reference"]["replay"] == true,
        "attachment replay created another reference"
    );
    read(client, config, entry, "probe", &bytes).await?;
    let url = format!("{}/attachments/0/0/bench-attachments/probe", config.urls[0]);
    let head = client.head(&url).bearer_auth(&config.token).send().await?;
    ensure!(
        head.status() == 200 && head.headers()["content-length"] == config.bytes.to_string(),
        "attachment HEAD metadata failed"
    );
    let range = client
        .get(&url)
        .bearer_auth(&config.token)
        .header("range", "bytes=0-15")
        .send()
        .await?;
    ensure!(
        range.status() == 206 && range.bytes().await?.as_ref() == &bytes[..16.min(bytes.len())],
        "attachment range failed"
    );
    let changed = client
        .put(&url)
        .bearer_auth(&config.token)
        .header("x-content-sha256", "00".repeat(32))
        .body(bytes)
        .send()
        .await?;
    ensure!(
        !changed.status().is_success(),
        "attachment key accepted different checksum"
    );
    Ok(
        json!({"case":"committed_attachment","passed":["stream_checksum_reference_replay_head_range_conflict"]}),
    )
}
