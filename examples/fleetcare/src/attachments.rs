// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Object transfer and the authoritative order reference are separate commits.
//! A repeated upload verifies the existing object's checksum before replaying
//! the same cell command; an interrupted transfer leaves no committed reference.
use crate::{
    application::{response, stub},
    config::AppConfig,
    contracts::Command,
};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{
    blob::{self, Algorithm, Checksum},
    clone::StructuredValue,
    Context, Output, Request,
};
use futures_util::StreamExt;
use serde_json::json;
use sha2::{Digest, Sha256};

pub async fn handle(context: &Context, config: &AppConfig, request: Request) -> Result<Output> {
    let url = url::Url::parse(&request.url)?;
    let parts: Vec<_> = url.path().trim_matches('/').split('/').collect();
    ensure!(parts.len() == 5, "use /attachments/TENANT/BOOK/ORDER/KEY");
    let tenant = parts[1].parse()?;
    let book = parts[2].parse()?;
    config.validate_target(tenant, "books", book)?;
    for text in &parts[3..] {
        ensure!(
            !text.is_empty()
                && text.len() <= 128
                && text
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)),
            "invalid attachment identity"
        );
    }
    let key = format!("tenant-{tenant}/book-{book}/{}/{}", parts[3], parts[4]);
    let bucket = context.blob("ATTACHMENTS")?;
    match request.method.as_str() {
        "PUT" => {
            let checksum = request
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("x-content-sha256"))
                .map(|(_, v)| v.to_ascii_lowercase())
                .context("attachment requires SHA256")?;
            ensure!(
                checksum.len() == 64 && checksum.bytes().all(|b| b.is_ascii_hexdigit()),
                "invalid SHA256"
            );
            let options = blob::WriteOptions {
                checksums: [(Algorithm::Sha256, Checksum::Hex(checksum.clone()))].into(),
                custom_metadata: [
                    ("order".into(), parts[3].into()),
                    ("tenant".into(), tenant.to_string()),
                ]
                .into(),
                only_if: Some(blob::Conditional {
                    etag_does_not_match: Some("*".into()),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let stream = context
                .take_body(request.body)?
                .scan(0usize, |size, chunk| {
                    let chunk = chunk.and_then(|chunk| {
                        *size = size.saturating_add(chunk.len());
                        if *size > 32 * 1024 * 1024 {
                            Err("attachment exceeds 32 MiB".into())
                        } else {
                            Ok(chunk)
                        }
                    });
                    futures_util::future::ready(Some(chunk))
                });
            let object = match bucket.put_stream(context, &key, stream, options).await? {
                Some(object) => object,
                None => bucket
                    .head(context, &key)
                    .await?
                    .context("conditional upload has no existing object")?,
            };
            ensure!(
                object.checksums.get("sha256") == Some(&checksum),
                "attachment key reused with other content"
            );
            let command = Command {
                command_id: format!(
                    "attachment-{:x}",
                    Sha256::digest(format!("{key}:{}", object.version).as_bytes())
                ),
                action: "attach".into(),
                payload: json!({"order_id": parts[3], "key": key,
                    "sha256": checksum, "bytes": object.size, "version": object.version}),
                expected_version: None,
            };
            let data = cellar::sdk::encode_rpc(&StructuredValue::from_json(
                serde_json::to_value(command)?,
            )?)?;
            let reply = stub(context, tenant, "books", book)?
                .rpc(context, "command", data)
                .await?;
            let receipt = cellar::sdk::decode_rpc_result(&reply)?.to_json()?;
            ensure!(
                receipt["receipt"]["accepted"] == true,
                "attachment reference rejected"
            );
            response(context, json!({"object": object, "reference": receipt}))
        }
        "GET" => {
            let range = request
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("range"))
                .map(|(_, v)| blob::Range::from_header(v))
                .transpose()?;
            let blob::GetResult::Body(object) = bucket
                .get(
                    context,
                    &key,
                    blob::GetOptions {
                        range: range.clone(),
                        ..Default::default()
                    },
                )
                .await?
            else {
                anyhow::bail!("attachment absent or conditional read failed");
            };
            Ok(Output::Http(object.into_response(
                context,
                if range.is_some() { 206 } else { 200 },
                Vec::new(),
            )?))
        }
        "HEAD" => {
            let object = bucket
                .head(context, &key)
                .await?
                .context("attachment absent")?;
            Ok(Output::Http(
                cellar::sdk::http::Response::new(
                    200,
                    Vec::<u8>::new(),
                    vec![
                        ("content-length".into(), object.size.to_string()),
                        ("etag".into(), object.http_etag),
                        (
                            "x-content-sha256".into(),
                            object.checksums.get("sha256").cloned().unwrap_or_default(),
                        ),
                    ],
                )
                .into_http(context)?,
            ))
        }
        _ => anyhow::bail!("use PUT, GET or HEAD for committed attachments"),
    }
}
