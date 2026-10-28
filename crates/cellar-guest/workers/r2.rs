// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native Blob operations needed by the published directory-backup gateway.
//! Buffered upload bytes retain the adapter's capacity through backend work.

use crate::{failure::Validation, transport, Limits};
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::{blob, Context};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};

#[path = "r2_parts.rs"]
mod parts;
pub(super) use parts::Parts;

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Input {
    PartBegin {
        binding: String,
        key: String,
        upload_id: String,
        number: u32,
        length: u64,
    },
    PartWrite {
        id: String,
        body: String,
    },
    PartFinish {
        id: String,
    },
    PartCancel {
        id: String,
    },
    Head {
        binding: String,
        key: String,
    },
    Get {
        binding: String,
        key: String,
        range: Option<Range>,
    },
    Delete {
        binding: String,
        key: String,
    },
    Create {
        binding: String,
        key: String,
        #[serde(default)]
        options: Metadata,
    },
    Part {
        binding: String,
        key: String,
        upload_id: String,
        number: u32,
        body: String,
    },
    Complete {
        binding: String,
        key: String,
        upload_id: String,
        parts: Vec<Receipt>,
    },
    Abort {
        binding: String,
        key: String,
        upload_id: String,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Range {
    offset: Option<u64>,
    length: Option<u64>,
    suffix: Option<u64>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
struct Metadata {
    http_metadata: BTreeMap<String, Value>,
    custom_metadata: BTreeMap<String, String>,
}

impl Metadata {
    fn options(self) -> Result<blob::WriteOptions> {
        ensure!(
            self.http_metadata.keys().all(|name| matches!(
                name.as_str(),
                "contentType"
                    | "contentLanguage"
                    | "contentDisposition"
                    | "contentEncoding"
                    | "cacheControl"
                    | "cacheExpiry"
            )),
            "unsupported R2 HTTP metadata"
        );
        Ok(blob::WriteOptions {
            http_metadata: serde_json::from_value(serde_json::to_value(self.http_metadata)?)?,
            custom_metadata: self.custom_metadata,
            ..Default::default()
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Receipt {
    part_number: u32,
    etag: String,
}

// Bytes::from_owner follows clones of the real payload into object_store and
// the local SQLite blocking worker. Dropping the awaiting event cannot release
// the host's reservation while that worker still holds the decoded bytes.
struct PartBytes {
    bytes: Vec<u8>,
    _capacity: Arc<dyn Send + Sync>,
}

impl AsRef<[u8]> for PartBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

fn bucket(context: &Context, binding: &str, key: &str) -> Result<blob::Bucket> {
    ensure!(!key.is_empty() && key.len() <= 1024, "invalid R2 key");
    context.blob(binding)
}

fn parse(name: &str, input: Value) -> Result<Input> {
    let Value::Object(mut input) = input else {
        anyhow::bail!("R2 requires an object")
    };
    ensure!(!input.contains_key("kind"), "unexpected R2 kind");
    input.insert(
        "kind".into(),
        json!(name
            .strip_prefix("workers.r2.")
            .context("invalid R2 operation")?),
    );
    Ok(serde_json::from_value(Value::Object(input))?)
}

pub(super) async fn execute(
    context: &Context,
    name: &str,
    input: Value,
    limits: &Limits,
    transport: &transport::Session,
    parts: &Parts,
    reserve: impl FnOnce(usize) -> Result<Arc<dyn Send + Sync>>,
) -> Result<Value> {
    match parse(name, input).context(Validation)? {
        Input::PartBegin {
            binding,
            key,
            upload_id,
            number,
            length,
        } => {
            let upload = bucket(context, &binding, &key)
                .context(Validation)?
                .resume_resumable_multipart(&key, &upload_id)
                .context(Validation)?;
            let id = parts
                .begin(context, upload, number, length, limits.max_batch_operations)
                .await?;
            Ok(json!({"id": id, "max_chunk_bytes": limits.max_body_bytes.min(64 * 1024)}))
        }
        Input::PartWrite { id, body } => {
            let bytes = part_bytes(body, limits.max_body_bytes.min(64 * 1024), reserve)
                .context(Validation)?;
            parts.write(context, &id, bytes).await?;
            Ok(Value::Null)
        }
        Input::PartFinish { id } => {
            let receipt = parts.complete(context, &id).await?;
            Ok(json!({"partNumber": receipt.part_number, "etag": receipt.etag}))
        }
        Input::PartCancel { id } => {
            parts.cancel(&id).await;
            Ok(Value::Null)
        }
        Input::Head { binding, key } => {
            let bucket = bucket(context, &binding, &key).context(Validation)?;
            Ok(serde_json::to_value(bucket.head(context, &key).await?)?)
        }
        Input::Get {
            binding,
            key,
            range,
        } => {
            ensure!(
                range.as_ref().is_none_or(|range| range.suffix.is_none()
                    || (range.offset.is_none() && range.length.is_none())),
                Validation
            );
            let bucket = bucket(context, &binding, &key).context(Validation)?;
            transport.response_capacity(context, limits)?;
            let result = bucket
                .get(
                    context,
                    &key,
                    blob::GetOptions {
                        range: range.map(|range| blob::Range {
                            offset: range.offset,
                            length: range.length,
                            suffix: range.suffix,
                        }),
                        ..Default::default()
                    },
                )
                .await?;
            match result {
                blob::GetResult::Missing => Ok(Value::Null),
                blob::GetResult::Unmet(_) => anyhow::bail!("unexpected R2 precondition result"),
                blob::GetResult::Body(body) => {
                    let metadata = serde_json::to_value(&body.object)?;
                    let response = body.into_response(context, 200, Vec::new())?;
                    let frame = transport.retain_response(context, response, limits)?;
                    Ok(json!({"object":metadata,"response":frame}))
                }
            }
        }
        Input::Delete { binding, key } => {
            bucket(context, &binding, &key)
                .context(Validation)?
                .delete(context, &[key])
                .await?;
            Ok(Value::Null)
        }
        Input::Create {
            binding,
            key,
            options,
        } => {
            let (bucket, options) = (|| -> Result<_> {
                let bucket = bucket(context, &binding, &key)?;
                ensure!(
                    bucket.supports_resumable_multipart()?,
                    "R2 multipart metadata is unavailable"
                );
                Ok((bucket, options.options()?))
            })()
            .context(Validation)?;
            let upload = bucket
                .create_resumable_multipart(context, &key, options)
                .await?;
            Ok(json!({"key":key,"uploadId":upload.upload_id()}))
        }
        Input::Part {
            binding,
            key,
            upload_id,
            number,
            body,
        } => {
            let (upload, bytes) = (|| -> Result<_> {
                ensure!((1..=10_000).contains(&number), "invalid R2 part number");
                let upload = bucket(context, &binding, &key)?
                    .resume_resumable_multipart(&key, &upload_id)?;
                let bound = limits
                    .max_body_bytes
                    .checked_add(2)
                    .context("R2 body limit overflow")?
                    / 3;
                ensure!(
                    body.len() <= bound.checked_mul(4).context("R2 body limit overflow")?,
                    "R2 part exceeds body limit"
                );
                // Reserve before decoding. The two padding bytes are also charged
                // until the payload is released; malformed base64 never enters I/O.
                let capacity = reserve(
                    body.len()
                        .div_ceil(4)
                        .checked_mul(3)
                        .context("R2 capacity overflow")?,
                )?;
                let bytes = STANDARD.decode(body)?;
                ensure!(
                    bytes.len() <= limits.max_body_bytes,
                    "R2 part exceeds body limit"
                );
                Ok((
                    upload,
                    bytes::Bytes::from_owner(PartBytes {
                        bytes,
                        _capacity: capacity,
                    }),
                ))
            })()
            .context(Validation)?;
            let part = upload.upload_part_bytes(context, number, bytes).await?;
            Ok(json!({"partNumber":part.part_number,"etag":part.etag}))
        }
        Input::Complete {
            binding,
            key,
            upload_id,
            parts,
        } => {
            let (upload, parts) = (|| -> Result<_> {
                let upload = bucket(context, &binding, &key)?
                    .resume_resumable_multipart(&key, &upload_id)?;
                ensure!(
                    !parts.is_empty() && parts.len() <= 10_000,
                    "invalid R2 completion parts"
                );
                for (index, part) in parts.iter().enumerate() {
                    ensure!(
                        part.part_number as usize == index + 1
                            && !part.etag.is_empty()
                            && part.etag.len() <= 16_384,
                        "R2 completion needs consecutive current receipts"
                    );
                }
                Ok((
                    upload,
                    parts
                        .into_iter()
                        .map(|part| blob::UploadedPart {
                            part_number: part.part_number,
                            etag: part.etag,
                        })
                        .collect::<Vec<_>>(),
                ))
            })()
            .context(Validation)?;
            Ok(serde_json::to_value(
                upload.complete(context, &parts).await?,
            )?)
        }
        Input::Abort {
            binding,
            key,
            upload_id,
        } => {
            let upload = bucket(context, &binding, &key)
                .and_then(|bucket| bucket.resume_resumable_multipart(&key, &upload_id))
                .context(Validation)?;
            upload.abort(context).await?;
            Ok(Value::Null)
        }
    }
}

fn part_bytes(
    body: String,
    maximum: usize,
    reserve: impl FnOnce(usize) -> Result<Arc<dyn Send + Sync>>,
) -> Result<bytes::Bytes> {
    let bound = maximum.checked_add(2).context("R2 body limit overflow")? / 3;
    ensure!(
        body.len() <= bound.checked_mul(4).context("R2 body limit overflow")?,
        "R2 chunk exceeds body limit"
    );
    let capacity = reserve(
        body.len()
            .div_ceil(4)
            .checked_mul(3)
            .context("R2 capacity overflow")?,
    )?;
    let bytes = STANDARD.decode(body)?;
    ensure!(bytes.len() <= maximum, "R2 chunk exceeds body limit");
    Ok(bytes::Bytes::from_owner(PartBytes {
        bytes,
        _capacity: capacity,
    }))
}

#[cfg(test)]
#[path = "r2_tests.rs"]
mod tests;
