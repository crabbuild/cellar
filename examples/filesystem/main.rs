// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A bounded per-cell notebook: append, business count and receipt commit together.

use crate::support;
use anyhow::{Context as _, Result};
use cellar::sdk::filesystem::{Errno, OpenOptions};
use cellar::sdk::{Cell, Context, HandlerFuture, Invocation, Output, Storage};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

const PATH: &str = "/notes.txt";
const MAX_ENTRIES: u64 = 128;
const MAX_ENTRY_BYTES: usize = 4096;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Append {
    id: String,
    text: String,
}

#[derive(Deserialize, Serialize)]
struct Receipt {
    id: String,
    entries: u64,
    size: u64,
    inode: String,
    version: String,
}

#[derive(Deserialize, Serialize)]
struct Saved {
    text: String,
    receipt: Receipt,
}

fn entries(storage: &Storage) -> Result<u64> {
    storage
        .get_json("entries")?
        .map(serde_json::from_value)
        .transpose()
        .context("invalid notebook count")
        .map(|count| count.unwrap_or(0))
}

pub struct Notebook;

impl Cell for Notebook {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            support::method(&request, &["GET", "POST"])?;
            let storage = context.storage()?;
            if request.method == "GET" {
                let fs = context.fs()?;
                let count = entries(&storage)?;
                let (text, file) = match fs.stat(PATH) {
                    Ok(stat) => (
                        String::from_utf8(fs.read_file(PATH)?)?,
                        json!({"inode":stat.inode.to_string(),"version":stat.version.to_string(),"size":stat.size}),
                    ),
                    Err(error) if error.errno == Errno::ENOENT && count == 0 => {
                        (String::new(), serde_json::Value::Null)
                    }
                    Err(error) => return Err(error.into()),
                };
                return support::json(&context, json!({"entries":count,"text":text,"file":file}));
            }
            // HTTP input has a separate 64 KiB bound in the shared reader.
            let command: Append =
                serde_json::from_value(support::payload(&context, request).await?)
                    .map_err(|_| support::reject(400, "expected only id and text strings"))?;
            support::name(&command.id)?;
            if command.text.is_empty() || command.text.len() > MAX_ENTRY_BYTES {
                return Err(support::reject(
                    413,
                    "text must contain 1..4096 UTF-8 bytes",
                ));
            }
            let key = format!("receipt:{}", command.id);
            let bytes = format!("{}\n", command.text).into_bytes();
            let receipt = storage.tx(|tx| {
                if let Some(saved) = tx.get_json(&key)? {
                    let saved: Saved = serde_json::from_value(saved)?;
                    if saved.text != command.text {
                        return Err(support::reject(409, "id already belongs to different text"));
                    }
                    return Ok(saved.receipt);
                }
                let count = entries(tx)?;
                if count >= MAX_ENTRIES {
                    return Err(support::reject(
                        413,
                        "notebook has reached its 128-entry limit",
                    ));
                }
                // This state write rolls back if any later file/receipt operation fails.
                tx.put_json("entries", &json!(count + 1))?;
                let file = tx.fs()?.open(
                    PATH,
                    OpenOptions::write_only()
                        .create(true)
                        .append(true)
                        .mode(0o600),
                )?;
                file.write_all(&bytes)?;
                let stat = file.fstat()?;
                file.close()?;
                let saved = Saved {
                    text: command.text.clone(),
                    receipt: Receipt {
                        id: command.id.clone(),
                        entries: count + 1,
                        size: stat.size,
                        inode: stat.inode.to_string(),
                        version: stat.version.to_string(),
                    },
                };
                tx.put_json(&key, &serde_json::to_value(&saved)?)?;
                Ok(saved.receipt)
            })?;
            // Request the configured durability proof only after the transaction closes.
            // A lost reply is reconciled by resubmitting the same id and text.
            context.fs()?.sync().await?;
            support::json(&context, serde_json::to_value(receipt)?)
        })
    }
}
