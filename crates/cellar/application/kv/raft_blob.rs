// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! KV's external bytes follow a proven Raft source turn and its original
//! deadline. The node-owned namespace capability replaces Fleet blob epochs.

use crate::application::{consensus::CommandFailure, Context, HandlerFuture};
use crate::kv_blob::BlobReply;
use crate::raft::cell_groups::CellGroupPayloadReference;
use anyhow::{ensure, Context as _, Result};
use serde_json::{json, Value};

enum Request {
    Get(CellGroupPayloadReference),
    Put(CellGroupPayloadReference, bytes::Bytes),
}

pub(crate) fn rpc(
    context: &Context,
    request: Value,
    value: Option<Vec<u8>>,
) -> HandlerFuture<BlobReply> {
    let prepared = (|| -> Result<HandlerFuture<BlobReply>> {
        context.check_binding_turn()?;
        let identity = context.identity().context("KV payload requires a cell")?;
        ensure!(
            identity.class == crate::deploy::KV_CLASS,
            CommandFailure::UnsupportedCapability
        );
        let dispatch = crate::storage::capture_raft_dispatch()?
            .ok_or(CommandFailure::UnsupportedCapability)?;
        let source = dispatch.binding_source()?;
        ensure!(
            source.history.cell.as_str() == identity.scope,
            CommandFailure::UnsupportedCapability
        );
        let store = source
            .host
            .kv_payload_store(&source.history, source.configuration)?;
        let mode = request["mode"].as_str().context("KV payload mode")?;
        if mode == "prepare" {
            ensure!(value.is_none(), "KV preparation received bytes");
            let mut digest = [0; 32];
            hex::decode_to_slice(
                request["digest"].as_str().context("KV payload digest")?,
                &mut digest,
            )?;
            let reference = store.prepare_reference(
                &source.history,
                digest,
                request["length"].as_u64().context("KV payload length")?,
            )?;
            let wire = super::raft_wire::prepared(&source.history, reference.clone())?;
            let reply = BlobReply::Json(
                json!({"reference":serde_json::to_string(&reference)?,"wire":wire}),
            );
            return Ok(Box::pin(async move { Ok(reply) }));
        }
        let encoded = request["reference"]
            .as_str()
            .context("KV payload reference")?;
        ensure!(encoded.len() <= 1024, CommandFailure::CorruptState);
        let reference = serde_json::from_str(encoded)?;
        let request = match mode {
            "get" => {
                ensure!(value.is_none(), "KV read received bytes");
                Request::Get(reference)
            }
            "put" => Request::Put(reference, value.context("KV payload bytes")?.into()),
            _ => anyhow::bail!(CommandFailure::UnsupportedCapability),
        };
        let dependency = match &request {
            Request::Get(reference) | Request::Put(reference, _) => reference.clone(),
        };
        let history = source.history.clone();
        let parent = source.parent;
        let guard = crate::storage::raft_payloads::record(
            parent,
            &history,
            history.clone(),
            dependency.clone(),
            false,
        )?;
        let source_history = history.clone();
        let call = dispatch.schedule_io(move |timeout| async move {
            let deadline = crate::asyncrt::mono_ms()
                .checked_add(timeout.as_millis().try_into()?)
                .ok_or(CommandFailure::Overloaded)?;
            let (reference, upload) = match &request {
                Request::Get(reference) => (reference, false),
                Request::Put(reference, _) => (reference, true),
            };
            store
                .protect_command(&source.history, reference, guard, upload, timeout)
                .await?;
            let timeout = std::time::Duration::from_millis(
                deadline
                    .checked_sub(crate::asyncrt::mono_ms())
                    .filter(|n| *n != 0)
                    .ok_or(crate::application::consensus::RuntimeError::DeadlineExceeded)?,
            );
            match request {
                Request::Get(reference) => Ok::<_, anyhow::Error>(BlobReply::GroupBytes(
                    store
                        .get_with_retained_copies(
                            &source.history,
                            &reference,
                            timeout,
                            super::raft_wire::DECODE_COPIES,
                        )
                        .await?,
                )),
                Request::Put(reference, bytes) => {
                    store
                        .publish_prepared(&source.history, reference, bytes, timeout)
                        .await?;
                    Ok(BlobReply::Json(json!({"ok":true})))
                }
            }
        });
        Ok(Box::pin(async move {
            let reply = call.future.await??;
            crate::storage::raft_payloads::record(
                parent,
                &source_history,
                history,
                dependency,
                true,
            )?;
            Ok(reply)
        }))
    })();
    Box::pin(async move { prepared?.await })
}
