// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Framework-only owner sampling; never initializes an unused filesystem.
use crate::application::{Context, Invocation, Output};
use crate::filesystem_state::FilesystemState;
use anyhow::{ensure, Context as _, Result};

pub(crate) const METHOD: &str = "__filesystemState";
pub(crate) const MAX_REPLY_BYTES: usize = 16 << 10;

fn sample(context: &Context) -> Result<FilesystemState> {
    let identity = context
        .identity()
        .context("filesystem observation has no cell")?;
    let storage = context.storage()?;
    // Recheck the installed storage authority without acquiring/initializing fs.
    let scope = storage.scope()?;
    let activation = context
        .io
        .native_activation
        .as_ref()
        .context("filesystem observation has no activation")?;
    let (handles, pins) = activation.resources.filesystem.observation()?;
    let quotas = crate::application::limits::current().filesystem.into();
    let enabled = context
        .worker_config
        .native_application
        .as_ref()
        .and_then(|application| application.filesystem_policy(&identity.class))
        .is_some();
    let result = FilesystemState {
        format: 1,
        scope: scope.into(),
        epoch: identity.epoch,
        sampled_ms: crate::asyncrt::wall_ms().max(0) as u64,
        enabled,
        quotas,
        usage: crate::storage::filesystem::inspect(scope)?,
        live_handles: handles as u64,
        pinned_inodes: pins.len() as u64,
    };
    result.validate(scope)?;
    Ok(result)
}

pub(crate) async fn invoke(
    context: Context,
    invocation: Invocation,
    authority: super::rpc_authority::StateAuthority,
) -> Result<Output> {
    context.check_turn()?;
    let Invocation::CellRpc { method, args } = invocation else {
        anyhow::bail!("filesystem observation requires RPC")
    };
    ensure!(
        authority.is_filesystem(),
        "filesystem observation requires its own authority"
    );
    authority.validate_target(
        &context
            .identity()
            .context("filesystem observation has no cell")?
            .scope,
        &method,
    )?;
    let bytes = args.payload_bytes();
    ensure!(
        bytes <= 1024 && crate::application::decode_rpc(&args)?.to_json()? == serde_json::json!({}),
        "filesystem observation requires an empty request"
    );
    let value = match sample(&context) {
        Ok(sample) => serde_json::json!({"result":sample}),
        Err(_) => {
            serde_json::json!({"error":{"code":"Unavailable","message":"filesystem observation unavailable"}})
        }
    };
    crate::application::event_stream::encoding::json(&value, MAX_REPLY_BYTES)?;
    Ok(Output::Rpc(crate::application::encode_rpc(
        &cellar_native::clone::StructuredValue::from_json(value)?,
    )?))
}
