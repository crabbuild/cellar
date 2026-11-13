// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Large KV arguments and replies remain immutable external bytes. Raft logs
//! retain compact descriptors rather than exceeding command/receipt envelopes.
//! Prepared locators are pending upload intents, not published dependencies.

use super::*;
use crate::application::consensus::{native_rpc, CellHistory, CommandFailure, IncarnationId};
use crate::raft::cell_groups::{
    CellGroupPayloadBytes, CellGroupPayloadReference, CellGroupPayloadStore,
};
use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{future::Future, task::Poll, time::Duration};

pub(crate) const PREPARE: &str = "__kvPrepareWire";
pub(crate) const EXTERNAL: &str = "__kvExternalInput";
pub(crate) const MAX_BYTES: usize = crate::raft::cell_groups::MAX_CELL_GROUP_PAYLOAD_BYTES;
pub(crate) const DECODE_COPIES: usize = 6;
pub(crate) const MIN_RETAINED_BYTES: usize = MAX_BYTES * (DECODE_COPIES + 3) + 128 * 1024;
const INPUT: &str = "__cellarKvInput";
const REPLY: &str = "__cellarKvReply";

pub(super) struct Reply {
    value: StructuredValue,
    _retained: Option<CellGroupPayloadBytes>,
    deadline: Option<u64>,
}

impl Reply {
    pub(super) fn deadline(&self) -> Result<u64> {
        self.deadline
            .ok_or_else(|| CommandFailure::UnsupportedCapability.into())
    }

    pub(super) fn check(&self, context: &Context) -> Result<()> {
        if let Some(deadline) = self.deadline {
            remaining(deadline)?;
            context.check_binding_turn()?;
        }
        Ok(())
    }

    pub(super) fn inline(value: StructuredValue) -> Self {
        Self {
            value,
            _retained: None,
            deadline: None,
        }
    }
}

impl std::ops::Deref for Reply {
    type Target = StructuredValue;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reference {
    format: u8,
    published: bool,
    cluster: [u8; 16],
    namespace: [u8; 16],
    cell: String,
    incarnation: [u8; 16],
    object: CellGroupPayloadReference,
}

impl Reference {
    fn pending(history: &CellHistory, object: CellGroupPayloadReference) -> Self {
        Self {
            format: 1,
            published: false,
            cluster: history.cluster.bytes(),
            namespace: history.namespace.bytes(),
            cell: history.cell.as_str().into(),
            incarnation: history.incarnation.bytes(),
            object,
        }
    }

    fn parse(encoded: &str) -> Result<Self> {
        ensure!(
            encoded.len() <= 4096,
            "KV_ERROR: external descriptor exceeds bounds"
        );
        let reference: Self = serde_json::from_str(encoded)?;
        ensure!(
            reference.format == 1,
            "KV_ERROR: external descriptor format"
        );
        reference.history()?;
        Ok(reference)
    }

    fn history(&self) -> Result<CellHistory> {
        Ok(CellHistory {
            cluster: super::super::consensus::ClusterId::new(self.cluster)?,
            namespace: super::super::consensus::NamespaceId::new(self.namespace)?,
            cell: super::super::consensus::CellId::new(&self.cell)?,
            incarnation: IncarnationId::new(self.incarnation)?,
        })
    }

    fn matches(&self, history: &CellHistory) -> Result<()> {
        // Immutable objects survive movement and a separately fenced recovery.
        // This checks the logical owner, never treats a locator as authority.
        ensure!(
            self.cluster == history.cluster.bytes()
                && self.namespace == history.namespace.bytes()
                && self.cell == history.cell.as_str(),
            "KV_ERROR: external logical owner differs"
        );
        Ok(())
    }
}

pub(crate) fn prepared(history: &CellHistory, object: CellGroupPayloadReference) -> Result<String> {
    Ok(serde_json::to_string(&Reference::pending(history, object))?)
}

pub(super) fn enabled(context: &Context) -> bool {
    context.raft_storage.is_some() || context.state.raft_kv
}

pub(super) fn deadline(context: &Context) -> Result<u64> {
    context.check_binding_turn()?;
    let timeout = if context.raft_storage.is_some() {
        crate::storage::capture_raft_dispatch()?
            .ok_or(CommandFailure::UnsupportedCapability)?
            .remaining_timeout()?
    } else {
        Duration::from_millis(crate::asyncrt::services().operation_deadline_ms()?)
    };
    let deadline = crate::asyncrt::mono_ms()
        .checked_add(u64::try_from(timeout.as_millis())?)
        .ok_or(super::super::consensus::RuntimeError::DeadlineExceeded)?;
    Ok(context
        .io
        .rpc_deadline_ms
        .get()
        .map_or(deadline, |parent| deadline.min(*parent)))
}

pub(super) fn remaining(deadline: u64) -> Result<Duration> {
    Ok(Duration::from_millis(
        deadline
            .checked_sub(crate::asyncrt::mono_ms())
            .filter(|ms| *ms != 0)
            .ok_or(super::super::consensus::RuntimeError::DeadlineExceeded)?,
    ))
}

fn raw(
    context: &Context,
    identity: &str,
    method: &str,
    args: RpcData,
    deadline: u64,
) -> HandlerFuture<RpcData> {
    crate::native_worker::rpc_cell(
        context,
        policy_scope(identity),
        Some(policy::cell_name(identity, 0)),
        method.into(),
        args,
        crate::host_rpc::CallScope::in_context(context, Some(deadline), None),
    )
}

fn public_method(method: &str) -> bool {
    matches!(
        method,
        "__kvGet" | "__kvPut" | "__kvDelete" | "__kvList" | "__kvMetrics" | "__kvSweep"
    )
}

pub(super) async fn call(
    context: &Context,
    identity: &str,
    method: &str,
    args: RpcData,
) -> Result<Reply> {
    call_at(context, identity, method, args, deadline(context)?).await
}

pub(super) async fn call_at(
    context: &Context,
    identity: &str,
    method: &str,
    args: RpcData,
    deadline: u64,
) -> Result<Reply> {
    ensure!(public_method(method), CommandFailure::UnsupportedCapability);
    remaining(deadline)?;
    context.check_binding_turn()?;
    let scope = policy_scope(identity);
    let name = policy::cell_name(identity, 0);
    let limit = if context.raft_storage.is_some() {
        let dispatch = crate::storage::capture_raft_dispatch()?
            .ok_or(CommandFailure::UnsupportedCapability)?;
        let source = dispatch.binding_source()?;
        source
            .host
            .route(&source.history, source.configuration, &scope, method)?
            .max_input_bytes
    } else {
        crate::asyncrt::services()
            .raft_lifecycle()
            .application_service()?
            .kv_wire_input_limit(&scope, method)?
    };
    let request = native_rpc::encode_request(Some(&name), &args, MAX_BYTES)?;
    let (method, args) = if request.len() > limit {
        drop(args);
        let reference = publish_bytes(context, identity, request.into(), deadline).await?;
        (
            EXTERNAL,
            encode_rpc(&StructuredValue::from_json(
                json!([{ "method":method,"input":reference }]),
            )?)?,
        )
    } else {
        drop(request);
        (method, args)
    };
    let result = raw(context, identity, method, args, deadline).await?;
    let value = decode_rpc_result(&result)?;
    if let Some(encoded) = external_reply(&value)? {
        let reference = Reference::parse(&encoded)?;
        let Transfer::Bytes(held) = transfer(context, identity, reference, None, deadline).await?
        else {
            bail!(CommandFailure::CorruptState)
        };
        let value = decode_rpc_result(&native_rpc::decode_response(held.as_ref())?)?;
        remaining(deadline)?;
        context.check_binding_turn()?;
        Ok(Reply {
            value,
            _retained: Some(held),
            deadline: Some(deadline),
        })
    } else {
        remaining(deadline)?;
        context.check_binding_turn()?;
        Ok(Reply {
            value,
            _retained: None,
            deadline: Some(deadline),
        })
    }
}

/// Publish caller-owned bytes before the small, independently durable child
/// operation. A publisher flag never substitutes for the receiver's read-back.
pub(super) async fn publish_bytes(
    context: &Context,
    identity: &str,
    bytes: bytes::Bytes,
    deadline: u64,
) -> Result<String> {
    context.check_binding_turn()?;
    remaining(deadline)?;
    ensure!(
        context
            .kv_bindings
            .values()
            .any(|declared| declared == identity),
        CommandFailure::UnsupportedCapability
    );
    let digest: [u8; 32] = Sha256::digest(&bytes).into();
    let scope = policy_scope(identity);
    let observed = if context.raft_storage.is_some() {
        let dispatch = crate::storage::capture_raft_dispatch()?
            .ok_or(CommandFailure::UnsupportedCapability)?;
        let source = dispatch.binding_source()?;
        source.host.prepare_kv_payload(
            &source.history,
            source.configuration,
            &scope,
            digest,
            bytes.len() as u64,
        )?
    } else {
        ensure!(
            context.identity().is_none(),
            CommandFailure::UnsupportedCapability
        );
        crate::asyncrt::services()
            .raft_lifecycle()
            .application_service()?
            .prepare_kv_payload(&scope, digest, bytes.len() as u64)?
    };
    let reference = if let Some((history, object)) = observed {
        Reference::pending(&history, object)
    } else {
        // Cold/remote-only targets retain ordinary durable discovery and
        // enrollment. Only the redundant pure preparation command is elided
        // for a target already present in the bounded applied route metadata.
        let args = encode_rpc(&StructuredValue::from_json(
            json!([{"digest":hex::encode(digest),"length":bytes.len()}]),
        )?)?;
        let prepared = if context.raft_storage.is_some() {
            raw(context, identity, PREPARE, args, deadline).await?
        } else {
            context.io.charge_subrequest().map_err(anyhow::Error::msg)?;
            let owner = crate::asyncrt::services()
                .raft_lifecycle()
                .application_service()?;
            let name = policy::cell_name(identity, 0);
            let admitted = crate::asyncrt::services()
                .admit_outbound_rpc(&scope, Some(&name), PREPARE, &args)
                .ok_or(CommandFailure::Overloaded)?;
            owner
                .prepare_kv_ingress(
                    admitted,
                    crate::host_rpc::CallScope::in_context(context, Some(deadline), None),
                    deadline,
                    digest,
                    bytes.len() as u64,
                )
                .await?
        };
        let prepared = decode_rpc_result(&prepared)?.to_json()?;
        Reference::parse(prepared.as_str().context("KV prepared locator")?)?
    };
    let Transfer::Published(reference) =
        transfer(context, identity, reference, Some(bytes), deadline).await?
    else {
        bail!(CommandFailure::CorruptState)
    };
    Ok(reference)
}

pub(super) fn value_length(encoded: &str) -> Result<usize> {
    let reference = Reference::parse(encoded)?;
    ensure!(
        reference.published,
        "KV_ERROR: unconfirmed value dependency"
    );
    let length = usize::try_from(reference.object.length())?;
    check_value(length)?;
    Ok(length)
}

pub(super) async fn fetch_value(
    context: &Context,
    identity: &str,
    encoded: &str,
    deadline: u64,
) -> Result<CellGroupPayloadBytes> {
    value_length(encoded)?;
    let Transfer::Bytes(bytes) = transfer(
        context,
        identity,
        Reference::parse(encoded)?,
        None,
        deadline,
    )
    .await?
    else {
        bail!(CommandFailure::CorruptState)
    };
    Ok(bytes)
}

/// The receiving quorum command checks complete actual bytes before installing
/// a row. The caller's confirmed-publication descriptor alone grants no proof.
pub(super) async fn verify_value(context: &Context, encoded: &str) -> Result<(String, usize)> {
    let length = value_length(encoded)?;
    let reference = Reference::parse(encoded)?;
    let bytes = read_current(context, encoded).await?;
    ensure!(bytes.as_ref().len() == length, CommandFailure::CorruptState);
    drop(bytes);
    Ok((serde_json::to_string(&reference.object)?, length))
}

/// Only a committed row supplies this locator. The SDK still reads and verifies
/// the immutable body before exposing a value, under the same original deadline.
pub(super) fn published_value(encoded: &str) -> Result<(String, usize)> {
    ensure!(encoded.len() <= 1024, CommandFailure::CorruptState);
    let object: CellGroupPayloadReference = serde_json::from_str(encoded)?;
    let length = usize::try_from(object.length())?;
    check_value(length)?;
    let history = crate::storage::capture_raft_dispatch()?
        .ok_or(CommandFailure::UnsupportedCapability)?
        .source_history()?;
    Ok((
        serde_json::to_string(&Reference {
            format: 1,
            published: true,
            cluster: history.cluster.bytes(),
            namespace: history.namespace.bytes(),
            cell: history.cell.as_str().into(),
            incarnation: history.incarnation.bytes(),
            object,
        })?,
        length,
    ))
}

enum Transfer {
    Published(String),
    Bytes(CellGroupPayloadBytes),
}

async fn transfer(
    context: &Context,
    identity: &str,
    reference: Reference,
    bytes: Option<bytes::Bytes>,
    deadline: u64,
) -> Result<Transfer> {
    context.check_binding_turn()?;
    context.io.charge_subrequest().map_err(anyhow::Error::msg)?;
    ensure!(
        context
            .kv_bindings
            .values()
            .any(|declared| declared == identity),
        CommandFailure::UnsupportedCapability
    );
    ensure!(
        reference.cell == policy_scope(identity),
        "KV_ERROR: external namespace differs"
    );
    let history = reference.history()?;
    if context.raft_storage.is_some() {
        let dispatch = crate::storage::capture_raft_dispatch()?
            .ok_or(CommandFailure::UnsupportedCapability)?
            .limit_source(remaining(deadline)?)?;
        let source = dispatch.binding_source()?;
        let store = source.host.kv_binding_payload_store(
            &source.history,
            source.configuration,
            &history,
        )?;
        dispatch
            .schedule(move |timeout| execute(store, reference, bytes, timeout))
            .future
            .await?
    } else {
        ensure!(
            context.identity().is_none(),
            CommandFailure::UnsupportedCapability
        );
        let owner = crate::asyncrt::services()
            .raft_lifecycle()
            .application_service()?;
        let store = owner.kv_wire_store(&history)?;
        let mut work = Box::pin(execute(store, reference, bytes, remaining(deadline)?));
        crate::asyncrt::timeout_at(
            deadline,
            futures_util::future::poll_fn(|cx| {
                if let Err(error) = context.check_turn().and_then(|()| {
                    ensure!(
                        !owner.is_closed(),
                        super::super::consensus::RuntimeError::Closed
                    );
                    remaining(deadline).map(|_| ())
                }) {
                    return Poll::Ready(Err(error));
                }
                work.as_mut().poll(cx)
            }),
        )
        .await
        .map_err(|_| super::super::consensus::RuntimeError::DeadlineExceeded)?
    }
}

async fn execute(
    store: CellGroupPayloadStore,
    mut reference: Reference,
    bytes: Option<bytes::Bytes>,
    timeout: Duration,
) -> Result<Transfer> {
    let history = reference.history()?;
    match bytes {
        Some(bytes) => {
            ensure!(!reference.published, "KV_ERROR: expected a prepared upload");
            store
                .publish_prepared(&history, reference.object.clone(), bytes, timeout)
                .await?;
            reference.published = true;
            Ok(Transfer::Published(serde_json::to_string(&reference)?))
        }
        None => {
            ensure!(reference.published, "KV_ERROR: unconfirmed wire dependency");
            Ok(Transfer::Bytes(
                store
                    .get_with_retained_copies(&history, &reference.object, timeout, DECODE_COPIES)
                    .await?,
            ))
        }
    }
}

fn external_reply(value: &StructuredValue) -> Result<Option<String>> {
    match wire::field(value, &value.root, REPLY) {
        CloneValue::Undefined => Ok(None),
        value => Ok(Some(wire::string(value)?)),
    }
}

/// Keep a decoded input's body lease through the operation and final result
/// encoding. The private/public operation remains part of the authenticated
/// command contract; a public wrapper cannot tunnel the private operator.
pub(super) async fn unwrap_input(
    context: &Context,
    method: &str,
    args: &RpcData,
) -> Result<(String, RpcData, Option<CellGroupPayloadBytes>)> {
    let value = decode_rpc(args)?;
    let input = if method == EXTERNAL {
        let arguments = wire::array(&value, &value.root)?;
        let body = arguments.first().context("KV external input")?;
        let operation = wire::string(wire::field(&value, body, "method"))?;
        ensure!(
            public_method(&operation),
            CommandFailure::UnsupportedCapability
        );
        Some((operation, wire::string(wire::field(&value, body, "input"))?))
    } else if method == OPERATOR
        && !matches!(
            wire::field(&value, &value.root, INPUT),
            CloneValue::Undefined
        )
    {
        Some((
            method.into(),
            wire::string(wire::field(&value, &value.root, INPUT))?,
        ))
    } else {
        None
    };
    if let Some((method, encoded)) = input {
        let held = read_current(context, &encoded).await?;
        let request = native_rpc::decode_request(held.as_ref())?;
        let identity = context.identity().context("KV executing identity")?;
        native_rpc::validate_identity(
            "cellar-kv-service",
            crate::deploy::KV_CLASS,
            &identity.scope,
            request.name.as_deref(),
        )?;
        Ok((method, request.args, Some(held)))
    } else {
        Ok((method.into(), args.clone(), None))
    }
}

async fn read_current(context: &Context, encoded: &str) -> Result<CellGroupPayloadBytes> {
    let reference = Reference::parse(encoded)?;
    ensure!(reference.published, "KV_ERROR: unconfirmed wire dependency");
    let dispatch =
        crate::storage::capture_raft_dispatch()?.ok_or(CommandFailure::UnsupportedCapability)?;
    let source = dispatch.binding_source()?;
    reference.matches(&source.history)?;
    let result = crate::native_worker::kv_blob(
        context,
        json!({"mode":"get","reference":serde_json::to_string(&reference.object)?}),
        None,
    )
    .await?;
    let crate::kv_blob::BlobReply::GroupBytes(bytes) = result else {
        bail!(CommandFailure::CorruptState)
    };
    Ok(bytes)
}

pub(super) async fn prepare_input(context: &Context, args: &RpcData) -> Result<RpcData> {
    let value = decode_rpc(args)?.to_json()?;
    let body = value
        .as_array()
        .and_then(|args| args.first())
        .context("KV wire preparation")?;
    let reply = crate::native_worker::kv_blob(
        context,
        json!({"mode":"prepare","digest":body["digest"],"length":body["length"]}),
        None,
    )
    .await?;
    let crate::kv_blob::BlobReply::Json(reply) = reply else {
        bail!(CommandFailure::CorruptState)
    };
    encode_rpc(&StructuredValue::from_json(reply["wire"].clone())?)
}

pub(super) async fn wrap_reply(context: &Context, result: RpcData) -> Result<RpcData> {
    let dispatch =
        crate::storage::capture_raft_dispatch()?.ok_or(CommandFailure::UnsupportedCapability)?;
    let bytes = native_rpc::encode_response(&result)?;
    if bytes.len() <= dispatch.result_limit()? {
        return Ok(result);
    }
    drop(result);
    let reply = crate::native_worker::kv_blob(
        context,
        json!({"mode":"prepare","digest":hex::encode(Sha256::digest(&bytes)),"length":bytes.len()}),
        None,
    )
    .await?;
    let crate::kv_blob::BlobReply::Json(reply) = reply else {
        bail!(CommandFailure::CorruptState)
    };
    let mut reference = Reference::parse(reply["wire"].as_str().context("KV prepared reply")?)?;
    crate::native_worker::kv_blob(
        context,
        json!({"mode":"put","reference":reply["reference"]}),
        Some(bytes),
    )
    .await?;
    reference.published = true;
    encode_rpc(&StructuredValue::from_json(
        json!({(REPLY):serde_json::to_string(&reference)?}),
    )?)
}

/// The caller is the peer-authenticated operator entrance, not an application
/// binding. The selected KV namespace still supplies the actual object store.
pub(super) async fn publish_admin_bytes(
    owner: &super::super::consensus::RaftCells,
    scope: &str,
    name: Option<&str>,
    bytes: bytes::Bytes,
    deadline: u64,
) -> Result<String> {
    owner.kv_wire_input_limit(scope, PREPARE)?;
    remaining(deadline)?;
    let digest: [u8; 32] = Sha256::digest(&bytes).into();
    let reference = if let Some((history, object)) =
        owner.prepare_kv_payload(scope, digest, bytes.len() as u64)?
    {
        Reference::pending(&history, object)
    } else {
        let preparation = encode_rpc(&StructuredValue::from_json(
            json!([{"digest":hex::encode(digest),"length":bytes.len()}]),
        )?)?;
        let admitted = crate::asyncrt::services()
            .admit_outbound_rpc(scope, name, PREPARE, &preparation)
            .ok_or(CommandFailure::Overloaded)?;
        let mut call_scope = crate::host_rpc::CallScope::default();
        call_scope.deadline_ms = Some(deadline);
        let result = owner
            .prepare_kv_ingress(admitted, call_scope, deadline, digest, bytes.len() as u64)
            .await?;
        let prepared = decode_rpc_result(&result)?.to_json()?;
        Reference::parse(prepared.as_str().context("KV prepared operator bytes")?)?
    };
    ensure!(
        reference.cell == scope,
        "KV_ERROR: external operator scope differs"
    );
    let store = owner.kv_wire_store(&reference.history()?)?;
    let Transfer::Published(reference) =
        execute(store, reference, Some(bytes), remaining(deadline)?).await?
    else {
        bail!(CommandFailure::CorruptState)
    };
    ensure!(
        !owner.is_closed(),
        super::super::consensus::RuntimeError::Closed
    );
    remaining(deadline)?;
    Ok(reference)
}

pub(super) async fn fetch_admin_value(
    owner: &super::super::consensus::RaftCells,
    scope: &str,
    encoded: &str,
    deadline: u64,
) -> Result<CellGroupPayloadBytes> {
    value_length(encoded)?;
    let reference = Reference::parse(encoded)?;
    ensure!(
        reference.cell == scope,
        "KV_ERROR: external operator scope differs"
    );
    let store = owner.kv_wire_store(&reference.history()?)?;
    let Transfer::Bytes(bytes) = execute(store, reference, None, remaining(deadline)?).await?
    else {
        bail!(CommandFailure::CorruptState)
    };
    ensure!(
        !owner.is_closed(),
        super::super::consensus::RuntimeError::Closed
    );
    remaining(deadline)?;
    Ok(bytes)
}

pub(crate) async fn stage_admin(
    owner: &super::super::consensus::RaftCells,
    scope: &str,
    name: Option<&str>,
    args: RpcData,
    deadline: u64,
) -> Result<RpcData> {
    let args = super::raft_value::stage_operator(owner, scope, name, args, deadline).await?;
    let request = native_rpc::encode_request(name, &args, MAX_BYTES)?;
    if request.len() <= owner.kv_wire_input_limit(scope, PREPARE)? {
        return Ok(args);
    }
    drop(args);
    let reference = publish_admin_bytes(owner, scope, name, request.into(), deadline).await?;
    encode_rpc(&StructuredValue::from_json(json!({(INPUT):reference}))?)
}

pub(crate) async fn expand_admin(
    owner: &super::super::consensus::RaftCells,
    scope: &str,
    result: RpcData,
    deadline: u64,
) -> Result<Value> {
    let value = decode_rpc_result(&result)?;
    let value = if let Some(encoded) = external_reply(&value)? {
        let reference = Reference::parse(&encoded)?;
        ensure!(
            reference.cell == scope,
            "KV_ERROR: external operator scope differs"
        );
        let store = owner.kv_wire_store(&reference.history()?)?;
        let Transfer::Bytes(held) = execute(store, reference, None, remaining(deadline)?).await?
        else {
            bail!(CommandFailure::CorruptState)
        };
        decode_rpc_result(&native_rpc::decode_response(held.as_ref())?)?.to_json()?
    } else {
        value.to_json()?
    };
    super::raft_value::expand_operator(owner, scope, value, deadline).await
}
