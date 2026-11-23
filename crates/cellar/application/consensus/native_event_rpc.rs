// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Private EventStream provenance participates in the retained request digest.
//! Decoding these bytes never grants admission: only the authenticated event
//! transport and host adapters may select `CommandAdmission::Event`.

use super::{native_rpc, CommandFailure, OperationContract, OperationKind};
use crate::application::event_stream;
use crate::engine_api::RpcData;
use anyhow::{ensure, Context as _, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const OPERATION: &str = "__sys_raft_event_rpc";
const MAGIC: &[u8; 5] = b"CEVT\x01";
const MAX_HEADER: usize = 16 << 10;

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "authority", deny_unknown_fields)]
pub(crate) enum Authority {
    Event(event_stream::Authority),
    Name(event_stream::name_resolution::NameAuthority),
    Catalog(crate::runtime::rpc_authority::CatalogAuthority),
    Sandbox(crate::application::sandbox_catalog::shared::Authority),
}

impl From<event_stream::Authority> for Authority {
    fn from(value: event_stream::Authority) -> Self {
        Self::Event(value)
    }
}

impl From<event_stream::name_resolution::NameAuthority> for Authority {
    fn from(value: event_stream::name_resolution::NameAuthority) -> Self {
        Self::Name(value)
    }
}

impl Authority {
    pub(crate) fn validate_target(
        &self,
        scope: &str,
        name: Option<&str>,
        method: &str,
    ) -> Result<()> {
        let expected_name = self.target_name(scope, method)?;
        ensure!(
            name == Some(expected_name.as_str()),
            "event RPC routing name differs"
        );
        Ok(())
    }

    pub(crate) fn target_name(&self, scope: &str, method: &str) -> Result<String> {
        Ok(match self {
            Self::Event(authority) => {
                event_stream::validate_target(scope, authority)?;
                authority.validate_operator_target(scope, method)?;
                if scope == event_stream::cell_scope(&authority.resource)? {
                    event_stream::cell_name(&authority.resource)?
                } else {
                    event_stream::catalog_name(&authority.resource)?
                }
            }
            Self::Name(authority) => {
                authority.validate_target(scope, method)?;
                authority.namespace().name()?
            }
            Self::Sandbox(authority) => authority.target_name(scope, method)?,
            Self::Catalog(authority) => {
                authority.validate_target(scope, method)?;
                authority.namespace().name()?
            }
        })
    }

    pub(crate) fn install(self, context: &mut crate::application::Context) {
        match self {
            Self::Event(authority) => context.event_authority = Some(authority),
            Self::Name(authority) => context.name_authority = Some(authority),
            Self::Catalog(authority) => context.catalog_authority = Some(authority),
            Self::Sandbox(authority) => context.sandbox_authority = Some(authority),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    method: String,
    authority: Authority,
}

pub(crate) struct Request {
    pub(crate) method: String,
    pub(crate) authority: Authority,
    pub(crate) rpc: native_rpc::Request,
}

pub(crate) fn contract(class: &str) -> OperationContract {
    let mut hash = Sha256::new();
    hash.update(b"cellar-raft-private-event-rpc-1");
    hash.update((class.len() as u64).to_be_bytes());
    hash.update(class.as_bytes());
    OperationContract {
        operation: OPERATION.into(),
        kind: OperationKind::Command,
        digest: hash.finalize().into(),
    }
}

fn validate_method(method: &str) -> Result<()> {
    ensure!(
        !method.is_empty() && method.len() <= 128 && !method.chars().any(char::is_control),
        CommandFailure::UnsupportedCapability
    );
    Ok(())
}

pub(crate) fn encode(
    scope: &str,
    name: Option<&str>,
    method: &str,
    authority: Authority,
    args: &RpcData,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    encode_inner(scope, name, method, authority, args, max_bytes, None)
}

pub(crate) fn encode_ingress(
    scope: &str,
    name: Option<&str>,
    method: &str,
    authority: Authority,
    args: &RpcData,
    max_bytes: usize,
    nonce: [u8; 16],
) -> Result<Vec<u8>> {
    encode_inner(scope, name, method, authority, args, max_bytes, Some(nonce))
}

fn encode_inner(
    scope: &str,
    name: Option<&str>,
    method: &str,
    authority: Authority,
    args: &RpcData,
    max_bytes: usize,
    nonce: Option<[u8; 16]>,
) -> Result<Vec<u8>> {
    validate_method(method)?;
    authority.validate_target(scope, name, method)?;
    let header = serde_json::to_vec(&Header {
        method: method.into(),
        authority,
    })?;
    ensure!(header.len() <= MAX_HEADER, CommandFailure::Overloaded);
    let remaining = max_bytes
        .checked_sub(MAGIC.len() + 4 + header.len())
        .ok_or(CommandFailure::Overloaded)?;
    let rpc = match nonce {
        Some(nonce) => native_rpc::encode_ingress_request(name, args, nonce, remaining)?,
        None => native_rpc::encode_request(name, args, remaining)?,
    };
    let mut output = Vec::with_capacity(MAGIC.len() + 4 + header.len() + rpc.len());
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&u32::try_from(header.len())?.to_be_bytes());
    output.extend_from_slice(&header);
    output.extend_from_slice(&rpc);
    Ok(output)
}

pub(crate) fn has_ingress_nonce(input: &[u8]) -> bool {
    let Some(length) = input
        .get(5..9)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_be_bytes)
    else {
        return false;
    };
    input.starts_with(MAGIC)
        && length as usize <= MAX_HEADER
        && input
            .get(9 + length as usize..)
            .is_some_and(native_rpc::has_ingress_nonce)
}

fn request_parts(input: &[u8]) -> Result<(Header, &[u8])> {
    ensure!(input.starts_with(MAGIC), "invalid private event RPC format");
    let length = u32::from_be_bytes(
        input
            .get(5..9)
            .context("truncated event RPC header")?
            .try_into()?,
    ) as usize;
    ensure!(length <= MAX_HEADER, CommandFailure::Overloaded);
    let (header, rpc) = input
        .get(9..)
        .context("truncated event RPC")?
        .split_at_checked(length)
        .context("truncated event RPC authority")?;
    let header: Header = serde_json::from_slice(header)?;
    validate_method(&header.method)?;
    Ok((header, rpc))
}

/// Probe an exact provisioning receipt without decoding the large definition
/// body. The original command remains owned by ingress for a known
/// absence fallback, with its nonce and complete fingerprint unchanged.
pub(super) fn configuration_receipt_query(
    scope: &str,
    input: &[u8],
    max_bytes: usize,
) -> Result<(Authority, Vec<u8>)> {
    let (mut header, rpc) = request_parts(input)?;
    ensure!(
        header.method == "__catalogConfigure",
        CommandFailure::UnsupportedCapability
    );
    let Authority::Event(caller) = &header.authority else {
        anyhow::bail!(CommandFailure::UnsupportedCapability);
    };
    caller.require_provisioning()?;
    header.method = "__catalogConfigureReceipt".into();
    header.authority.target_name(scope, &header.method)?;
    let encoded = serde_json::to_vec(&header)?;
    ensure!(encoded.len() <= MAX_HEADER, CommandFailure::Overloaded);
    let (prefix, body) = native_rpc::query_request_parts(rpc)?;
    let length = MAGIC
        .len()
        .checked_add(4)
        .and_then(|n| n.checked_add(encoded.len()))
        .and_then(|n| n.checked_add(prefix.len()))
        .and_then(|n| n.checked_add(body.len()))
        .ok_or(CommandFailure::Overloaded)?;
    ensure!(length <= max_bytes, CommandFailure::Overloaded);
    let mut output = Vec::with_capacity(length);
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&u32::try_from(encoded.len())?.to_be_bytes());
    output.extend_from_slice(&encoded);
    output.extend_from_slice(prefix);
    output.extend_from_slice(body);
    Ok((header.authority, output))
}

pub(crate) fn decode(scope: &str, input: &[u8]) -> Result<Request> {
    let (Header { method, authority }, rpc) = request_parts(input)?;
    let rpc = native_rpc::decode_request(rpc)?;
    authority.validate_target(scope, rpc.name.as_deref(), &method)?;
    Ok(Request {
        method,
        authority,
        rpc,
    })
}

#[cfg(test)]
#[path = "native_event_rpc_tests.rs"]
mod tests;
