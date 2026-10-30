// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded query RPC frames. Their MAC domains, exporter and framing are
//! separate from Raft messages; a read RPC can never become a consensus step.

use super::query_identity::QueryRoute as Route;
use super::transport::TransportError;
use super::transport_frame::{FrameHead, PREFIX};
use super::transport_options::TransportOptions;
use crate::application::consensus::{
    CommandFailure, OperationContract, OperationKind, QueryConsistency, QueryOptions, QueryResult,
    QueryRouting, StaleReadPolicy, VersionKeys, VersionToken,
};
use crate::peer_auth::PeerAuth;
use cellar_logic::consensus::{GroupScope, MemberId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Duration;

#[path = "query_cell_wire.rs"]
pub(super) mod cell;

pub(super) const EXPORT_LABEL: &[u8] = b"EXPORTER-cellar-raft-query-1";
const REQUEST: &[u8] = b"cellar-raft-query-request-1";
const RESPONSE: &[u8] = b"cellar-raft-query-response-1";
const MAX_TOKEN: usize = 1878;

pub(super) struct QueryCall {
    pub contract: OperationContract,
    pub input: Vec<u8>,
    pub options: QueryOptions,
}

impl QueryCall {
    pub fn encode(&self, limit: usize) -> Result<Vec<u8>, TransportError> {
        if self.contract.kind != OperationKind::Query
            || self.contract.operation.is_empty()
            || self.contract.operation.len() > 128
            || self.contract.operation.chars().any(char::is_control)
            || self.options.timeout.as_millis() == 0
            || self.options.timeout > Duration::from_secs(3600)
            || !matches!(self.options.routing, QueryRouting::Local)
        {
            return Err(TransportError::Protocol);
        }
        let mut body = b"CLRQRQ01".to_vec();
        string(&mut body, &self.contract.operation)?;
        body.extend_from_slice(&self.contract.digest);
        match &self.options.consistency {
            QueryConsistency::Stale => body.push(0),
            QueryConsistency::AtLeast(token) => {
                body.push(1);
                string(&mut body, token.as_str())?;
            }
            QueryConsistency::Linearizable => body.push(2),
        }
        body.push(match self.options.stale_policy {
            StaleReadPolicy::RecentSource => 0,
            StaleReadPolicy::DisconnectedHistorical => 1,
        });
        body.extend_from_slice(&(self.options.timeout.as_millis() as u64).to_be_bytes());
        let length = u32::try_from(self.input.len()).map_err(|_| TransportError::Limit)?;
        if body
            .len()
            .checked_add(4)
            .and_then(|n| n.checked_add(self.input.len()))
            .is_none_or(|n| n > limit)
        {
            return Err(TransportError::Limit);
        }
        body.extend_from_slice(&length.to_be_bytes());
        body.extend_from_slice(&self.input);
        Ok(body)
    }

    pub fn decode(bytes: &[u8], limit: usize) -> Result<Self, TransportError> {
        if bytes.len() > limit {
            return Err(TransportError::Limit);
        }
        let mut body = Reader(bytes);
        if body.take(8)? != b"CLRQRQ01" {
            return Err(TransportError::Protocol);
        }
        let operation = body.string(128)?;
        if operation.is_empty() || operation.chars().any(char::is_control) {
            return Err(TransportError::Protocol);
        }
        let contract = OperationContract {
            operation,
            kind: OperationKind::Query,
            digest: body.array()?,
        };
        let consistency = match body.byte()? {
            0 => QueryConsistency::Stale,
            1 => QueryConsistency::AtLeast(
                VersionToken::from_encoded(&body.string(MAX_TOKEN)?)
                    .map_err(|_| TransportError::Protocol)?,
            ),
            2 => QueryConsistency::Linearizable,
            _ => return Err(TransportError::Protocol),
        };
        let stale_policy = match body.byte()? {
            0 => StaleReadPolicy::RecentSource,
            1 => StaleReadPolicy::DisconnectedHistorical,
            _ => return Err(TransportError::Protocol),
        };
        let millis = u64::from_be_bytes(body.array()?);
        if millis == 0 || millis > 3_600_000 {
            return Err(TransportError::Protocol);
        }
        let length = u32::from_be_bytes(body.array()?) as usize;
        let input = body.take(length)?.to_vec();
        if !body.0.is_empty() {
            return Err(TransportError::Protocol);
        }
        Ok(Self {
            contract,
            input,
            options: QueryOptions {
                consistency,
                stale_policy,
                timeout: Duration::from_millis(millis),
                routing: QueryRouting::Local,
            },
        })
    }
}

pub(super) fn encode_result(
    result: &Result<QueryResult, CommandFailure>,
    limit: usize,
) -> Result<Vec<u8>, TransportError> {
    let mut body = b"CLRQRS01".to_vec();
    match result {
        Ok(result) => {
            body.push(0);
            string(&mut body, result.version_token.as_str())?;
            let length = u32::try_from(result.result.len()).map_err(|_| TransportError::Limit)?;
            if body
                .len()
                .checked_add(4)
                .and_then(|n| n.checked_add(result.result.len()))
                .is_none_or(|n| n > limit)
            {
                return Err(TransportError::Limit);
            }
            body.extend_from_slice(&length.to_be_bytes());
            body.extend_from_slice(&result.result);
        }
        Err(failure) => body.push(failure_code(failure)),
    }
    if body.len() > limit {
        return Err(TransportError::Limit);
    }
    Ok(body)
}

pub(super) fn decode_result(
    bytes: &[u8],
    scope: &GroupScope,
    schema: [u8; 32],
    keys: &VersionKeys,
    now_ms: u64,
    limit: usize,
    served_by: MemberId,
) -> Result<Result<QueryResult, CommandFailure>, TransportError> {
    if bytes.len() > limit {
        return Err(TransportError::Limit);
    }
    let mut body = Reader(bytes);
    if body.take(8)? != b"CLRQRS01" {
        return Err(TransportError::Protocol);
    }
    let result = match body.byte()? {
        0 => {
            let version_token = VersionToken::from_encoded(&body.string(MAX_TOKEN)?)
                .map_err(|_| TransportError::Authentication)?;
            let version = keys
                .verify(&version_token, scope, schema, now_ms)
                .map_err(|_| TransportError::Authentication)?;
            let length = u32::from_be_bytes(body.array()?) as usize;
            Ok(QueryResult {
                result: body.take(length)?.to_vec(),
                version,
                version_token,
                served_by,
            })
        }
        status => Err(decode_failure(status)?),
    };
    if !body.0.is_empty() {
        return Err(TransportError::Protocol);
    }
    Ok(result)
}

fn failure_code(failure: &CommandFailure) -> u8 {
    match failure {
        CommandFailure::VersionUnavailable => 1,
        CommandFailure::WrongIncarnation => 2,
        CommandFailure::UnsupportedCapability => 3,
        CommandFailure::UnsupportedConsistency => 4,
        CommandFailure::Overloaded => 5,
        CommandFailure::CorruptState => 6,
        CommandFailure::StorageFull => 8,
        _ => 7,
    }
}

fn decode_failure(status: u8) -> Result<CommandFailure, TransportError> {
    Ok(match status {
        1 => CommandFailure::VersionUnavailable,
        2 => CommandFailure::WrongIncarnation,
        3 => CommandFailure::UnsupportedCapability,
        4 => CommandFailure::UnsupportedConsistency,
        5 => CommandFailure::Overloaded,
        6 => CommandFailure::CorruptState,
        7 => CommandFailure::Unavailable,
        8 => CommandFailure::StorageFull,
        _ => return Err(TransportError::Protocol),
    })
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RequestHead {
    pub route: Route,
    sequence: String,
    digest: [u8; 32],
}

impl RequestHead {
    pub fn new(route: Route, sequence: u64, body: &[u8]) -> Self {
        Self {
            route,
            sequence: sequence.to_string(),
            digest: Sha256::digest(body).into(),
        }
    }

    pub fn sequence(&self) -> Result<u64, TransportError> {
        let sequence: u64 = self
            .sequence
            .parse()
            .map_err(|_| TransportError::Protocol)?;
        if sequence == 0 || sequence.to_string() != self.sequence {
            return Err(TransportError::Replay);
        }
        Ok(sequence)
    }

    pub fn verify_body(&self, bytes: &[u8]) -> Result<(), TransportError> {
        if self.digest != <[u8; 32]>::from(Sha256::digest(bytes)) {
            return Err(TransportError::Authentication);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseHead {
    route: Route,
    sequence: String,
    request: [u8; 32],
    digest: [u8; 32],
}

pub(super) fn request_head(
    auth: &PeerAuth,
    exporter: &[u8; 32],
    request: &RequestHead,
    body: &[u8],
    options: &TransportOptions,
) -> Result<FrameHead, TransportError> {
    request.sequence()?;
    request.route.validate()?;
    request.verify_body(body)?;
    make_head(
        auth,
        exporter,
        None,
        b"CLQP",
        &serde_json::to_vec(request).map_err(|_| TransportError::Protocol)?,
        body.len(),
        options,
    )
}

pub(super) fn read_request(
    head: &FrameHead,
    auth: &PeerAuth,
    exporter: &[u8; 32],
    options: &TransportOptions,
) -> Result<RequestHead, TransportError> {
    verify_head(head, auth, exporter, None, options)?;
    let request: RequestHead =
        serde_json::from_slice(&head.header).map_err(|_| TransportError::Protocol)?;
    request.sequence()?;
    request.route.validate()?;
    if serde_json::to_vec(&request).map_err(|_| TransportError::Protocol)? != head.header {
        return Err(TransportError::Protocol);
    }
    Ok(request)
}

pub(super) fn response_head(
    auth: &PeerAuth,
    exporter: &[u8; 32],
    request: &RequestHead,
    request_tag: &[u8; 32],
    body: &[u8],
    options: &TransportOptions,
) -> Result<FrameHead, TransportError> {
    let header = ResponseHead {
        route: request.route.reversed(),
        sequence: request.sequence.clone(),
        request: *request_tag,
        digest: Sha256::digest(body).into(),
    };
    make_head(
        auth,
        exporter,
        Some(request_tag),
        b"CLQA",
        &serde_json::to_vec(&header).map_err(|_| TransportError::Protocol)?,
        body.len(),
        options,
    )
}

pub(super) fn read_response(
    head: &FrameHead,
    auth: &PeerAuth,
    exporter: &[u8; 32],
    request: &RequestHead,
    request_tag: &[u8; 32],
    options: &TransportOptions,
) -> Result<[u8; 32], TransportError> {
    verify_head(head, auth, exporter, Some(request_tag), options)?;
    let header: ResponseHead =
        serde_json::from_slice(&head.header).map_err(|_| TransportError::Protocol)?;
    if header.route != request.route.reversed()
        || header.sequence != request.sequence
        || header.request != *request_tag
        || serde_json::to_vec(&header).map_err(|_| TransportError::Protocol)? != head.header
    {
        return Err(TransportError::Authentication);
    }
    Ok(header.digest)
}

fn make_head(
    auth: &PeerAuth,
    exporter: &[u8; 32],
    request: Option<&[u8; 32]>,
    magic: &[u8; 4],
    header: &[u8],
    payload_len: usize,
    options: &TransportOptions,
) -> Result<FrameHead, TransportError> {
    let limit = if request.is_some() {
        options.max_query_response_bytes
    } else {
        options.max_query_request_bytes
    };
    if header.is_empty() || header.len() > options.max_header_bytes || payload_len > limit {
        return Err(TransportError::Limit);
    }
    let mut prefix = [0; PREFIX];
    prefix[..4].copy_from_slice(magic);
    prefix[4..8].copy_from_slice(&1u32.to_be_bytes());
    prefix[8..12].copy_from_slice(&(header.len() as u32).to_be_bytes());
    prefix[12..].copy_from_slice(&(payload_len as u64).to_be_bytes());
    let tag = match request {
        Some(request) => auth.raft_tag(RESPONSE, &[exporter, request, &prefix, header]),
        None => auth.raft_tag(REQUEST, &[exporter, &prefix, header]),
    };
    Ok(FrameHead {
        prefix,
        header: header.to_vec(),
        tag,
        payload_len,
    })
}

fn verify_head(
    head: &FrameHead,
    auth: &PeerAuth,
    exporter: &[u8; 32],
    request: Option<&[u8; 32]>,
    options: &TransportOptions,
) -> Result<(), TransportError> {
    let (header, payload) = parse_prefix(&head.prefix, options, request.is_some())?;
    if header != head.header.len() || payload != head.payload_len {
        return Err(TransportError::Protocol);
    }
    let valid = match request {
        Some(request) => auth.verify_raft_tag(
            RESPONSE,
            &[exporter, request, &head.prefix, &head.header],
            &head.tag,
        ),
        None => auth.verify_raft_tag(REQUEST, &[exporter, &head.prefix, &head.header], &head.tag),
    };
    if !valid {
        return Err(TransportError::Authentication);
    }
    Ok(())
}

pub(super) fn parse_prefix(
    bytes: &[u8; PREFIX],
    options: &TransportOptions,
    response: bool,
) -> Result<(usize, usize), TransportError> {
    if &bytes[..4] != if response { b"CLQA" } else { b"CLQP" } || bytes[4..8] != 1u32.to_be_bytes()
    {
        return Err(TransportError::Protocol);
    }
    let header = u32::from_be_bytes(
        bytes[8..12]
            .try_into()
            .map_err(|_| TransportError::Protocol)?,
    ) as usize;
    let payload = u64::from_be_bytes(
        bytes[12..]
            .try_into()
            .map_err(|_| TransportError::Protocol)?,
    );
    let limit = if response {
        options.max_query_response_bytes
    } else {
        options.max_query_request_bytes
    };
    if header == 0 || header > options.max_header_bytes || payload > limit as u64 {
        return Err(TransportError::Limit);
    }
    Ok((header, payload as usize))
}

fn string(bytes: &mut Vec<u8>, text: &str) -> Result<(), TransportError> {
    let length = u16::try_from(text.len()).map_err(|_| TransportError::Limit)?;
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(text.as_bytes());
    Ok(())
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], TransportError> {
        let (value, rest) = self.0.split_at_checked(n).ok_or(TransportError::Protocol)?;
        self.0 = rest;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], TransportError> {
        self.take(N)?
            .try_into()
            .map_err(|_| TransportError::Protocol)
    }

    fn byte(&mut self) -> Result<u8, TransportError> {
        Ok(self.array::<1>()?[0])
    }

    fn string(&mut self, max: usize) -> Result<String, TransportError> {
        let n = u16::from_be_bytes(self.array()?) as usize;
        if n > max {
            return Err(TransportError::Limit);
        }
        Ok(std::str::from_utf8(self.take(n)?)
            .map_err(|_| TransportError::Protocol)?
            .into())
    }
}

#[cfg(all(test, not(cellar_internal_tests)))]
#[path = "query_wire_tests.rs"]
mod tests;
