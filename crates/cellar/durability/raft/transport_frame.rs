//! Bounded marker-1 frames and authenticated acknowledgements. An accepted frame
//! is never an append response, commit proof or snapshot-install proof.

use super::transport::{Lane, TransportError};
use super::transport_options::TransportOptions;
use crate::application::consensus::protocol::WireScope;
use crate::peer_auth::PeerAuth;
use ::raft::prelude::{Message, MessageType};
use cellar_logic::consensus::{
    DiskLineage, GroupScope, MemberId, NodeId, ProcessGeneration, ReplicaIdentity,
};
use protobuf::Message as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const PREFIX: usize = 20;
pub(crate) const TAG: usize = 32;
pub(crate) const EXPORT_LABEL: &[u8] = b"EXPORTER-cellar-raft-1";
const REQUEST: &[u8] = b"cellar-raft-request-1";
const RESPONSE: &[u8] = b"cellar-raft-response-1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WirePeer {
    node: [u8; 16],
    disk: [u8; 16],
    process: [u8; 16],
    member: String,
    label: String,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Route {
    scope: WireScope,
    schema: [u8; 32],
    source: WirePeer,
    target: WirePeer,
}

impl Route {
    pub fn new(
        source: &ReplicaIdentity,
        source_label: &str,
        target: &ReplicaIdentity,
        target_label: &str,
        schema: [u8; 32],
    ) -> Result<Self, TransportError> {
        source
            .scope
            .validate()
            .map_err(|_| TransportError::Identity)?;
        if source.scope != target.scope {
            return Err(TransportError::Identity);
        }
        Ok(Self {
            scope: WireScope::from(&source.scope),
            schema,
            source: WirePeer::new(source, source_label),
            target: WirePeer::new(target, target_label),
        })
    }

    pub fn decode(&self) -> Result<(ReplicaIdentity, ReplicaIdentity), TransportError> {
        let scope = self
            .scope
            .clone()
            .decode()
            .map_err(|_| TransportError::Identity)?;
        scope.validate().map_err(|_| TransportError::Identity)?;
        Ok((
            self.source.decode(scope.clone())?,
            self.target.decode(scope)?,
        ))
    }

    pub fn schema(&self) -> [u8; 32] {
        self.schema
    }

    pub fn source_label(&self) -> &str {
        &self.source.label
    }

    pub fn target_label(&self) -> &str {
        &self.target.label
    }

    fn reversed(&self) -> Self {
        Self {
            scope: self.scope.clone(),
            schema: self.schema,
            source: self.target.clone(),
            target: self.source.clone(),
        }
    }
}

impl WirePeer {
    fn new(replica: &ReplicaIdentity, label: &str) -> Self {
        Self {
            node: replica.node.bytes(),
            disk: replica.disk.bytes(),
            process: replica.process.bytes(),
            member: replica.member.get().to_string(),
            label: label.into(),
        }
    }

    fn decode(&self, scope: GroupScope) -> Result<ReplicaIdentity, TransportError> {
        crate::machine::validate_node_name(&self.label).map_err(|_| TransportError::Identity)?;
        Ok(ReplicaIdentity {
            scope,
            node: NodeId::new(self.node).map_err(|_| TransportError::Identity)?,
            disk: DiskLineage::new(self.disk).map_err(|_| TransportError::Identity)?,
            process: ProcessGeneration::new(self.process).map_err(|_| TransportError::Identity)?,
            member: MemberId::new(decimal(&self.member)?).map_err(|_| TransportError::Identity)?,
        })
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RequestHeader {
    protocol: [u8; 32],
    pub route: Route,
    sequence: String,
    pub lane: Lane,
    body_digest: [u8; 32],
}

impl RequestHeader {
    pub fn new(route: Route, sequence: u64, lane: Lane, body: &[u8]) -> Self {
        Self {
            protocol: protocol_digest(),
            route,
            sequence: sequence.to_string(),
            lane,
            body_digest: Sha256::digest(body).into(),
        }
    }

    pub fn sequence(&self) -> Result<u64, TransportError> {
        let value = decimal(&self.sequence)?;
        if value == 0 {
            return Err(TransportError::Replay);
        }
        Ok(value)
    }

    pub fn set_sequence(&mut self, sequence: u64) {
        self.sequence = sequence.to_string();
    }

    pub fn validate(&self) -> Result<(), TransportError> {
        if self.protocol != protocol_digest() {
            return Err(TransportError::Protocol);
        }
        self.sequence()?;
        self.route.decode()?;
        Ok(())
    }

    pub fn verify_body(
        &self,
        bytes: &[u8],
        options: &TransportOptions,
    ) -> Result<Message, TransportError> {
        if bytes.len() > payload_limit(self.lane, options) {
            return Err(TransportError::Limit);
        }
        if self.body_digest != <[u8; 32]>::from(Sha256::digest(bytes)) {
            return Err(TransportError::Authentication);
        }
        // Preflight the protobuf without allocating repeated entries or nested
        // unknown fields. Bounding the encoded bytes alone does not bound the
        // number of decoded Entry objects.
        scan_message(bytes, options.max_entries)?;
        let message = Message::parse_from_bytes(bytes).map_err(|_| TransportError::Protocol)?;
        let (source, target) = self.route.decode()?;
        if message.from != source.member.get()
            || message.to != target.member.get()
            || classify(&message)? != self.lane
        {
            return Err(TransportError::Identity);
        }
        Ok(message)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Disposition {
    Accepted,
    UnknownGroup,
    StalePeer,
    Overloaded,
    Refused,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseHeader {
    protocol: [u8; 32],
    route: Route,
    sequence: String,
    request: [u8; 32],
    disposition: Disposition,
}

pub(crate) struct FrameHead {
    pub prefix: [u8; PREFIX],
    pub header: Vec<u8>,
    pub tag: [u8; TAG],
    pub payload_len: usize,
}

impl FrameHead {
    pub fn request(
        &self,
        auth: &PeerAuth,
        exporter: &[u8; 32],
    ) -> Result<RequestHeader, TransportError> {
        if &self.prefix[..4] != b"CLRP"
            || !auth.verify_raft_tag(REQUEST, &[exporter, &self.prefix, &self.header], &self.tag)
        {
            return Err(TransportError::Authentication);
        }
        let header: RequestHeader =
            serde_json::from_slice(&self.header).map_err(|_| TransportError::Protocol)?;
        header.validate()?;
        Ok(header)
    }

    pub fn response(
        &self,
        auth: &PeerAuth,
        exporter: &[u8; 32],
        request: &RequestHeader,
        request_tag: &[u8; 32],
    ) -> Result<Disposition, TransportError> {
        if &self.prefix[..4] != b"CLRA"
            || self.payload_len != 0
            || !auth.verify_raft_tag(
                RESPONSE,
                &[exporter, request_tag, &self.prefix, &self.header],
                &self.tag,
            )
        {
            return Err(TransportError::Authentication);
        }
        let header: ResponseHeader =
            serde_json::from_slice(&self.header).map_err(|_| TransportError::Protocol)?;
        if header.protocol != protocol_digest()
            || header.request != *request_tag
            || header.sequence != request.sequence
            || header.route != request.route.reversed()
        {
            return Err(TransportError::Authentication);
        }
        Ok(header.disposition)
    }
}

pub(crate) fn request_head(
    auth: &PeerAuth,
    exporter: &[u8; 32],
    request: &RequestHeader,
    payload_len: usize,
    options: &TransportOptions,
) -> Result<FrameHead, TransportError> {
    request.validate()?;
    let header = serde_json::to_vec(request).map_err(|_| TransportError::Protocol)?;
    if payload_len > payload_limit(request.lane, options) {
        return Err(TransportError::Limit);
    }
    let prefix = prefix(b"CLRP", header.len(), payload_len, options)?;
    let tag = auth.raft_tag(REQUEST, &[exporter, &prefix, &header]);
    Ok(FrameHead {
        prefix,
        header,
        tag,
        payload_len,
    })
}

pub(crate) fn response_head(
    auth: &PeerAuth,
    exporter: &[u8; 32],
    request: &RequestHeader,
    request_tag: &[u8; 32],
    disposition: Disposition,
    options: &TransportOptions,
) -> Result<FrameHead, TransportError> {
    let header = ResponseHeader {
        protocol: protocol_digest(),
        route: request.route.reversed(),
        sequence: request.sequence.clone(),
        request: *request_tag,
        disposition,
    };
    let header = serde_json::to_vec(&header).map_err(|_| TransportError::Protocol)?;
    let prefix = prefix(b"CLRA", header.len(), 0, options)?;
    let tag = auth.raft_tag(RESPONSE, &[exporter, request_tag, &prefix, &header]);
    Ok(FrameHead {
        prefix,
        header,
        tag,
        payload_len: 0,
    })
}

fn prefix(
    magic: &[u8; 4],
    header: usize,
    payload: usize,
    options: &TransportOptions,
) -> Result<[u8; PREFIX], TransportError> {
    if header == 0 || header > options.max_header_bytes || payload > options.max_message_bytes {
        return Err(TransportError::Limit);
    }
    let mut bytes = [0; PREFIX];
    bytes[..4].copy_from_slice(magic);
    bytes[4..8].copy_from_slice(&1_u32.to_be_bytes());
    bytes[8..12].copy_from_slice(&(header as u32).to_be_bytes());
    bytes[12..].copy_from_slice(&(payload as u64).to_be_bytes());
    Ok(bytes)
}

pub(crate) fn parse_prefix(
    bytes: &[u8; PREFIX],
    options: &TransportOptions,
    lane: Lane,
    response: bool,
) -> Result<(usize, usize), TransportError> {
    let magic = if response { b"CLRA" } else { b"CLRP" };
    if &bytes[..4] != magic || bytes[4..8] != 1_u32.to_be_bytes() {
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
    if header == 0
        || header > options.max_header_bytes
        || payload > payload_limit(lane, options) as u64
        || (response && payload != 0)
    {
        return Err(TransportError::Limit);
    }
    Ok((header, payload as usize))
}

pub(crate) fn payload_limit(lane: Lane, options: &TransportOptions) -> usize {
    match lane {
        Lane::Control => options.max_control_bytes,
        Lane::Data => options.max_message_bytes,
    }
}

pub(crate) fn classify(message: &Message) -> Result<Lane, TransportError> {
    use MessageType::*;
    match message.get_msg_type() {
        MsgAppend if !message.entries.is_empty() => Ok(Lane::Data),
        MsgSnapshot => Ok(Lane::Data),
        MsgAppend
        | MsgAppendResponse
        | MsgRequestVote
        | MsgRequestVoteResponse
        | MsgHeartbeat
        | MsgHeartbeatResponse
        | MsgTransferLeader
        | MsgTimeoutNow
        | MsgReadIndex
        | MsgReadIndexResp
        | MsgRequestPreVote
        | MsgRequestPreVoteResponse => {
            if message.has_snapshot()
                || (!message.entries.is_empty()
                    && !matches!(message.get_msg_type(), MsgReadIndex | MsgReadIndexResp))
            {
                return Err(TransportError::Protocol);
            }
            Ok(Lane::Control)
        }
        _ => Err(TransportError::Protocol),
    }
}

fn protocol_digest() -> [u8; 32] {
    Sha256::digest(b"cellar-raft-peer-1:tls13-exporter:hmac-sha256:protobuf2:bounded-strict").into()
}

fn decimal(value: &str) -> Result<u64, TransportError> {
    if value.is_empty()
        || value.len() > 20
        || !value.bytes().all(|b| b.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return Err(TransportError::Protocol);
    }
    value.parse().map_err(|_| TransportError::Protocol)
}

/// Strict wire scanner with fixed nesting depth (Message -> Snapshot -> Metadata
/// -> ConfState). Unknown/duplicate singular fields and groups are refused.
pub(crate) fn scan_message(bytes: &[u8], max_entries: usize) -> Result<(), TransportError> {
    scan(bytes, Kind::Message, max_entries)
}

#[derive(Clone, Copy)]
enum Kind {
    Message,
    Entry,
    Snapshot,
    Metadata,
    ConfState,
}

fn scan(mut bytes: &[u8], kind: Kind, max_entries: usize) -> Result<(), TransportError> {
    let mut seen = 0_u32;
    let mut repeated = 0_usize;
    while !bytes.is_empty() {
        let key = varint(&mut bytes)?;
        let field = key >> 3;
        let wire = key & 7;
        if field == 0 || field > 16 {
            return Err(TransportError::Protocol);
        }
        let is_repeated = matches!(kind, Kind::Message) && field == 7
            || matches!(kind, Kind::ConfState) && field <= 4;
        let bit = 1_u32 << field;
        if !is_repeated && seen & bit != 0 {
            return Err(TransportError::Protocol);
        }
        seen |= bit;
        let (allowed, nested) = match kind {
            Kind::Message => (
                matches!(field, 1..=16),
                match field {
                    7 => Some(Kind::Entry),
                    9 => Some(Kind::Snapshot),
                    _ => None,
                },
            ),
            Kind::Entry => (matches!(field, 1..=6), None),
            Kind::Snapshot => (field <= 2, (field == 2).then_some(Kind::Metadata)),
            Kind::Metadata => (field <= 3, (field == 1).then_some(Kind::ConfState)),
            Kind::ConfState => (field <= 5, None),
        };
        if !allowed {
            return Err(TransportError::Protocol);
        }
        let binary = match kind {
            Kind::Message => matches!(field, 7 | 9 | 12),
            Kind::Entry => matches!(field, 4 | 6),
            Kind::Snapshot => true,
            Kind::Metadata => field == 1,
            Kind::ConfState => field <= 4 && wire == 2,
        };
        if binary {
            if wire != 2 {
                return Err(TransportError::Protocol);
            }
            let length = usize::try_from(varint(&mut bytes)?).map_err(|_| TransportError::Limit)?;
            if length > bytes.len() {
                return Err(TransportError::Protocol);
            }
            let value = &bytes[..length];
            bytes = &bytes[length..];
            if matches!(kind, Kind::Message) && field == 7 {
                repeated += 1;
                if repeated > max_entries {
                    return Err(TransportError::Limit);
                }
            }
            if matches!(kind, Kind::Message) && field == 9 && length > 64 * 1024 {
                return Err(TransportError::Limit);
            }
            if matches!(kind, Kind::ConfState) {
                let mut packed = value;
                while !packed.is_empty() {
                    varint(&mut packed)?;
                    repeated += 1;
                    if repeated > 128 {
                        return Err(TransportError::Limit);
                    }
                }
            } else if let Some(nested) = nested {
                scan(value, nested, max_entries)?;
            }
        } else {
            if wire != 0 {
                return Err(TransportError::Protocol);
            }
            let value = varint(&mut bytes)?;
            if matches!(kind, Kind::Message) && field == 1 && value > 18
                || matches!(kind, Kind::Entry) && field == 1 && value > 2
            {
                return Err(TransportError::Protocol);
            }
            if value > 1
                && (matches!(kind, Kind::Message) && field == 10
                    || matches!(kind, Kind::Entry) && field == 5
                    || matches!(kind, Kind::ConfState) && field == 5)
            {
                return Err(TransportError::Protocol);
            }
            if matches!(kind, Kind::ConfState) && field <= 4 {
                repeated += 1;
                if repeated > 128 {
                    return Err(TransportError::Limit);
                }
            }
        }
    }
    Ok(())
}

fn varint(bytes: &mut &[u8]) -> Result<u64, TransportError> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let (&byte, tail) = bytes.split_first().ok_or(TransportError::Protocol)?;
        *bytes = tail;
        if shift == 63 && byte > 1 {
            return Err(TransportError::Protocol);
        }
        value |= ((byte & 127) as u64) << shift;
        if byte & 128 == 0 {
            if shift != 0 && byte == 0 {
                return Err(TransportError::Protocol);
            }
            return Ok(value);
        }
    }
    Err(TransportError::Protocol)
}

#[cfg(test)]
#[path = "transport_frame_tests.rs"]
mod tests;
