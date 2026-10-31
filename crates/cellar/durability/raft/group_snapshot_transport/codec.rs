// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
use super::*;
use crate::raft::cell_groups::checkpoint::wire::Part;
use sha2::{Digest, Sha256};

pub(in crate::raft::transport) const START_MAGIC: &[u8; 4] = b"CLGS";
pub(super) const DATA_MAGIC: &[u8; 4] = b"CLGD";
pub(super) const ACK_MAGIC: &[u8; 4] = b"CLGA";
pub(super) const START: &[u8] = b"cellar-group-snapshot-start-1";
pub(super) const DATA: &[u8] = b"cellar-group-snapshot-data-1";
pub(super) const ACK: &[u8] = b"cellar-group-snapshot-admitted-1";
pub(super) const CHUNK_BYTES: usize = 64 * 1024;
pub(super) const MESSAGE_BYTES: usize = 128 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Start {
    pub route: Route,
    pub digest: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Packet {
    pub sequence: u64,
    pub part: Option<Part>,
    pub offset: u64,
    pub length: u64,
    pub digest: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Ack {
    pub sequence: u64,
}

pub(super) fn digest(body: &[u8]) -> [u8; 32] {
    Sha256::digest(body).into()
}

pub(super) fn message(body: &[u8], route: &Route) -> Result<Message, TransportError> {
    if body.len() > MESSAGE_BYTES {
        return Err(TransportError::Limit);
    }
    frame::scan_message(body, 0)?;
    let message = Message::parse_from_bytes(body).map_err(|_| TransportError::Protocol)?;
    let (source, target) = route.decode_group()?;
    if message.get_msg_type() != MessageType::MsgSnapshot
        || !message.has_snapshot()
        || message.from != source.member.get()
        || message.to != target.member.get()
        || message.term == 0
        || message.get_snapshot().is_empty()
        || !message.entries.is_empty()
        || !message.context.is_empty()
    {
        return Err(TransportError::Identity);
    }
    Ok(message)
}
