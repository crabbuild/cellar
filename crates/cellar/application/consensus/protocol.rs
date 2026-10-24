//! Bounded marker-1 encoding. LTX bytes retain their upstream representation.

use cellar_logic::consensus::{
    ClusterId, GroupId, GroupScope, IncarnationId, NamespaceId, ProposalId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

const MAGIC: &[u8; 4] = b"CLRF";
const MARKER: u32 = 1;
const PREFIX: usize = 20;
const DIGEST_LEN: usize = 32;

// Wire markers remain 1. The immutable namespace identity also binds member
// metadata so incompatible binaries cannot attach to the same namespace.
const PROTOCOL: &str =
    "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1/enrollment-1/node-probes-1/birth-admission-1/managed-genesis-1/group-reservations-1/birth-inventory-1";

/// Bounds apply before decoding the header or copying any payload. The LTX
/// decoder must additionally enforce these bounds on actual decoded pages;
/// declarations in this envelope are never a substitute for that check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnvelopeLimits {
    pub max_header_bytes: usize,
    pub max_payload_bytes: usize,
    pub max_files: usize,
    pub max_decoded_bytes: u64,
    pub max_pages: u64,
}

impl Default for EnvelopeLimits {
    fn default() -> Self {
        Self {
            max_header_bytes: 64 * 1024,
            max_payload_bytes: 16 * 1024 * 1024,
            max_files: 64,
            max_decoded_bytes: 64 * 1024 * 1024,
            max_pages: 131_072,
        }
    }
}

impl EnvelopeLimits {
    pub fn validate(self) -> Result<(), ProtocolError> {
        if self.max_header_bytes == 0
            || self.max_header_bytes > 1024 * 1024
            || self.max_payload_bytes == 0
            || self.max_payload_bytes > 64 * 1024 * 1024
            || self.max_files == 0
            || self.max_files > 1024
            || self.max_decoded_bytes == 0
            || self.max_decoded_bytes > 256 * 1024 * 1024
            || self.max_pages == 0
            || self.max_pages > 524_288
        {
            return Err(ProtocolError::Limit);
        }
        Ok(())
    }
}

/// LTX position and independent full-image SHA-256. A NoChecksum LTX still
/// requires this image digest to establish the exact base/result state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LtxImage {
    pub epoch: u64,
    pub txid: u64,
    pub checksum: u64,
    pub image_digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LtxUpdate {
    pub min_txid: u64,
    pub max_txid: u64,
    pub page_count: u64,
    pub decoded_bytes: u64,
    pub bytes: Vec<u8>,
}

/// One atomic data proposal, potentially containing several complete LTX files.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellUpdate {
    pub scope: GroupScope,
    pub application_digest: [u8; 32],
    pub proposal: ProposalId,
    pub base: LtxImage,
    pub result: LtxImage,
    pub files: Vec<LtxUpdate>,
}

impl CellUpdate {
    pub fn encode(&self, limits: EnvelopeLimits) -> Result<Vec<u8>, ProtocolError> {
        limits.validate()?;
        self.validate(limits)?;
        let mut offset = 0_u64;
        let mut spans = Vec::with_capacity(self.files.len());
        for file in &self.files {
            spans.push(WireFile {
                offset: offset.into(),
                length: (file.bytes.len() as u64).into(),
                min_txid: file.min_txid.into(),
                max_txid: file.max_txid.into(),
                page_count: file.page_count.into(),
                decoded_bytes: file.decoded_bytes.into(),
            });
            offset = offset
                .checked_add(file.bytes.len() as u64)
                .ok_or(ProtocolError::Limit)?;
        }
        let header = WireHeader {
            protocol: protocol_digest(),
            scope: WireScope::from(&self.scope),
            application_digest: hex::encode(self.application_digest),
            proposal: hex::encode(self.proposal.bytes()),
            base: self.base.into(),
            result: self.result.into(),
            files: spans,
        };
        let header = serde_json::to_vec(&header).map_err(|_| ProtocolError::Header)?;
        if header.len() > limits.max_header_bytes {
            return Err(ProtocolError::Limit);
        }
        let length = PREFIX
            .checked_add(header.len())
            .and_then(|n| n.checked_add(offset as usize))
            .and_then(|n| n.checked_add(DIGEST_LEN))
            .ok_or(ProtocolError::Limit)?;
        let mut bytes = Vec::with_capacity(length);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&MARKER.to_be_bytes());
        bytes.extend_from_slice(&(header.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&offset.to_be_bytes());
        bytes.extend_from_slice(&header);
        for file in &self.files {
            bytes.extend_from_slice(&file.bytes);
        }
        let digest = Sha256::digest(&bytes);
        bytes.extend_from_slice(&digest);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8], limits: EnvelopeLimits) -> Result<Self, ProtocolError> {
        limits.validate()?;
        if bytes.len() < PREFIX + DIGEST_LEN || &bytes[..4] != MAGIC {
            return Err(ProtocolError::Framing);
        }
        let read_u32 = |start| -> Result<u32, ProtocolError> {
            Ok(u32::from_be_bytes(
                bytes[start..start + 4]
                    .try_into()
                    .map_err(|_| ProtocolError::Framing)?,
            ))
        };
        if read_u32(4)? != MARKER {
            return Err(ProtocolError::Protocol);
        }
        let header_len = read_u32(8)? as usize;
        let payload_len = u64::from_be_bytes(
            bytes[12..20]
                .try_into()
                .map_err(|_| ProtocolError::Framing)?,
        );
        if header_len > limits.max_header_bytes || payload_len > limits.max_payload_bytes as u64 {
            return Err(ProtocolError::Limit);
        }
        let payload_start = PREFIX.checked_add(header_len).ok_or(ProtocolError::Limit)?;
        let end = payload_start
            .checked_add(payload_len as usize)
            .ok_or(ProtocolError::Limit)?;
        if end.checked_add(DIGEST_LEN) != Some(bytes.len()) {
            return Err(ProtocolError::Framing);
        }
        if Sha256::digest(&bytes[..end])[..] != bytes[end..] {
            return Err(ProtocolError::Digest);
        }
        // Derive rejects duplicate named fields; deny_unknown_fields prevents
        // peers from assuming support for an unrecognized required contract.
        let header: WireHeader = serde_json::from_slice(&bytes[PREFIX..payload_start])
            .map_err(|_| ProtocolError::Header)?;
        if header.protocol != protocol_digest() {
            return Err(ProtocolError::Protocol);
        }
        if header.files.is_empty() || header.files.len() > limits.max_files {
            return Err(ProtocolError::Limit);
        }
        let mut expected_offset = 0_u64;
        let mut files = Vec::with_capacity(header.files.len());
        for file in header.files {
            let offset = file.offset.value()?;
            let length = file.length.value()?;
            let next = offset.checked_add(length).ok_or(ProtocolError::Limit)?;
            if offset != expected_offset || length == 0 || next > payload_len {
                return Err(ProtocolError::FileTable);
            }
            files.push(LtxUpdate {
                min_txid: file.min_txid.value()?,
                max_txid: file.max_txid.value()?,
                page_count: file.page_count.value()?,
                decoded_bytes: file.decoded_bytes.value()?,
                bytes: bytes[payload_start + offset as usize..payload_start + next as usize]
                    .to_vec(),
            });
            expected_offset = next;
        }
        if expected_offset != payload_len {
            return Err(ProtocolError::FileTable);
        }
        let update = Self {
            scope: header.scope.decode()?,
            application_digest: decode_hex(&header.application_digest)?,
            proposal: ProposalId::new(decode_hex(&header.proposal)?)
                .map_err(|_| ProtocolError::Identity)?,
            base: header.base.decode()?,
            result: header.result.decode()?,
            files,
        };
        update.validate(limits)?;
        Ok(update)
    }

    fn validate(&self, limits: EnvelopeLimits) -> Result<(), ProtocolError> {
        self.scope.validate().map_err(|_| ProtocolError::Identity)?;
        if self.base.epoch == 0
            || self.base.epoch != self.result.epoch
            || self.result.txid <= self.base.txid
        {
            return Err(ProtocolError::Chain);
        }
        if self.files.is_empty() || self.files.len() > limits.max_files {
            return Err(ProtocolError::Limit);
        }
        let mut txid = self.base.txid;
        let (mut encoded, mut decoded, mut pages) = (0_u64, 0_u64, 0_u64);
        for file in &self.files {
            if txid.checked_add(1) != Some(file.min_txid)
                || file.max_txid < file.min_txid
                || file.bytes.is_empty()
            {
                return Err(ProtocolError::Chain);
            }
            txid = file.max_txid;
            encoded = encoded
                .checked_add(file.bytes.len() as u64)
                .ok_or(ProtocolError::Limit)?;
            decoded = decoded
                .checked_add(file.decoded_bytes)
                .ok_or(ProtocolError::Limit)?;
            pages = pages
                .checked_add(file.page_count)
                .ok_or(ProtocolError::Limit)?;
        }
        if txid != self.result.txid {
            return Err(ProtocolError::Chain);
        }
        if encoded > limits.max_payload_bytes as u64
            || decoded > limits.max_decoded_bytes
            || pages > limits.max_pages
        {
            return Err(ProtocolError::Limit);
        }
        Ok(())
    }
}

pub(crate) fn protocol_identity() -> [u8; 32] {
    Sha256::digest(PROTOCOL.as_bytes()).into()
}

fn protocol_digest() -> String {
    hex::encode(protocol_identity())
}

fn decode_hex<const N: usize>(value: &str) -> Result<[u8; N], ProtocolError> {
    if value.len() != 2 * N
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ProtocolError::Header);
    }
    let mut bytes = [0; N];
    hex::decode_to_slice(value, &mut bytes).map_err(|_| ProtocolError::Header)?;
    Ok(bytes)
}

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
struct Decimal(String);

impl From<u64> for Decimal {
    fn from(value: u64) -> Self {
        Self(value.to_string())
    }
}

impl Decimal {
    fn value(&self) -> Result<u64, ProtocolError> {
        if self.0.is_empty()
            || self.0.len() > 20
            || !self.0.bytes().all(|byte| byte.is_ascii_digit())
            || (self.0.len() > 1 && self.0.starts_with('0'))
        {
            return Err(ProtocolError::Header);
        }
        self.0.parse().map_err(|_| ProtocolError::Header)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireHeader {
    protocol: String,
    scope: WireScope,
    application_digest: String,
    proposal: String,
    base: WireImage,
    result: WireImage,
    files: Vec<WireFile>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WireScope {
    cluster: String,
    namespace: String,
    cell: String,
    group: String,
    incarnation: String,
}

impl From<&GroupScope> for WireScope {
    fn from(scope: &GroupScope) -> Self {
        Self {
            cluster: hex::encode(scope.cluster.bytes()),
            namespace: hex::encode(scope.namespace.bytes()),
            cell: scope.cell.clone(),
            group: hex::encode(scope.group.bytes()),
            incarnation: hex::encode(scope.incarnation.bytes()),
        }
    }
}

impl WireScope {
    pub(crate) fn cell(&self) -> &str {
        &self.cell
    }

    pub(crate) fn decode(self) -> Result<GroupScope, ProtocolError> {
        Ok(GroupScope {
            cluster: ClusterId::new(decode_hex(&self.cluster)?)
                .map_err(|_| ProtocolError::Identity)?,
            namespace: NamespaceId::new(decode_hex(&self.namespace)?)
                .map_err(|_| ProtocolError::Identity)?,
            cell: self.cell,
            group: GroupId::new(decode_hex(&self.group)?).map_err(|_| ProtocolError::Identity)?,
            incarnation: IncarnationId::new(decode_hex(&self.incarnation)?)
                .map_err(|_| ProtocolError::Identity)?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireImage {
    epoch: Decimal,
    txid: Decimal,
    checksum: Decimal,
    image_digest: String,
}

impl From<LtxImage> for WireImage {
    fn from(image: LtxImage) -> Self {
        Self {
            epoch: image.epoch.into(),
            txid: image.txid.into(),
            checksum: image.checksum.into(),
            image_digest: hex::encode(image.image_digest),
        }
    }
}

impl WireImage {
    fn decode(self) -> Result<LtxImage, ProtocolError> {
        Ok(LtxImage {
            epoch: self.epoch.value()?,
            txid: self.txid.value()?,
            checksum: self.checksum.value()?,
            image_digest: decode_hex(&self.image_digest)?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFile {
    offset: Decimal,
    length: Decimal,
    min_txid: Decimal,
    max_txid: Decimal,
    page_count: Decimal,
    decoded_bytes: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    Framing,
    Protocol,
    Header,
    Digest,
    Identity,
    FileTable,
    Chain,
    Limit,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid Raft update envelope: {self:?}")
    }
}

impl std::error::Error for ProtocolError {}
