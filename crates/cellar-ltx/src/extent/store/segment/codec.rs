// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independently authenticated framing. Payload identities are checked by the
//! ordinary extent/root readers; recovery never promotes framing into a seal.
use super::*;
use sha2::{Digest, Sha256};

pub(super) const HEADER: u64 = 64;
pub(super) const FRAME: u64 = 96;
pub(super) const FOOTER: u64 = 32;
pub(super) const OVERHEAD: u64 = FRAME + FOOTER;

pub(super) fn segment_header(id: u64) -> [u8; 64] {
    let mut bytes = [0; 64];
    bytes[..8].copy_from_slice(b"CELSEG01");
    bytes[8..16].copy_from_slice(&id.to_le_bytes());
    let hash = Sha256::digest(&bytes[..32]);
    bytes[32..].copy_from_slice(&hash);
    bytes
}

pub(super) fn frame(key: Key, offset: u64, length: u64) -> [u8; 96] {
    let mut bytes = [0; 96];
    bytes[..8].copy_from_slice(b"CELREC01");
    bytes[8..16].copy_from_slice(&offset.to_le_bytes());
    bytes[16..24].copy_from_slice(&length.to_le_bytes());
    let (kind, hash) = match key {
        Key::Object(id) => (1u64, id.bytes()),
        Key::Root(root) => (2, root.bytes()),
    };
    bytes[24..32].copy_from_slice(&kind.to_le_bytes());
    bytes[32..64].copy_from_slice(&hash);
    let hash = Sha256::digest(&bytes[..64]);
    bytes[64..].copy_from_slice(&hash);
    bytes
}

pub(super) fn footer(header: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"cellar-segment-record-end-1");
    hash.update(header);
    hash.finalize().into()
}

pub(super) fn decode(bytes: &[u8], offset: u64) -> Result<(Key, u64), StoreError> {
    if bytes.len() != FRAME as usize
        || &bytes[..8] != b"CELREC01"
        || bytes[64..] != Sha256::digest(&bytes[..64])[..]
        || u64::from_le_bytes(bytes[8..16].try_into().map_err(|_| StoreError::Layout)?) != offset
    {
        return Err(StoreError::Integrity);
    }
    let length = u64::from_le_bytes(bytes[16..24].try_into().map_err(|_| StoreError::Layout)?);
    let kind = u64::from_le_bytes(bytes[24..32].try_into().map_err(|_| StoreError::Layout)?);
    let hash = bytes[32..64].try_into().map_err(|_| StoreError::Layout)?;
    let key = match kind {
        1 => Key::Object(ExtentId::from_parts(
            hash,
            u32::try_from(length).map_err(|_| StoreError::Layout)?,
        )?),
        2 => {
            let root = ImageRoot::from_bytes(hash);
            ExtentRootReference::new(root, u32::try_from(length).map_err(|_| StoreError::Layout)?)?;
            Key::Root(root)
        }
        _ => return Err(StoreError::Layout),
    };
    Ok((key, length))
}
