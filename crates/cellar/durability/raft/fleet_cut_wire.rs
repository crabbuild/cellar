//! Fixed bounded publication statement inside the authenticated quorum body.
use super::*;
use crate::application::consensus::ImageCommitment;
use crate::raft::host::{FleetImageState, FleetPublishedImage, Publication};
use cellar_logic::consensus::{LogPosition, Term};
use cellar_ltx::internal::{ExtentRootReference, ImageRoot, EXTENT_BYTES};

pub(super) const MAX_BODY: usize = 85;

pub(super) fn encode(publication: &Publication) -> Vec<u8> {
    match publication {
        Publication::Empty => vec![0],
        Publication::Pending => vec![1],
        Publication::Image(image) => {
            let mut bytes = Vec::with_capacity(MAX_BODY);
            bytes.push(match image.commitment() {
                ImageCommitment::Sha256(_) => 2,
                ImageCommitment::Extents(_) => 3,
            });
            for word in [
                image.position().index.get(),
                image.position().term.get(),
                image.epoch(),
                image.txid(),
                image.checksum(),
            ] {
                bytes.extend_from_slice(&word.to_be_bytes());
            }
            bytes.extend_from_slice(&image.page_size().to_be_bytes());
            bytes.extend_from_slice(&image.length().to_be_bytes());
            let digest = match image.commitment() {
                ImageCommitment::Sha256(digest) => digest,
                ImageCommitment::Extents(reference) => reference.root().bytes(),
            };
            bytes.extend_from_slice(&digest);
            bytes
        }
    }
}

pub(super) fn decode(bytes: &[u8]) -> Result<Publication, TransportError> {
    match bytes {
        [0] => Ok(Publication::Empty),
        [1] => Ok(Publication::Pending),
        bytes if bytes.len() == MAX_BODY && matches!(bytes[0], 2 | 3) => {
            let word = |i| {
                u64::from_be_bytes([
                    bytes[i],
                    bytes[i + 1],
                    bytes[i + 2],
                    bytes[i + 3],
                    bytes[i + 4],
                    bytes[i + 5],
                    bytes[i + 6],
                    bytes[i + 7],
                ])
            };
            let mut digest = [0; 32];
            digest.copy_from_slice(&bytes[53..85]);
            let commitment = if bytes[0] == 2 {
                ImageCommitment::Sha256(digest)
            } else {
                let length = 112u64
                    .checked_add(
                        32u64
                            .checked_mul(word(45).div_ceil(EXTENT_BYTES))
                            .ok_or(TransportError::Limit)?,
                    )
                    .ok_or(TransportError::Limit)?;
                ImageCommitment::Extents(
                    ExtentRootReference::new(
                        ImageRoot::from_bytes(digest),
                        u32::try_from(length).map_err(|_| TransportError::Limit)?,
                    )
                    .map_err(|_| TransportError::Protocol)?,
                )
            };
            let image = FleetPublishedImage::new_state(
                LogPosition {
                    index: LogIndex::new(word(1)),
                    term: Term::new(word(9)),
                },
                FleetImageState::new(
                    word(17),
                    word(25),
                    word(33),
                    u32::from_be_bytes([bytes[41], bytes[42], bytes[43], bytes[44]]),
                    word(45),
                    commitment,
                )
                .map_err(|_| TransportError::Protocol)?,
            )
            .map_err(|_| TransportError::Protocol)?;
            Ok(Publication::Image(image))
        }
        _ => Err(TransportError::Protocol),
    }
}
