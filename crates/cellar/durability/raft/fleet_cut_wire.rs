//! Fixed bounded publication statement inside the authenticated quorum body.
use super::*;
use crate::raft::host::{FleetPublishedImage, Publication};
use cellar_logic::consensus::{LogPosition, Term};
use cellar_ltx::{apply::ImageState, TXID};

pub(super) const MAX_BODY: usize = 85;

pub(super) fn encode(publication: &Publication) -> Vec<u8> {
    match publication {
        Publication::Empty => vec![0],
        Publication::Pending => vec![1],
        Publication::Image(image) => {
            let mut bytes = Vec::with_capacity(MAX_BODY);
            bytes.push(2);
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
            bytes.extend_from_slice(&image.digest());
            bytes
        }
    }
}

pub(super) fn decode(bytes: &[u8]) -> Result<Publication, TransportError> {
    match bytes {
        [0] => Ok(Publication::Empty),
        [1] => Ok(Publication::Pending),
        bytes if bytes.len() == MAX_BODY && bytes[0] == 2 => {
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
            let image = FleetPublishedImage::new(
                LogPosition {
                    index: LogIndex::new(word(1)),
                    term: Term::new(word(9)),
                },
                ImageState {
                    epoch: word(17),
                    txid: TXID(word(25)),
                    checksum: word(33),
                    page_size: u32::from_be_bytes([bytes[41], bytes[42], bytes[43], bytes[44]]),
                    length: word(45),
                    digest,
                },
            )
            .map_err(|_| TransportError::Protocol)?;
            Ok(Publication::Image(image))
        }
        _ => Err(TransportError::Protocol),
    }
}
