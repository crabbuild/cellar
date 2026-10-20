//! SQLite produces expected bytes; the LTX fixture encodes their exact page delta.
#![allow(clippy::disallowed_methods)]

use anyhow::Result;
use cellar_ltx::apply::{ImageState, ImageUpdate, LtxSegment};
use cellar_ltx::generation::{GenerationPin, ImageId};
use cellar_ltx::ltx::{self, Header};
use cellar_ltx::{CHECKSUM_FLAG, TXID};
use sha2::{Digest, Sha256};

pub fn id(value: u8) -> ImageId {
    ImageId::new([value; 16]).unwrap()
}

pub fn image(before: Option<&[u8]>, sql: &str) -> Result<Vec<u8>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("db.sqlite");
    if let Some(before) = before {
        std::fs::write(&path, before)?;
    }
    let connection = rusqlite::Connection::open(&path)?;
    connection.execute_batch("PRAGMA page_size=512; PRAGMA journal_mode=DELETE;")?;
    connection.execute_batch(sql)?;
    drop(connection);
    Ok(std::fs::read(path)?)
}

pub fn rolling(image: &[u8]) -> u64 {
    image
        .as_chunks::<512>()
        .0
        .iter()
        .enumerate()
        .fold(CHECKSUM_FLAG, |sum, (i, p)| {
            CHECKSUM_FLAG | (sum ^ ltx::checksum_page(i as u32 + 1, p))
        })
}

pub fn state(image: &[u8], txid: u64, checksums: bool) -> ImageState {
    if txid == 0 {
        return ImageState::empty(1).unwrap();
    }
    ImageState {
        epoch: 1,
        txid: TXID(txid),
        checksum: if checksums { rolling(image) } else { 0 },
        page_size: 512,
        length: image.len() as u64,
        digest: Sha256::digest(image).into(),
    }
}

pub struct Cut {
    pub bytes: Vec<u8>,
    pub base: ImageState,
    pub result: ImageState,
    pub pages: Vec<(u32, Vec<u8>)>,
}

impl Cut {
    pub fn new(before: &[u8], after: &[u8], base_txid: u64, checksums: bool, block: bool) -> Self {
        let pages: Vec<_> = after
            .as_chunks::<512>()
            .0
            .iter()
            .enumerate()
            .filter(|(i, p)| before.get(i * 512..i * 512 + 512) != Some(p.as_slice()))
            .map(|(i, p)| (i as u32 + 1, p.to_vec()))
            .collect();
        let base = state(before, base_txid, checksums);
        let result = state(after, base_txid + 1, checksums);
        let header = Header {
            version: ltx::VERSION,
            flags: if checksums {
                0
            } else {
                ltx::HEADER_FLAG_NO_CHECKSUM
            },
            page_size: 512,
            commit: (after.len() / 512) as u32,
            min_txid: TXID(base_txid + 1),
            max_txid: TXID(base_txid + 1),
            pre_apply_checksum: if checksums && base_txid != 0 {
                rolling(before)
            } else {
                0
            },
            ..Default::default()
        };
        let bytes = if block {
            ltx::encode_file_v0_5_2(&header, &pages, result.checksum).unwrap()
        } else {
            ltx::encode_file(&header, &pages, result.checksum).unwrap()
        };
        Self {
            bytes,
            base,
            result,
            pages,
        }
    }

    pub fn update(&self) -> ImageUpdate<'_> {
        ImageUpdate {
            base: self.base,
            result: self.result,
            segments: vec![LtxSegment {
                bytes: &self.bytes,
                min_txid: TXID(self.base.txid.0 + 1),
                max_txid: self.result.txid,
                page_count: self.pages.len(),
                decoded_bytes: self.pages.len() as u64 * 512,
            }],
        }
    }
}

pub fn read(pin: &GenerationPin) -> Result<Vec<u8>> {
    let mut reader = pin.open_reader()?;
    let mut bytes = Vec::new();
    while (bytes.len() as u64) < reader.length() {
        let amount = (reader.length() - bytes.len() as u64).min(64 * 1024) as usize;
        bytes.extend(reader.read_exact_at(bytes.len() as u64, amount)?);
    }
    Ok(bytes)
}

pub fn query(pin: GenerationPin) -> Result<cellar::storage::ImmutableQuery> {
    cellar::storage::open_immutable_generation(
        pin,
        cellar::storage::ImmutableQueryOptions {
            sqlite_vec: false,
            services: cellar_ltx::generation::ReadServices {
                now_unix_millis: std::sync::Arc::new(|| 0),
                fill_random: std::sync::Arc::new(|bytes| bytes.fill(17)),
            },
        },
    )
}

pub fn save_marker(root: &std::path::Path, pin: &GenerationPin) -> Result<()> {
    use std::io::Write as _;
    let state = pin.state();
    let bytes = serde_json::to_vec(
        &serde_json::json!({"id":hex::encode(pin.id().bytes()),"epoch":state.epoch,"txid":state.txid.0,"checksum":state.checksum,"page_size":state.page_size,"length":state.length,"digest":hex::encode(state.digest)}),
    )?;
    let temporary = root.join("head.next");
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(temporary, root.join("head.json"))?;
    std::fs::File::open(root)?.sync_all()?;
    Ok(())
}

pub fn load_marker(root: &std::path::Path) -> Result<(ImageId, ImageState)> {
    let value: serde_json::Value = serde_json::from_slice(&std::fs::read(root.join("head.json"))?)?;
    let identity = hex::decode(value["id"].as_str().unwrap())?;
    let digest = hex::decode(value["digest"].as_str().unwrap())?;
    Ok((
        ImageId::new(identity.try_into().unwrap())?,
        ImageState {
            epoch: value["epoch"].as_u64().unwrap(),
            txid: TXID(value["txid"].as_u64().unwrap()),
            checksum: value["checksum"].as_u64().unwrap(),
            page_size: value["page_size"].as_u64().unwrap() as u32,
            length: value["length"].as_u64().unwrap(),
            digest: digest.try_into().unwrap(),
        },
    ))
}
