// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independent physical locations for storage faults, never a production reader.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context, Result};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

pub struct Frame {
    pub path: PathBuf,
    pub offset: usize,
    pub length: usize,
}

pub fn find(directory: &Path, key: [u8; 32]) -> Result<Option<Frame>> {
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path.extension().is_none_or(|s| s != "pack") {
            continue;
        }
        let bytes = std::fs::read(&path)?;
        ensure!(bytes.len() >= 60 && bytes.len() <= 96 * (16 * 1024 + 36) + 60);
        ensure!(&bytes[..8] == b"CLRCMP01");
        let end = bytes.len() - 52;
        ensure!(&bytes[end..end + 8] == b"CLRCMS01");
        let count = u32::from_be_bytes(bytes[end + 8..end + 12].try_into()?) as usize;
        ensure!((1..=96).contains(&count));
        let mut cursor = 8;
        for _ in 0..count {
            ensure!(cursor + 36 <= end);
            let digest = &bytes[cursor..cursor + 32];
            let length = u32::from_be_bytes(bytes[cursor + 32..cursor + 36].try_into()?) as usize;
            ensure!((1..=16 * 1024).contains(&length) && cursor + 36 + length <= end);
            if digest == key {
                return Ok(Some(Frame {
                    path,
                    offset: cursor + 36,
                    length,
                }));
            }
            cursor += 36 + length;
        }
        ensure!(cursor == end);
    }
    Ok(None)
}

pub fn locate(directory: &Path, key: [u8; 32]) -> Result<Frame> {
    find(directory, key)?.context("selected logical page in physical manifest pack")
}

pub fn read(directory: &Path, key: [u8; 32]) -> Result<Vec<u8>> {
    let frame = locate(directory, key)?;
    let bytes = std::fs::read(&frame.path)?;
    let page = bytes[frame.offset..frame.offset + frame.length].to_vec();
    ensure!(page_digest(&page) == key);
    Ok(page)
}

/// Authenticate a completed fixture container, including its independent index
/// encoding. Pending files may be inspected after a child has stopped writing.
pub fn keys(path: &Path) -> Result<Vec<[u8; 32]>> {
    let bytes = std::fs::read(path)?;
    ensure!(bytes.len() >= 60 && bytes.len() <= 96 * (16 * 1024 + 36) + 60);
    ensure!(&bytes[..8] == b"CLRCMP01");
    let end = bytes.len() - 52;
    ensure!(&bytes[end..end + 8] == b"CLRCMS01");
    let count = u32::from_be_bytes(bytes[end + 8..end + 12].try_into()?) as usize;
    ensure!((1..=96).contains(&count));
    let mut cursor = 8;
    let mut keys = Vec::with_capacity(count);
    let mut pages = Vec::with_capacity(count);
    for _ in 0..count {
        ensure!(cursor + 36 <= end);
        let key: [u8; 32] = bytes[cursor..cursor + 32].try_into()?;
        let length = u32::from_be_bytes(bytes[cursor + 32..cursor + 36].try_into()?) as usize;
        ensure!((1..=16 * 1024).contains(&length) && cursor + 36 + length <= end);
        let page = bytes[cursor + 36..cursor + 36 + length].to_vec();
        ensure!(page_digest(&page) == key);
        keys.push(key);
        pages.push(page);
        cursor += 36 + length;
    }
    ensure!(cursor == end);
    let (_, expected) = encode(&pages)?;
    ensure!(bytes == expected);
    Ok(keys)
}

/// Valid closed-store fixtures for interrupted compaction and malformed-input
/// tests. The independent encoder does not call any PageStore implementation.
pub fn encode(pages: &[Vec<u8>]) -> Result<(String, Vec<u8>)> {
    ensure!((1..=96).contains(&pages.len()));
    let mut bytes = b"CLRCMP01".to_vec();
    let mut records = Vec::new();
    for page in pages {
        ensure!((1..=16 * 1024).contains(&page.len()));
        let key = page_digest(page);
        ensure!(!records.iter().any(|(other, _, _)| other == &key));
        let offset = bytes.len() as u64;
        let length = page.len() as u32;
        bytes.extend_from_slice(&key);
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(page);
        records.push((key, offset, length));
    }
    let end = bytes.len() as u64;
    let count = records.len() as u32;
    let mut hash = Sha256::new();
    hash.update(b"cellar manifest pack 1\0");
    hash.update(count.to_be_bytes());
    hash.update(end.to_be_bytes());
    for (key, offset, length) in records {
        hash.update(key);
        hash.update(offset.to_be_bytes());
        hash.update(length.to_be_bytes());
    }
    let id: [u8; 32] = hash.finalize().into();
    bytes.extend_from_slice(b"CLRCMS01");
    bytes.extend_from_slice(&count.to_be_bytes());
    bytes.extend_from_slice(&end.to_be_bytes());
    bytes.extend_from_slice(&id);
    Ok((format!("{}.pack", hex::encode(id)), bytes))
}

fn page_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"cellar-cell-group-manifest-page-1\0");
    hash.update(bytes);
    hash.finalize().into()
}
