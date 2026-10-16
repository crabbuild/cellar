//! Verify a retained CLB1 envelope into the bounded recovery spool.
//! No row escapes until the complete envelope and unique identities verify.
use super::recovery_rows::{Rows, SortedRows};
use anyhow::{ensure, Context, Result};
use cellar_ltx::{
    ltx::{Crc64, Header, HEADER_SIZE},
    HostFile, TXID,
};
use sha2::{Digest, Sha256};
use std::sync::atomic::Ordering;

const CHUNK_BYTES: usize = 128 << 10;
const TRAILER_BYTES: u64 = 16;

pub(super) fn decode(mut file: HostFile, mut rows: Rows) -> Result<SortedRows> {
    let cancelled = rows.cancellation();
    let length = file.file_len()?;
    ensure!(
        length >= TRAILER_BYTES,
        "bundle is shorter than its trailer"
    );
    let trailer = exact(&mut file, length - TRAILER_BYTES, TRAILER_BYTES as usize)?;
    ensure!(&trailer[12..] == b"CLB1", "bundle has bad magic");
    let expected = u64::from_le_bytes(trailer[..8].try_into()?);
    let footer_bytes = u32::from_le_bytes(trailer[8..12].try_into()?) as u64;
    let footer_end = length - TRAILER_BYTES;
    let footer_start = footer_end
        .checked_sub(footer_bytes)
        .context("bundle footer overruns object")?;
    let mut footer = footer_start;
    let mut next_payload = 0_u64;
    let mut count = 0_u64;
    let mut crc = Crc64::new();
    let mut parsed_footer = Sha256::new();
    while footer < footer_end {
        ensure!(!cancelled.load(Ordering::Relaxed), "bundle read cancelled");
        ensure!(footer_end - footer >= 2, "bundle has truncated row");
        let size = exact(&mut file, footer, 2)?;
        let cell_bytes = u16::from_le_bytes(size.as_slice().try_into()?) as usize;
        parsed_footer.update(&size);
        footer += 2;
        let descriptor_bytes = cell_bytes.checked_add(32).context("bundle row overflow")?;
        ensure!(
            descriptor_bytes as u64 <= footer_end - footer,
            "bundle has truncated descriptor"
        );
        let descriptor = exact(&mut file, footer, descriptor_bytes)?;
        parsed_footer.update(&descriptor);
        footer += descriptor_bytes as u64;
        let cell = std::str::from_utf8(&descriptor[..cell_bytes])?.to_owned();
        let value = |at| {
            u64::from_le_bytes(
                descriptor[at..at + 8]
                    .try_into()
                    .expect("checked descriptor"),
            )
        };
        let epoch = value(cell_bytes);
        let txid = value(cell_bytes + 8);
        let offset = value(cell_bytes + 16);
        let payload_bytes = value(cell_bytes + 24);
        let end = offset
            .checked_add(payload_bytes)
            .context("bundle row overflow")?;
        ensure!(
            offset == next_payload && end <= footer_start,
            "bundle rows do not partition payload"
        );
        ensure!(
            !cell.is_empty() && epoch > 0 && txid > 0,
            "bundle row identity is empty or zero"
        );
        ensure!(
            payload_bytes >= HEADER_SIZE as u64,
            "bundle row has truncated L0 header"
        );
        let prefix = exact(&mut file, offset, HEADER_SIZE)?;
        let header = Header::parse(&prefix)?;
        header.validate()?;
        ensure!(
            header.min_txid == TXID(txid) && header.max_txid == TXID(txid),
            "bundle row transaction does not match L0 payload"
        );
        // insert_file hashes the exact bytes included in the envelope CRC,
        // then checks that the actual spool copy has that same identity.
        rows.insert_file(
            (cell, epoch, txid),
            &mut file,
            offset,
            payload_bytes,
            &prefix,
            &mut crc,
        )?;
        next_payload = end;
        count = count.checked_add(1).context("bundle row count overflow")?;
    }
    ensure!(
        count > 0 && next_payload == footer_start,
        "bundle has missing rows or unindexed payload"
    );
    let mut footer_digest = Sha256::new();
    let mut position = footer_start;
    while position < footer_end {
        ensure!(!cancelled.load(Ordering::Relaxed), "bundle read cancelled");
        let count = usize::try_from((footer_end - position).min(CHUNK_BYTES as u64))?;
        let bytes = exact(&mut file, position, count)?;
        crc.update(&bytes);
        footer_digest.update(&bytes);
        position += count as u64;
    }
    ensure!(
        footer_digest.finalize() == parsed_footer.finalize(),
        "bundle footer changed while reading"
    );
    crc.update(&trailer[8..]);
    ensure!(crc.sum64() == expected, "bundle checksum mismatch");
    ensure!(
        exact(&mut file, length - TRAILER_BYTES, TRAILER_BYTES as usize)? == trailer,
        "bundle trailer changed while reading"
    );
    ensure!(
        file.file_len()? == length,
        "bundle length changed while reading"
    );
    ensure!(!cancelled.load(Ordering::Relaxed), "bundle read cancelled");
    rows.finish()?.unique()
}

fn exact(file: &mut HostFile, offset: u64, count: usize) -> Result<Vec<u8>> {
    let bytes = file.read_exact_at(offset, count)?;
    ensure!(bytes.len() == count, "bundle scratch short read");
    Ok(bytes)
}

#[cfg(test)]
#[path = "bundle_snapshot/tests.rs"]
mod tests;
