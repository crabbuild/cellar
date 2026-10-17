use super::*;
use cellar_ltx::{
    bundle::{self, BundleEntry},
    ltx, DirectFileSystem, FileSystem,
};
use std::sync::Arc;

fn entry(txid: u64) -> Result<BundleEntry> {
    let header = Header {
        version: ltx::VERSION,
        flags: ltx::HEADER_FLAG_NO_CHECKSUM,
        page_size: 512,
        commit: 1,
        min_txid: TXID(txid),
        max_txid: TXID(txid),
        ..Default::default()
    };
    Ok(BundleEntry {
        cell: "cell".into(),
        cell_epoch: 1,
        txid,
        bytes: ltx::encode_file(&header, &[(1, vec![txid as u8; 512])], 0)?,
    })
}

fn staged(bytes: &[u8]) -> Result<(tempfile::TempDir, HostFile, Rows)> {
    let directory = tempfile::tempdir()?;
    let mut file = DirectFileSystem.temporary_file(Some(directory.path()))?;
    file.write_all(bytes)?;
    let rows = Rows::new(Arc::new(DirectFileSystem), directory.path().to_owned());
    Ok((directory, file, rows))
}

fn rehash(bytes: &mut [u8]) {
    let end = bytes.len() - 16;
    let mut crc = Crc64::new();
    crc.update(&bytes[..end]);
    crc.update(&bytes[end + 8..]);
    bytes[end..end + 8].copy_from_slice(&crc.sum64().to_le_bytes());
}

#[test]
fn checked_bundle_reader_preserves_exact_rows_and_numeric_order() -> Result<()> {
    let entries = [entry(11)?, entry(2)?, entry(9)?];
    let bytes = bundle::encode(&entries)?;
    let (_directory, file, rows) = staged(&bytes)?;
    let mut rows = decode(file, rows)?;
    let batch = rows.next_batch()?;
    assert_eq!(
        batch.keys().map(|key| key.2).collect::<Vec<_>>(),
        vec![2, 9, 11]
    );
    for entry in entries {
        assert_eq!(
            batch
                .get(&(entry.cell, entry.cell_epoch, entry.txid))
                .map(AsRef::as_ref),
            Some(entry.bytes.as_slice())
        );
    }
    assert!(rows.next_batch()?.is_empty());
    Ok(())
}

#[test]
fn malformed_or_duplicate_envelopes_never_return_a_gather() -> Result<()> {
    let entries = [entry(2)?, entry(9)?];
    let original = bundle::encode(&entries)?;
    let first = entries[0].bytes.len();
    let footer = first + entries[1].bytes.len();
    let row = 2 + "cell".len() + 32;
    let mut cases = Vec::new();
    let mut bytes = original.clone();
    bytes[HEADER_SIZE + 1] ^= 1;
    cases.push(bytes);
    let mut bytes = original.clone();
    bytes[footer + 2 + 4..footer + 2 + 4 + 8].fill(0);
    rehash(&mut bytes);
    cases.push(bytes);
    let mut bytes = original.clone();
    bytes[footer + 2 + 4 + 16] = 1;
    rehash(&mut bytes);
    cases.push(bytes);
    let mut bytes = original.clone();
    bytes[footer + 2 + 4 + 8..footer + 2 + 4 + 16].copy_from_slice(&7_u64.to_le_bytes());
    rehash(&mut bytes);
    cases.push(bytes);
    let mut bytes = original.clone();
    assert_eq!(entries[0].bytes.len(), entries[1].bytes.len());
    bytes[first..footer].copy_from_slice(&entries[0].bytes);
    bytes[footer + row + 2 + 4 + 8..footer + row + 2 + 4 + 16]
        .copy_from_slice(&2_u64.to_le_bytes());
    rehash(&mut bytes);
    cases.push(bytes);
    let mut bytes = original.clone();
    let end = bytes.len();
    bytes[end - 1] ^= 1;
    cases.push(bytes);
    for bytes in cases {
        assert!(
            bundle::decode_rows(&bytes).is_err(),
            "fixture must violate the original format contract"
        );
        let (_directory, file, rows) = staged(&bytes)?;
        assert!(decode(file, rows).is_err());
    }
    Ok(())
}

#[test]
fn source_changes_between_hash_and_copy_cannot_enter_the_gather() -> Result<()> {
    use cellar_ltx::HostFileIo;

    struct Changed {
        inner: HostFile,
        payload_reads: usize,
        length: usize,
    }

    impl HostFileIo for Changed {
        fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
            self.inner.write_all(bytes)
        }

        fn read_exact_at(&mut self, offset: u64, length: usize) -> std::io::Result<Vec<u8>> {
            let mut bytes = self.inner.read_exact_at(offset, length)?;
            if offset == 0 && length == self.length {
                self.payload_reads += 1;
                if self.payload_reads == 2 {
                    bytes[HEADER_SIZE] ^= 1;
                }
            }
            Ok(bytes)
        }

        fn sync_all(&mut self) -> std::io::Result<()> {
            self.inner.sync_all()
        }

        fn file_len(&mut self) -> std::io::Result<u64> {
            self.inner.file_len()
        }
    }
    let entry = entry(2)?;
    let length = entry.bytes.len();
    let bytes = bundle::encode(&[entry])?;
    let (_directory, file, rows) = staged(&bytes)?;
    let file = HostFile::from_io(Changed {
        inner: file,
        payload_reads: 0,
        length,
    });
    let error = decode(file, rows)
        .err()
        .context("changed source was accepted")?;
    assert!(
        error.to_string().contains("changed while copying"),
        "{error}"
    );
    Ok(())
}

#[test]
fn a_duplicate_after_a_sort_run_cannot_expose_an_earlier_prefix() -> Result<()> {
    let entries = (2..=151).map(entry).collect::<Result<Vec<_>>>()?;
    let first_txid = entries[0].txid;
    let last_offset = entries
        .iter()
        .take(entries.len() - 1)
        .map(|entry| entry.bytes.len())
        .sum::<usize>();
    let footer = entries.iter().map(|entry| entry.bytes.len()).sum::<usize>();
    let last_descriptor = footer + (entries.len() - 1) * (2 + 4 + 32);
    let mut bytes = bundle::encode(&entries)?;
    assert_eq!(
        entries[0].bytes.len(),
        entries.last().expect("last entry").bytes.len()
    );
    bytes[last_offset..footer].copy_from_slice(&entries[0].bytes);
    bytes[last_descriptor + 2 + 4 + 8..last_descriptor + 2 + 4 + 16]
        .copy_from_slice(&first_txid.to_le_bytes());
    rehash(&mut bytes);
    assert!(bundle::decode_rows(&bytes).is_err());
    let (_directory, file, rows) = staged(&bytes)?;
    let error = decode(file, rows)
        .err()
        .context("duplicate returned a verified spool")?;
    assert!(
        error.to_string().contains("duplicate row identities"),
        "{error}"
    );
    Ok(())
}
