// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An anonymous, bounded part spool. Only completion obtains SQLite's writer;
//! network/source waits and scratch writes cannot hold its lease-renewal lock.

use super::*;
use crate::object_multipart::StreamedPart;
use object_store::multipart::PartId;
use sha2::{Digest, Sha256};

struct Spool {
    file: cellar_ltx::HostFile,
    expected: usize,
    written: usize,
    hash: Sha256,
    failed: bool,
    _capacity: Arc<dyn Send + Sync>,
}

struct Part {
    store: LocalStore,
    key: String,
    id: String,
    index: i64,
    spool: Arc<Mutex<Spool>>,
}

pub(super) async fn begin(
    store: LocalStore,
    key: String,
    id: String,
    index: usize,
    length: u64,
    capacity: Arc<dyn Send + Sync>,
) -> object_store::Result<Box<dyn StreamedPart>> {
    let index = super::multipart::check_part(index)?;
    let length = usize::try_from(length).map_err(db_error)?;
    i32::try_from(length).map_err(db_error)?;
    let fs = crate::asyncrt::fs();
    store_blocking!(move || {
        // Refuse an absent/foreign upload before allocating scratch storage.
        LocalStore::upload_attributes(&store.connect_reader()?, &key, &id)?;
        let file = fs
            .temporary_file(store.database.parent())
            .map_err(db_error)?;
        Ok(Box::new(Part {
            store,
            key,
            id,
            index,
            spool: Arc::new(Mutex::new(Spool {
                file,
                expected: length,
                written: 0,
                hash: Sha256::new(),
                failed: false,
                _capacity: capacity,
            })),
        }) as Box<dyn StreamedPart>)
    })
    .await
    .map_err(db_error)?
}

#[async_trait]
impl StreamedPart for Part {
    async fn write(&mut self, bytes: Bytes) -> object_store::Result<()> {
        if bytes.len() > crate::application::blob::part::PART_CHUNK_BYTES {
            return Err(message_error("streamed part chunk exceeds 64 KiB"));
        }
        let spool = self.spool.clone();
        store_blocking!(move || {
            // This allocation owns both its capacity and the entered byte
            // buffer even if the awaiting event/part has already been dropped.
            let mut spool = spool.lock().unwrap();
            if spool.failed || bytes.len() > spool.expected - spool.written {
                return Err(message_error(
                    "streamed part interrupted or exceeds declared length",
                ));
            }
            spool.failed = true;
            spool.file.write_all_window(&bytes).map_err(db_error)?;
            spool.hash.update(&bytes);
            spool.written += bytes.len();
            spool.failed = false;
            Ok(())
        })
        .await
        .map_err(db_error)?
    }

    async fn complete(self: Box<Self>) -> object_store::Result<PartId> {
        store_blocking!(move || {
            let mut spool = self.spool.lock().unwrap();
            if spool.failed || spool.written != spool.expected {
                return Err(message_error(
                    "streamed part requires its exact declared length",
                ));
            }
            let length = spool.written;
            let receipt = hex::encode(spool.hash.clone().finalize());
            self.store
                .install_part_sync(&self.key, &self.id, self.index, length, receipt, |part| {
                    let mut buffer = vec![
                        0;
                        READ_CHUNK_BYTES
                            .min(crate::application::blob::part::PART_CHUNK_BYTES)
                    ];
                    let mut offset = 0;
                    while offset < length {
                        let count = buffer.len().min(length - offset);
                        spool
                            .file
                            .read_exact_into_at(offset as u64, &mut buffer[..count])
                            .map_err(db_error)?;
                        part.write_all_at(&buffer[..count], offset)
                            .map_err(db_error)?;
                        offset += count;
                    }
                    Ok(())
                })
        })
        .await
        .map_err(db_error)?
    }
}
