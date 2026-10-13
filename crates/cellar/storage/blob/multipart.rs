// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/js/r2_ops.rs.

//! Shared Blob host state, used by native bindings and the transitional adapter.

use super::*;
use std::sync::atomic::Ordering;
const IDLE: std::time::Duration = std::time::Duration::from_secs(3600);

const PART_BACKLOG: usize = 256 << 20;

struct UploadEntry {
    bucket_name: String,
    key: String,
    upload: UploadGuard,
    /// Part numbers already handed to the store, in that order. The store
    /// owns the part bookkeeping and concatenates the parts in the order
    /// it received them, so this is the object's real shape.
    flushed: Vec<u32>,
    /// Parts that arrived before the one in front of them, held until
    /// their turn. A caller uploading parts sequentially never fills this.
    pending: BTreeMap<u32, Vec<u8>>,
    /// Bytes in `pending`, against [`PART_BACKLOG`].
    held: usize,
    /// The record the object was opened with, answered again by the
    /// completion the way Blob answers one.
    envelope: Envelope,
    /// Bytes handed to the store, which is the completed object's size.
    written: u64,
    touched: Instant,
}

impl UploadEntry {
    /// The next part number the store can take. Nothing may go before
    /// part 1, because a part that arrived first is not necessarily the
    /// first part; everything after that is one past what last went.
    fn next(&self) -> u32 {
        self.flushed.last().map_or(1, |last| last + 1)
    }

    /// Hand the store every held part that is now in turn.
    async fn drain(&mut self) -> Result<(), String> {
        loop {
            let next = self.next();
            let Some(bytes) = self.pending.remove(&next) else {
                return Ok(());
            };
            self.held -= bytes.len();
            let size = bytes.len() as u64;
            self.upload
                .put_part(bytes.into())
                .await
                .map_err(|error| format!("Blob multipart part {next} of {}: {error}", self.key))?;
            self.written += size;
            self.flushed.push(next);
        }
    }
}

static UPLOADS: OnceLock<Open<UploadEntry>> = OnceLock::new();

fn uploads() -> &'static Open<UploadEntry> {
    UPLOADS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Abort every upload nothing has touched for [`IDLE`]. A dropped handle
/// leaves the parts on the store, so the abort is issued rather than left
/// to the bucket's lifecycle rules.
fn reap_uploads() {
    let stale = {
        let mut uploads = uploads().lock().unwrap();
        let ids = uploads
            .iter()
            .filter(|(_, entry)| {
                // An upload with a part in flight is busy, not abandoned.
                entry
                    .try_lock()
                    .is_ok_and(|entry| entry.touched.elapsed() >= IDLE)
            })
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        ids.into_iter()
            .filter_map(|id| uploads.remove(&id))
            .collect::<Vec<_>>()
    };
    for entry in stale {
        asyncrt::spawn(async move {
            if let Err(error) = entry.lock().await.upload.abort().await {
                tracing::warn!(%error, "abandoned Blob multipart upload could not be aborted");
            }
        });
    }
}

pub(crate) fn is_open(id: u64) -> bool {
    uploads().lock().unwrap().contains_key(&id)
}
pub(crate) async fn begin(
    bucket_name: String,
    key: String,
    request: PutRequest,
) -> Result<String, String> {
    reap_uploads();
    // A multipart object carries no md5, on Blob or here, so nothing is
    // computed over parts that were never seen whole.
    let envelope = Envelope {
        custom: request.custom,
        http: request.http,
        checksums: BTreeMap::new(),
        storage_class: request.storage_class,
    };
    let upload = store()?
        .begin_multipart(&blob_key(&bucket_name, &key), &envelope.write())
        .await
        .map_err(|error| error.to_string())?;
    let upload_id = super::NEXT_ID.fetch_add(1, Ordering::Relaxed);
    uploads().lock().unwrap().insert(
        upload_id,
        Arc::new(tokio::sync::Mutex::new(UploadEntry {
            bucket_name,
            key,
            upload: UploadGuard::new(upload),
            flushed: Vec::new(),
            pending: BTreeMap::new(),
            held: 0,
            envelope,
            written: 0,
            touched: idle_stamp(),
        })),
    );
    Ok(upload_id.to_string())
}
pub(crate) async fn resume(
    bucket_name: String,
    key: String,
    upload_id: String,
) -> Result<String, String> {
    let parsed = upload_id.parse::<u64>().ok();
    let entry = parsed.and_then(|id| uploads().lock().unwrap().get(&id).cloned());
    let entry = match entry {
        Some(entry) => Some(entry.lock_owned().await),
        None => None,
    };
    match entry.as_deref() {
        Some(entry) if entry.bucket_name == bucket_name && entry.key == key => {
            Ok(upload_id.clone())
        }
        Some(_) => Err(format!(
            "Blob multipart upload {upload_id} was opened for another key or binding"
        )),
        // The handle the object store hands out cannot be re-derived
        // from an id, so an upload outlives only the node that opened
        // it — and only until that node restarts.
        None => Err(format!(
            "Blob multipart upload {upload_id} is not open on this node: cellar can resume an \
                 upload within the node that created it, not across nodes or restarts"
        )),
    }
}
pub(crate) async fn part(
    upload_id: u64,
    part_number: u32,
    bytes: Vec<u8>,
) -> Result<String, String> {
    let entry = uploads()
        .lock()
        .unwrap()
        .get(&upload_id)
        .cloned()
        .ok_or_else(|| format!("Blob multipart upload {upload_id} is not open"))?;
    let mut entry = entry.lock().await;
    entry.touched = idle_stamp();
    let result = async {
        if entry.flushed.contains(&part_number) {
            return Err(format!(
                "Blob multipart part {part_number} of upload {upload_id} was already handed to \
                     the object store, which appends parts and cannot rewrite one: cellar \
                     replaces a part that is still waiting its turn, not one already written. \
                     Send the replacement before the part in front of it, or open the upload \
                     again"
            ));
        }
        // Blob replaces a part when its number is uploaded again. A part
        // still waiting its turn is replaced in place, so the bytes it
        // displaces leave the backlog with it: counting the replacement
        // without discounting the replaced would grow `held` past what
        // is actually held and refuse an upload well inside the limit.
        let replaced = entry.pending.get(&part_number).map_or(0, Vec::len);
        let held = entry.held - replaced + bytes.len();
        if held > PART_BACKLOG {
            return Err(format!(
                "Blob multipart upload {upload_id} is holding {} bytes of parts waiting for \
                     part {}: cellar hands parts to the object store in ascending order, so \
                     upload the parts in front of the ones running ahead",
                entry.held,
                entry.next(),
            ));
        }
        entry.held = held;
        entry.pending.insert(part_number, bytes);
        entry.drain().await
    }
    .await;
    result?;
    Ok(serde_json::json!({ "partNumber": part_number }).to_string())
}
pub(crate) async fn finish(upload_id: u64, claimed: Vec<u32>) -> Result<String, String> {
    let Some(entry) = uploads().lock().unwrap().remove(&upload_id) else {
        return Err(format!("Blob multipart upload {upload_id} is not open"));
    };
    let mut entry = entry.lock().await;
    let result = complete(&mut entry, upload_id, claimed).await;
    if result.is_err() {
        if let Err(error) = entry.upload.abort().await {
            tracing::warn!(
                %error,
                upload_id,
                "mismatched Blob multipart upload could not be aborted"
            );
        }
    }
    result
}
async fn complete(
    entry: &mut UploadEntry,
    upload_id: u64,
    claimed: Vec<u32>,
) -> Result<String, String> {
    if claimed.len() < entry.flushed.len() || claimed[..entry.flushed.len()] != entry.flushed {
        return Err(format!(
            "Blob multipart complete of upload {upload_id} names {claimed:?}, which does not start \
             with the parts already written in order ({:?}); cellar hands parts to the object \
             store as they arrive, so a completion may add to that order but not rewrite it",
            entry.flushed
        ));
    }
    for number in &claimed[entry.flushed.len()..] {
        let Some(bytes) = entry.pending.remove(number) else {
            return Err(format!(
                "Blob multipart complete of upload {upload_id} names part {number}, which was never \
                 uploaded"
            ));
        };
        entry.held -= bytes.len();
        let size = bytes.len() as u64;
        entry.upload.put_part(bytes.into()).await.map_err(|error| {
            format!("Blob multipart part {number} of upload {upload_id}: {error}")
        })?;
        entry.written += size;
        entry.flushed.push(*number);
    }
    let result = entry
        .upload
        .complete()
        .await
        .map_err(|error| format!("Blob multipart complete of upload {upload_id}: {error}"))?;
    let meta = BlobMeta {
        size: entry.written,
        etag: result.e_tag.clone(),
        version: result.version,
        cas: None,
        uploaded_ms: asyncrt::wall_ms(),
        attributes: entry.envelope.write(),
    };
    Ok(object_json(&entry.key, &meta, None).to_string())
}

pub(crate) async fn abort(upload_id: u64) -> Result<String, String> {
    let entry = uploads().lock().unwrap().remove(&upload_id);
    if let Some(entry) = entry {
        let mut entry = entry.lock().await;
        entry
            .upload
            .abort()
            .await
            .map_err(|error| format!("Blob multipart abort of upload {upload_id}: {error}"))?;
    }
    Ok(String::new())
}
