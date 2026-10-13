// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/js/r2_ops.rs.

//! Shared Blob host state, used by native bindings and the transitional adapter.

use super::*;
use std::sync::atomic::Ordering;

const IDLE: std::time::Duration = std::time::Duration::from_secs(3600);

fn reap_puts() {
    let stale = {
        let mut puts = puts().lock().unwrap();
        let ids = puts
            .iter()
            .filter(|(_, put)| {
                // A write with a call in flight is busy, not abandoned.
                put.try_lock()
                    .is_ok_and(|put| put.touched.elapsed() >= IDLE)
            })
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        ids.into_iter()
            .filter_map(|id| puts.remove(&id))
            .collect::<Vec<_>>()
    };
    for put in stale {
        asyncrt::spawn(async move {
            let Some(mut upload) = put.lock().await.upload.take() else {
                return;
            };
            if let Err(error) = upload.abort().await {
                tracing::warn!(%error, "abandoned Blob streaming write could not be aborted");
            }
        });
    }
}

pub(crate) async fn begin(
    bucket_name: String,
    key: String,
    request_json: String,
) -> Result<String, String> {
    let put = open_put(bucket_name, key, &request_json)?;
    reap_puts();
    let put_id = super::NEXT_ID.fetch_add(1, Ordering::Relaxed);
    puts()
        .lock()
        .unwrap()
        .insert(put_id, Arc::new(tokio::sync::Mutex::new(put)));
    Ok(put_id.to_string())
}

pub(crate) async fn chunk(put_id: u64, chunk: Vec<u8>) -> Result<String, String> {
    let put = puts()
        .lock()
        .unwrap()
        .get(&put_id)
        .cloned()
        .ok_or_else(|| format!("Blob streaming write {put_id} is not open"))?;
    // A failed chunk leaves the write open, so `__blob_put_end` can
    // abort the parts already on the store.
    let mut put = put.lock().await;
    put.touched = idle_stamp();
    put.push(chunk).await.map(|()| String::new())
}

pub(crate) async fn end(put_id: u64, abort: bool) -> Result<String, String> {
    let put = puts().lock().unwrap().remove(&put_id);
    let Some(put) = put else {
        return Err(format!("Blob streaming write {put_id} is not open"));
    };
    let mut put = put.lock().await;
    if abort {
        if let Some(mut upload) = put.upload.take() {
            let _ = upload.abort().await;
        }
        return Ok(serde_json::json!({ "stored": false }).to_string());
    }
    put.finish().await
}
