// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! Internal-development fleet admission. State from other layouts is recreated.
use crate::bucket::Bucket;
use anyhow::{ensure, Context};

const KEY: &str = "wake/format.json";
const MAX_FORMAT_BYTES: usize = 8 << 10;
pub const FORMAT: u8 = 1;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FormatRecord {
    system: String,
    format: u8,
    peer_protocol: u16,
    native_host_protocol: u32,
    event_delivery: u8,
}

fn current() -> FormatRecord {
    FormatRecord {
        system: "cellar".into(),
        format: FORMAT,
        peer_protocol: crate::peer_auth::PROTOCOL_VERSION,
        native_host_protocol: crate::protocol::NATIVE_HOST_PROTOCOL,
        event_delivery: 1,
    }
}

fn decode(bytes: &[u8]) -> anyhow::Result<FormatRecord> {
    let record: FormatRecord = serde_json::from_slice(bytes)
        .context("unsupported fleet format; recreate development state")?;
    ensure!(
        record.system == "cellar"
            && record.format == FORMAT
            && record.peer_protocol == crate::peer_auth::PROTOCOL_VERSION
            && record.native_host_protocol == crate::protocol::NATIVE_HOST_PROTOCOL
            && record.event_delivery == 1,
        "unsupported fleet format; recreate development state"
    );
    Ok(record)
}

/// Concurrent fresh starters and a lost create reply converge on the same
/// marker. Other layouts are refused and require recreated development state.
pub async fn ensure_ready(bucket: &Bucket) -> anyhow::Result<()> {
    let result = initialize(bucket).await;
    if result.is_err() {
        if let Some((bytes, _)) = bucket.get_bounded(KEY, MAX_FORMAT_BYTES).await? {
            decode(&bytes)?;
            return Ok(());
        }
    }
    result
}

async fn initialize(bucket: &Bucket) -> anyhow::Result<()> {
    if let Some((bytes, _)) = bucket.get_bounded(KEY, MAX_FORMAT_BYTES).await? {
        decode(&bytes)?;
        return Ok(());
    }
    for prefix in ["cells/", "nodes/", "wake/entries/", "wake/retired/"] {
        ensure!(bucket.objects_page(prefix,None,1).await?.objects.is_empty(),
            "uninitialized fleet contains state under {prefix}; Cellar refuses to infer a storage format");
    }
    if bucket
        .put_cas(KEY, serde_json::to_vec(&current())?, None)
        .await?
        .is_none()
    {
        let (bytes, _) = bucket
            .get_bounded(KEY, MAX_FORMAT_BYTES)
            .await?
            .context("fleet format create was rejected without a readable marker")?;
        decode(&bytes)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fresh_fleet_initialization_is_concurrent_and_reopenable() -> anyhow::Result<()> {
        in_test_domain(async {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("store.sqlite");
            let bucket = Bucket::open_dev(&path)?;
            let results =
                futures_util::future::join_all((0..8).map(|_| ensure_ready(&bucket))).await;
            for result in results {
                result?;
            }
            let reopened = Bucket::open_dev(&path)?;
            ensure_ready(&reopened).await?;
            let marker = reopened.get(KEY).await?.unwrap().0;
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&marker)?,
                serde_json::json!({
                    "system":"cellar", "format":1, "peer_protocol":1,
                    "native_host_protocol":1, "event_delivery":1
                })
            );
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn cutover_refuses_unmarked_state_and_previous_markers() -> anyhow::Result<()> {
        in_test_domain(async {
            let directory = tempfile::tempdir()?;
            let bucket = Bucket::open_dev(&directory.path().join("state.sqlite"))?;
            bucket
                .put("cells/Account:old/owner.json", b"{}".to_vec())
                .await?;
            assert!(ensure_ready(&bucket).await.is_err());
            assert!(bucket.get(KEY).await?.is_none());
            let old = Bucket::open_dev(&directory.path().join("old.sqlite"))?;
            old.put(KEY, br#"{"format":2,"migrating":false}"#.to_vec())
                .await?;
            assert!(ensure_ready(&old).await.is_err());
            Ok(())
        })
        .await
    }

    async fn in_test_domain(
        future: impl std::future::Future<Output = anyhow::Result<()>>,
    ) -> anyhow::Result<()> {
        // Other library tests install a process host on a runtime they later
        // drop. These bucket fixtures must own their I/O independently.
        #[cfg(not(cellar_internal_tests))]
        {
            let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
            let result = domain.run(future).await;
            domain.shutdown().await;
            result?
        }
        #[cfg(cellar_internal_tests)]
        future.await
    }
}
