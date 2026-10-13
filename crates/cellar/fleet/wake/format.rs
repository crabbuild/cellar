// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! Fleet admission and explicit offline protocol cutover. Ordinary startup
//! never upgrades storage containing older ownership/work semantics.
use crate::bucket::Bucket;
use anyhow::{ensure, Context};

const KEY: &str = "wake/format.json";
const MAX_FORMAT_BYTES: usize = 8 << 10;
pub const FORMAT: u8 = 3;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upgrade {
    pub operation: String,
    pub reason: String,
    pub confirmed_stopped_writers: bool,
}

impl Upgrade {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.operation.len() == 32
                && self
                    .operation
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "fleet upgrade requires a canonical 32-character operation UUID"
        );
        ensure!(
            !self.reason.trim().is_empty() && self.reason.len() <= 512,
            "fleet upgrade requires a nonempty reason of at most 512 bytes"
        );
        cutover_allowed(self.confirmed_stopped_writers, None, 0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpgradeReceipt {
    pub request: Upgrade,
    pub from_format: u8,
    pub applied_ms: i64,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FormatRecord {
    system: String,
    format: u8,
    peer_protocol: u16,
    native_host_protocol: u32,
    event_delivery: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    upgrade: Option<UpgradeReceipt>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PreviousFormat {
    system: String,
    format: u8,
}

fn current(upgrade: Option<UpgradeReceipt>) -> FormatRecord {
    FormatRecord {
        system: "cellar".into(),
        format: FORMAT,
        peer_protocol: crate::peer_auth::PROTOCOL_VERSION,
        native_host_protocol: crate::protocol::NATIVE_HOST_PROTOCOL,
        event_delivery: 1,
        upgrade,
    }
}

fn decode(bytes: &[u8]) -> anyhow::Result<FormatRecord> {
    let record: FormatRecord = serde_json::from_slice(bytes).context(
        "unsupported fleet format; upgrade stopped Cellar format-2 fleets with cellar fleet upgrade")?;
    ensure!(
        record.system == "cellar"
            && record.format == FORMAT
            && record.peer_protocol == crate::peer_auth::PROTOCOL_VERSION
            && record.native_host_protocol == crate::protocol::NATIVE_HOST_PROTOCOL
            && record.event_delivery == 1,
        "unsupported fleet format; coordinated Cellar upgrade is required"
    );
    if let Some(receipt) = &record.upgrade {
        receipt.request.validate()?;
        ensure!(
            receipt.from_format == 2 && receipt.applied_ms >= 0,
            "invalid fleet upgrade receipt"
        );
    }
    Ok(record)
}

/// Concurrent fresh starters and a lost create reply converge on the same
/// marker. Existing formats are refused; enabling new semantics is explicit.
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
        .put_cas(KEY, serde_json::to_vec(&current(None))?, None)
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

fn cutover_allowed(confirmed: bool, latest_expiry: Option<u64>, now: u64) -> anyhow::Result<()> {
    cellar_logic::format::coordinated_cutover(confirmed,latest_expiry,now).map_err(|failure| match failure {
        cellar_logic::format::CutoverRefusal::ConfirmationRequired=>anyhow::anyhow!("fleet upgrade requires confirmation that all old writers and recovery processes are stopped"),
        cellar_logic::format::CutoverRefusal::LiveLease=>anyhow::anyhow!("fleet upgrade refused while a node lease remains live; stop writers and wait for lease expiry"),
    })
}

async fn check_stopped(bucket: &Bucket, confirmed: bool) -> anyhow::Result<()> {
    let now = u64::try_from(crate::asyncrt::wall_ms())?;
    let mut token = None;
    let mut latest = None;
    loop {
        let page = bucket.objects_page("nodes/", token, 100).await?;
        for object in page.objects {
            let Some((bytes, _)) = bucket
                .get_bounded(object.location.as_ref(), 64 << 10)
                .await?
            else {
                continue;
            };
            let lease: crate::ownership_store::NodeLeaseWire =
                serde_json::from_slice(&bytes).context("decode node lease before fleet upgrade")?;
            latest =
                Some(latest.map_or(lease.expires_ms, |prior: u64| prior.max(lease.expires_ms)));
            cutover_allowed(confirmed, latest, now)?;
        }
        token = page.page_token;
        if token.is_none() {
            break;
        }
    }
    cutover_allowed(confirmed, latest, now)
}

fn replay(record: FormatRecord, request: &Upgrade) -> anyhow::Result<Option<UpgradeReceipt>> {
    if let Some(receipt) = &record.upgrade {
        ensure!(
            receipt.request == *request,
            "fleet upgrade already completed with another operation or different arguments"
        );
    }
    Ok(record.upgrade)
}

/// Upgrade only the exact prior Cellar marker, preserving every cell, lease,
/// log and wake object. The recorded confirmation supplies the offline premise;
/// a paginated lease scan additionally refuses any still-live authority.
/// Already committed intent is immutable and safe to retry after a lost reply.
pub async fn upgrade(bucket: &Bucket, request: &Upgrade) -> anyhow::Result<Option<UpgradeReceipt>> {
    request.validate()?;
    let (bytes, etag) = bucket
        .get_bounded(KEY, MAX_FORMAT_BYTES)
        .await?
        .context("fleet upgrade requires an existing Cellar format-2 marker")?;
    if let Ok(record) = decode(&bytes) {
        return replay(record, request);
    }
    let prior: PreviousFormat = serde_json::from_slice(&bytes)?;
    ensure!(
        prior.system == "cellar" && prior.format == 2,
        "only Cellar format 2 supports this coordinated upgrade"
    );
    check_stopped(bucket, request.confirmed_stopped_writers).await?;
    let receipt = UpgradeReceipt {
        request: request.clone(),
        from_format: 2,
        applied_ms: crate::asyncrt::wall_ms(),
    };
    let result = bucket
        .put_cas(
            KEY,
            serde_json::to_vec(&current(Some(receipt.clone())))?,
            Some(&etag),
        )
        .await;
    match result {
        Ok(Some(_)) => Ok(Some(receipt)),
        result => {
            // CAS conflict and an ambiguous applied write are both resolved
            // by reading durable intent. Never overwrite a competing cutover.
            if let Some((bytes, _)) = bucket.get_bounded(KEY, MAX_FORMAT_BYTES).await? {
                if let Ok(record) = decode(&bytes) {
                    return replay(record, request);
                }
            }
            Err(match result {
                Err(error) => error,
                _ => anyhow::anyhow!(
                    "fleet upgrade CAS was rejected without the matching durable receipt"
                ),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fresh_fleet_initialization_is_concurrent_and_reopenable() -> anyhow::Result<()> {
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let result = domain.run(fresh_case()).await?;
        domain.shutdown().await;
        result
    }

    async fn fresh_case() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("store.sqlite");
        let bucket = Bucket::open_dev(&path)?;
        let results = futures_util::future::join_all((0..8).map(|_| ensure_ready(&bucket))).await;
        for result in results {
            result?;
        }
        let reopened = Bucket::open_dev(&path)?;
        ensure_ready(&reopened).await?;
        let marker = reopened.get(KEY).await?.unwrap().0;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&marker)?["system"],
            "cellar"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cutover_refuses_unmarked_state_and_previous_markers() -> anyhow::Result<()> {
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let result = domain.run(cutover_case()).await?;
        domain.shutdown().await;
        result
    }

    async fn cutover_case() -> anyhow::Result<()> {
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
    }

    #[tokio::test]
    async fn coordinated_upgrade_preserves_data_refuses_live_leases_and_replays_receipts(
    ) -> anyhow::Result<()> {
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let result = domain.run(upgrade_case()).await?;
        domain.shutdown().await;
        result
    }

    async fn upgrade_case() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let bucket = Bucket::open_dev(&directory.path().join("upgrade.sqlite"))?;
        let previous = br#"{"system":"cellar","format":2}"#.to_vec();
        bucket.put(KEY, previous.clone()).await?;
        let cell = b"original ownership and event bytes".to_vec();
        let wake = b"original pending wake".to_vec();
        bucket
            .put("cells/Account:original/ltx/e4/retained", cell.clone())
            .await?;
        bucket.put("wake/entries/original", wake.clone()).await?;
        assert!(ensure_ready(&bucket).await.is_err());
        let mut request = Upgrade {
            operation: "00000000000000000000000000000099".into(),
            reason: "coordinated event protocol upgrade".into(),
            confirmed_stopped_writers: false,
        };
        assert!(upgrade(&bucket, &request).await.is_err());
        request.confirmed_stopped_writers = true;
        // A live lease on the next page must still prevent the marker CAS.
        for index in 0..101 {
            let node = format!("n{index:03}");
            let lease = serde_json::json!({"node":node,"expires_ms":0,"addr":"127.0.0.1:1","probe_public_key":"prior-key",
                "peer_protocol":5,"paced_handoff":true,"generation":"prior-generation"});
            bucket
                .put(&format!("nodes/{node}.json"), serde_json::to_vec(&lease)?)
                .await?;
        }
        let mut lease = serde_json::json!({"node":"z-live","expires_ms":u64::try_from(crate::asyncrt::wall_ms())?+60_000,
            "addr":"127.0.0.1:1","probe_public_key":"prior-key","peer_protocol":5,"paced_handoff":true,"generation":"prior-generation"});
        bucket
            .put("nodes/z-live.json", serde_json::to_vec(&lease)?)
            .await?;
        assert!(upgrade(&bucket, &request).await.is_err());
        assert_eq!(bucket.get(KEY).await?.unwrap().0.as_ref(), previous);
        lease["expires_ms"] = serde_json::json!(0);
        bucket
            .put("nodes/z-live.json", serde_json::to_vec(&lease)?)
            .await?;
        let results =
            futures_util::future::join_all((0..8).map(|_| upgrade(&bucket, &request))).await;
        let mut committed = None;
        for result in results {
            let receipt = result?.unwrap();
            if let Some(prior) = &committed {
                assert_eq!(prior, &receipt);
            }
            committed = Some(receipt);
        }
        let marker = bucket.get(KEY).await?.unwrap().0;
        let record = decode(&marker)?;
        assert_eq!(record.format, 3);
        assert_eq!(record.peer_protocol, 6);
        assert_eq!(record.native_host_protocol, 3);
        assert!(
            serde_json::from_slice::<PreviousFormat>(&marker).is_err(),
            "previous marker reader admitted event-enabled ownership"
        );
        assert_eq!(upgrade(&bucket, &request).await?, committed);
        let mut changed = request.clone();
        changed.reason = "different reason".into();
        assert!(upgrade(&bucket, &changed).await.is_err());
        changed = request.clone();
        changed.operation = "00000000000000000000000000000098".into();
        assert!(upgrade(&bucket, &changed).await.is_err());
        assert_eq!(bucket.get(KEY).await?.unwrap().0, marker);
        assert_eq!(
            bucket
                .get("cells/Account:original/ltx/e4/retained")
                .await?
                .unwrap()
                .0
                .as_ref(),
            cell
        );
        assert_eq!(
            bucket
                .get("wake/entries/original")
                .await?
                .unwrap()
                .0
                .as_ref(),
            wake
        );
        ensure_ready(&bucket).await?;
        Ok(())
    }
}
