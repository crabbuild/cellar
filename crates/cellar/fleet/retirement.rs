// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! Retire expired Cellar node leases without erasing a successor generation.
//! All retired leases remain CAS-written tombstones; unsealed logs retain their
//! recovery pointers. Original ownership-index garbage is unsupported.

use crate::bucket::Bucket;
use anyhow::Context as _;
use futures_util::stream::{self, StreamExt as _};
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeSet;
use std::time::Duration;
use tracing::warn;

const NODE_SCAN_CONCURRENCY: usize = 64;
const NODE_SCAN_PAGE_SIZE: usize = 256;

#[derive(Deserialize, Serialize)]
struct NodeWire {
    lineage: String,
    node: String,
    expires_ms: u64,
    generation: String,
    #[serde(default)]
    probe_public_key: String,
    /// The folded node log: this record IS
    /// the fleet log's root of truth, so retirement must respect its
    /// state and the tombstone must carry it through unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    log: Option<serde_json::Value>,
    /// Every other lease field is preserved verbatim in the CAS tombstone.
    #[serde(flatten)]
    rest: serde_json::Map<String, serde_json::Value>,
}

/// Elected maintenance capability. The bucket retains every retirement;
/// no compatibility index scan or process-local marker latch is needed.
#[derive(Default)]
pub struct DeadNodeGc;

impl DeadNodeGc {
    /// Run one pass while renewing the advisory fleet-waker role. No task is
    /// spawned: the caller's existing wake-loop future polls both work and
    /// renewal, and dropping a lost-role pass cancels remaining I/O.
    ///
    /// Returns the node sessions whose leases were live when the pass began,
    /// and only when this node held the role for the whole pass. The caller
    /// uses that as its election result: the fleet-wide wake probe that
    /// follows runs on the same tick, against the same leases, and only on
    /// the node that holds the role. `None` means either that another node
    /// holds the role or that this node lost it mid-pass; both mean "do not
    /// probe this tick".
    pub async fn run_elected_pass(
        &mut self,
        bucket: &Bucket,
        node: &str,
        tick_ms: u64,
    ) -> Option<BTreeSet<String>> {
        let lease_ttl_ms = tick_ms.saturating_mul(3).min(i64::MAX as u64);
        if !crate::wake::try_hold_waker(
            bucket,
            node,
            crate::ownership_store::now_ms() as i64,
            lease_ttl_ms as i64,
        )
        .await
        {
            return None;
        }

        let renew_ms = (lease_ttl_ms / 3).max(1);
        let mut renewal = crate::asyncrt::interval(Duration::from_millis(renew_ms));
        renewal.set_missed_tick_behavior(crate::asyncrt::MissedTickBehavior::Delay);
        renewal.tick().await;
        let pass = self.run_pass(bucket);
        tokio::pin!(pass);
        loop {
            crate::asyncrt::select_biased! {
                "a due waker-lease renewal wins a tie with completion of the GC pass";
                _ = renewal.tick() => {
                    if !crate::wake::try_hold_waker(
                        bucket,
                        node,
                        crate::ownership_store::now_ms() as i64,
                        lease_ttl_ms as i64,
                    ).await {
                        warn!("waker lease renewal failed; cancelling dead-node GC");
                        return None;
                    }
                },
                live = &mut pass => return Some(live),
            }
        }
    }

    /// One pass, with a fresh authority read before each CAS retirement.
    async fn run_pass(&mut self, bucket: &Bucket) -> BTreeSet<String> {
        let observed_ms = crate::ownership_store::now_ms();
        let NodeScan { dead, live } = scan_nodes(bucket, observed_ms).await;
        for node in dead {
            if let Err(error) = retire_dead_node(bucket, &node, observed_ms).await {
                warn!(%node, %error, "dead-node lease retirement failed");
            }
        }
        live
    }
}

/// The node sessions with a live lease, for a caller that has no GC pass to
/// take them from (the boot-time wake scan).
pub async fn live_nodes(bucket: &Bucket, now_ms: u64) -> BTreeSet<String> {
    scan_nodes(bucket, now_ms).await.live
}

/// The fleet's node records, split by lease state at one instant.
struct NodeScan {
    dead: Vec<String>,
    /// Sessions whose record matches its key and whose lease has not
    /// expired. A record that fails to read is in neither set, so an owner
    /// named by such a record counts as not live and the wake probe hints
    /// its cells; the core's own lease read then decides.
    live: BTreeSet<String>,
}

async fn scan_nodes(bucket: &Bucket, now_ms: u64) -> NodeScan {
    let mut dead = Vec::new();
    let mut live = BTreeSet::new();
    let mut token = None;
    loop {
        let page = match bucket
            .objects_page("nodes", token, NODE_SCAN_PAGE_SIZE)
            .await
        {
            Ok(page) => page,
            Err(error) => {
                warn!(%error, "dead-node scan list failed");
                break;
            }
        };
        let mut reads = stream::iter(page.objects)
            .map(|object| async move {
                let key = object.location.as_ref();
                let node = key
                    .strip_prefix("nodes/")
                    .and_then(|key| key.strip_suffix(".json"))?;
                match read_node(bucket, key).await {
                    Ok(Some((record, _))) if record.node == node => {
                        Some((node.to_owned(), record.expires_ms <= now_ms))
                    }
                    Ok(_) => None,
                    Err(error) => {
                        warn!(%node, %error, "dead-node scan read failed");
                        None
                    }
                }
            })
            .buffer_unordered(NODE_SCAN_CONCURRENCY);
        while let Some(read) = reads.next().await {
            if let Some((node, is_dead)) = read {
                if is_dead {
                    dead.push(node);
                } else {
                    live.insert(node);
                }
            }
        }
        token = page.page_token;
        if token.is_none() {
            break;
        }
    }
    NodeScan { dead, live }
}

async fn retire_dead_node(bucket: &Bucket, node: &str, now_ms: u64) -> anyhow::Result<bool> {
    let key = format!("nodes/{node}.json");
    let Some((record, etag)) = read_node(bucket, &key).await? else {
        return Ok(true);
    };
    if record.node != node || record.expires_ms > now_ms || record.expires_ms == 0 {
        return Ok(false);
    }
    // Stable node keys can be reused immediately after expiry. Even a lease
    // with no log can be superseded by a successor that opens one, so an
    // unconditional delete after a tombstone CAS is never safe.
    if record
        .log
        .as_ref()
        .is_some_and(|log| log.get("state").and_then(|state| state.as_str()) != Some("sealed"))
    {
        return Ok(false);
    }
    let tombstone = serde_json::to_vec(&NodeWire {
        expires_ms: 0,
        ..record
    })?;
    Ok(bucket
        .put_cas(&key, tombstone, Some(&etag))
        .await?
        .is_some())
}

async fn read_node(bucket: &Bucket, key: &str) -> anyhow::Result<Option<(NodeWire, String)>> {
    let Some((bytes, etag)) = bucket.get(key).await? else {
        return Ok(None);
    };
    let record: NodeWire = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode {}://{}/{key}", bucket.scheme(), bucket.name))?;
    crate::ownership_store::validate_lineage(&record.lineage)?;
    crate::machine::validate_node_name(&record.node)?;
    crate::machine::validate_node_name(&record.generation)?;
    Ok(Some((record, etag)))
}
