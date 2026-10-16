//! Wake failed startup recovery when an actual witness lease returns.
use crate::ownership_store::BucketOwnership;
use cellar_logic::NodeLeaseRecord;
use std::time::Duration;

#[derive(Debug)]
pub(super) struct Witnesses {
    observed_ms: u64,
    members: Vec<(String, Option<NodeLeaseRecord>)>,
}

impl std::fmt::Display for Witnesses {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("node-log recovery is waiting for witness proof")
    }
}

impl std::error::Error for Witnesses {}

impl Witnesses {
    pub(super) fn new(observed_ms: u64) -> Self {
        Self {
            observed_ms,
            members: Vec::new(),
        }
    }

    pub(super) fn observe(&mut self, member: &str, lease: Option<&NodeLeaseRecord>) {
        self.members.push((member.to_owned(), lease.cloned()));
    }

    fn returned(&self, old: Option<&NodeLeaseRecord>, current: &NodeLeaseRecord, now: u64) -> bool {
        current.expires_ms > now
            && current.peer_protocol == crate::peer_auth::PROTOCOL_VERSION
            && old.is_none_or(|old| {
                old.lineage != current.lineage
                    || old.generation != current.generation
                    || old.addr != current.addr
                    || old.expires_ms <= self.observed_ms
            })
    }
}

pub(super) async fn wait(error: &anyhow::Error, ownership: &BucketOwnership, backoff: Duration) {
    let Some(witnesses) = error.downcast_ref::<Witnesses>() else {
        crate::asyncrt::sleep(backoff).await;
        return;
    };
    // Check the exact records used by the failed proof, including a change
    // between that failure and this wait. Heartbeats of an already-live peer
    // do not reset backoff. Read failures/absence cannot supply a wake signal.
    // The timeout includes all reads; no detached task survives cancellation.
    let _ = crate::asyncrt::timeout(backoff, async {
        loop {
            for (member, old) in &witnesses.members {
                if let Ok(Some(current)) = ownership.read_node_lease(member).await {
                    if witnesses.returned(old.as_ref(), &current, crate::ownership_store::now_ms())
                    {
                        return;
                    }
                }
            }
            crate::asyncrt::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{asyncrt::ExecutionDomain, bucket::Bucket, ownership_store::NodeLeaseWire};

    fn record(generation: &str, expires_ms: u64) -> NodeLeaseWire {
        NodeLeaseWire {
            lineage: "11111111111111111111111111111111".into(),
            node: "witness".into(),
            addr: "127.0.0.1:1234".into(),
            expires_ms,
            probe_public_key: String::new(),
            peer_protocol: crate::peer_auth::PROTOCOL_VERSION,
            paced_handoff: false,
            generation: generation.into(),
            log: None,
            load: Default::default(),
        }
    }

    async fn fixture(
        bucket: &Bucket,
        old: &NodeLeaseWire,
    ) -> anyhow::Result<(BucketOwnership, anyhow::Error)> {
        bucket
            .put("nodes/witness.json", serde_json::to_vec(old)?)
            .await?;
        let ownership = BucketOwnership::new(
            bucket.clone(),
            bucket.clone(),
            "recoverer".into(),
            "generation".into(),
        );
        let prior = ownership.read_node_lease("witness").await?;
        let mut witnesses = Witnesses::new(crate::ownership_store::now_ms());
        witnesses.observe("witness", prior.as_ref());
        Ok((
            ownership,
            anyhow::anyhow!("original recovery still needs real proof").context(witnesses),
        ))
    }

    #[tokio::test]
    async fn lease_return_before_wait_wakes_without_waiting_for_ttl() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let result = domain
            .run(async {
                let bucket = Bucket::open_dev(&directory.path().join("store.sqlite"))?;
                for change in ["generation", "address", "lineage", "renewed"] {
                    let old = record("old", if change == "renewed" { 1 } else { u64::MAX });
                    let (ownership, error) = fixture(&bucket, &old).await?;
                    let mut current = record("old", u64::MAX);
                    match change {
                        "generation" => current.generation = "new".into(),
                        "address" => current.addr = "127.0.0.1:2345".into(),
                        "lineage" => current.lineage = "22222222222222222222222222222222".into(),
                        _ => {}
                    }
                    bucket
                        .put("nodes/witness.json", serde_json::to_vec(&current)?)
                        .await?;
                    crate::asyncrt::timeout(
                        Duration::from_secs(1),
                        wait(&error, &ownership, Duration::from_secs(30)),
                    )
                    .await?;
                    // Waking only changes scheduling. It cannot write or seal either
                    // node's state, or turn the original failure into success.
                    assert!(format!("{error:#}").contains("still needs real proof"));
                    assert!(bucket.get("nodes/recoverer.json").await?.is_none());
                    assert!(ownership
                        .read_node_lease("witness")
                        .await?
                        .unwrap()
                        .log_state
                        .is_none());
                }
                anyhow::Ok(())
            })
            .await?;
        domain.shutdown().await;
        result
    }

    #[tokio::test]
    async fn unchanged_heartbeat_absence_and_invalid_reads_preserve_backoff() -> anyhow::Result<()>
    {
        let directory = tempfile::tempdir()?;
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let result = domain
            .run(async {
                let bucket = Bucket::open_dev(&directory.path().join("store.sqlite"))?;
                for mode in ["heartbeat", "missing", "corrupt", "expired", "protocol"] {
                    let (ownership, error) = fixture(&bucket, &record("old", u64::MAX - 1)).await?;
                    let mut current = record("old", u64::MAX);
                    match mode {
                        "missing" => bucket.delete("nodes/witness.json").await?,
                        "corrupt" => {
                            bucket
                                .put("nodes/witness.json", b"bad metadata".to_vec())
                                .await?
                        }
                        _ => {
                            if mode == "expired" {
                                current.generation = "new".into();
                                current.expires_ms = 1;
                            }
                            if mode == "protocol" {
                                current.generation = "new".into();
                                current.peer_protocol = 2;
                            }
                            bucket
                                .put("nodes/witness.json", serde_json::to_vec(&current)?)
                                .await?;
                        }
                    }
                    let start = crate::asyncrt::mono_ms();
                    wait(&error, &ownership, Duration::from_millis(40)).await;
                    assert!(
                        crate::asyncrt::mono_ms().saturating_sub(start) >= 40,
                        "{mode} interrupted backoff without a returning witness"
                    );
                }
                anyhow::Ok(())
            })
            .await?;
        domain.shutdown().await;
        result
    }

    #[tokio::test]
    async fn untagged_errors_preserve_backoff() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let result = domain
            .run(async {
                let bucket = Bucket::open_dev(&directory.path().join("store.sqlite"))?;
                let (ownership, _) = fixture(&bucket, &record("new", u64::MAX)).await?;
                let start = crate::asyncrt::mono_ms();
                wait(
                    &anyhow::anyhow!("live predecessor refusal"),
                    &ownership,
                    Duration::from_millis(40),
                )
                .await;
                assert!(crate::asyncrt::mono_ms().saturating_sub(start) >= 40);
                anyhow::Ok(())
            })
            .await?;
        domain.shutdown().await;
        result
    }
}
