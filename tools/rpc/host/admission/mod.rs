//! Refuse cold typed RPC while acknowledged log proof lacks its original root.
mod evidence;
mod snapshot;
mod store;

use super::{application::Composite, fleet};
use anyhow::{ensure, Context, Result};
use cellar::{bucket::Bucket, sdk::*};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    path::Path,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use store::Store;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Point {
    Missing,
    Replaced,
}

impl Point {
    fn name(self) -> &'static str {
        match self {
            Self::Missing => "missing_root",
            Self::Replaced => "replaced_root",
        }
    }
}

async fn request(client: &reqwest::Client, url: &str, path: &str) -> Result<Value> {
    let response = client.get(format!("{url}{path}")).send().await?;
    let status = response.status();
    let text = response.text().await?;
    ensure!(
        status.is_success(),
        "recovery fixture request failed: {path}: {status}: {text}"
    );
    Ok(serde_json::from_str(&text)?)
}

async fn start(
    path: &Path,
    app: Arc<Composite>,
    name: &str,
    bucket: Bucket,
    observer: Arc<Store>,
) -> Result<(Node, String)> {
    fleet::start_configured(path, app, name, bucket, None, move |config, services| {
        config.actor.core.operation_deadline_ms = Some(1500);
        config.runtime.handler_budget = Duration::from_secs(3);
        config.startup.recovery_attempt_timeout = Duration::from_millis(300);
        config.startup.recovery_attempts = 1;
        config.startup.recovery_retry_backoff = Duration::from_secs(1);
        let fleet = services
            .fleet
            .as_mut()
            .context("recovery node has no fleet")?;
        if observer.exclude {
            fleet.bucket.cas_store = observer;
        } else {
            fleet.bucket.store = observer.clone();
            fleet.bucket.cas_store = observer.clone();
            fleet.replica_storage = ReplicaStorage::new(
                observer,
                ReplicaStorageOptions {
                    prefix: fleet.bucket.prefix.clone(),
                    ..Default::default()
                },
            )?;
        }
        Ok(())
    })
    .await
}

#[cfg(unix)]
async fn case(
    app: Arc<Composite>,
    client: &reqwest::Client,
    point: Point,
    caller: &str,
    provider: &str,
    class: &str,
) -> Result<Value> {
    use super::crash::process::Owner;
    app.audit.reset_after_node_drain();
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let (mut owner, owner_url) = Owner::start(directory.path(), "recovery-owner").await?;
    let worker = request(client, &owner_url, "/node/process").await?;
    owner.worker(worker["pid"].as_u64().context("Node worker PID missing")?)?;
    ensure!(
        request(client, &owner_url, &format!("/rust/read?target={provider}")).await?
            == json!({"value":"0"}),
        "cold owner already has count"
    );
    let identity = request(client, &owner_url, &format!("/control/cell?class={class}")).await?;
    ensure!(
        identity["node"] == "recovery-owner" && identity["class"] == class,
        "seed ran in another cell"
    );
    let scope = identity["scope"]
        .as_str()
        .context("seed scope missing")?
        .to_owned();
    let epoch = identity["epoch"].as_u64().context("seed epoch missing")?;
    let replacement = PeerProbeSigner::generate().public_key_hex();
    let exclusion = Store::new(
        bucket.cas_store.clone(),
        &bucket.prefix,
        &scope,
        true,
        replacement.clone(),
    );
    let (witness, _) = start(
        &directory.path().join("witness"),
        app.clone(),
        "recovery-witness",
        bucket.clone(),
        exclusion.clone(),
    )
    .await?;

    let mut baseline = 0_i64;
    tokio::time::timeout(Duration::from_secs(10), async {
        for _ in 0..32 {
            baseline += 1;
            ensure!(
                request(
                    client,
                    &owner_url,
                    &format!(
                        "/rust/increment?target={provider}&delta=1&id=recovery-warm-{baseline}"
                    )
                )
                .await?
                    == json!({"value":baseline.to_string()}),
                "recovery warmup diverged"
            );
            let observed = request(client, &owner_url, "/control/durability").await?;
            if observed["peer_members"] == json!(["recovery-witness"]) {
                return Ok::<_, anyhow::Error>(observed);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        anyhow::bail!("no actual signed follower acknowledgement")
    })
    .await??;
    ensure!(request(client, &owner_url, "/control/data-hold").await? == json!({"held":true}));
    let before = snapshot::bucket(&domain, &bucket, &scope).await?;
    let proof_before = request(client, &owner_url, "/control/durability").await?;
    let large = 9_007_199_254_740_993_i64;
    baseline += large;
    ensure!(
        request(
            client,
            &owner_url,
            &format!("/rust/increment?target={provider}&delta={large}&id=recovery-seed")
        )
        .await?
            == json!({"value":baseline.to_string()}),
        "log-only seed did not acknowledge exact state"
    );
    let proof_after = request(client, &owner_url, "/control/durability").await?;
    let peer_acks = proof_after["peer_completed"]
        .as_u64()
        .context("peer ack count missing")?
        .checked_sub(
            proof_before["peer_completed"]
                .as_u64()
                .context("original peer ack count missing")?,
        )
        .context("peer ack count moved backwards")?;
    let data_held = proof_after["data_entered"]
        .as_u64()
        .context("data proof count missing")?
        .checked_sub(
            proof_before["data_entered"]
                .as_u64()
                .context("original data proof count missing")?,
        )
        .context("data proof count moved backwards")?;
    ensure!(
        peer_acks > 0 && data_held > 0,
        "log-only seed did not enter held data publication and obtain a real follower ack"
    );
    ensure!(
        snapshot::bucket(&domain, &bucket, &scope).await? == before,
        "held bucket published new proof"
    );
    owner.kill().await?;
    let original = snapshot::root(&domain, &bucket).await?;
    ensure!(
        original["node"] == "recovery-owner"
            && original["log"]["state"] == "open"
            && original["log"]["may_have_fleet_acks"] == true
            && original["log"]["ensemble"] == json!(["recovery-witness"]),
        "crashed owner did not retain its actual fleet proof root: {original}"
    );
    let retained = snapshot::follower(directory.path(), &original)?;
    domain
        .run(async {
            if point == Point::Missing {
                bucket.delete("nodes/recovery-owner.json").await?;
            } else {
                let mut replaced = original.clone();
                replaced["generation"] = json!(replacement);
                replaced["probe_public_key"] = json!(replacement);
                replaced["lineage"] = json!(&replacement[..32]);
                replaced["expires_ms"] = json!(0);
                replaced["addr"] = json!("127.0.0.1:1");
                replaced["log"] = json!({"state":"open","epoch":1,"ensemble":[],"tiered":0,
                "may_have_fleet_acks":false,"claimant":null,"claimed_ms":null});
                bucket
                    .put("nodes/recovery-owner.json", serde_json::to_vec(&replaced)?)
                    .await?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;

    let observer = Store::new(
        bucket.store.clone(),
        &bucket.prefix,
        &scope,
        false,
        replacement.clone(),
    );
    let opened = start(
        &directory.path().join("successor"),
        app.clone(),
        "recovery-successor",
        bucket.clone(),
        observer.clone(),
    )
    .await;
    // Always return the original proof metadata after a failed cold-call audit.
    // The owned child is already gone, so only expiry changes on restoration.
    let cold = async {
        let (successor, url) = opened?;
        let id = format!("recovery-{}-{caller}-{provider}", point.name());
        let mut failures = Vec::new();
        for _ in 0..2 {
            let failure = request(
                client,
                &url,
                &format!("/{caller}/increment?target={provider}&delta=1&id={id}"),
            )
            .await?;
            ensure!(
                failure["outcome"] == "unknown"
                    && matches!(
                        failure["code"].as_str(),
                        Some(
                            "Unavailable" | "DeadlineExceeded" | "OwnershipLost" | "ContextExpired"
                        )
                    ),
                "failed recovery admitted a business mutation: {failure}"
            );
            ensure!(
                app.audit.factories().is_empty()
                    && app.audit.since(0).iter().all(|visit| visit.class.is_none()),
                "failed recovery reached a provider factory or handler"
            );
            ensure!(
                !successor.is_closed(),
                "recovery refusal closed the live successor"
            );
            failures.push(failure);
        }
        let observed = match point {
            Point::Missing => observer.missing_reads.load(Ordering::Acquire),
            Point::Replaced => observer.replaced_reads.load(Ordering::Acquire),
        };
        ensure!(
            observed > 0,
            "successor never observed the controlled metadata fault"
        );
        domain
            .run(async {
                let root = bucket.get("nodes/recovery-owner.json").await?;
                if point == Point::Missing {
                    ensure!(
                        root.is_none(),
                        "failed recovery fabricated the missing proof root"
                    );
                } else {
                    let (bytes, _) = root.context("replacement proof root was removed")?;
                    let actual: Value = serde_json::from_slice(&bytes)?;
                    ensure!(
                        actual["generation"].as_str() == Some(replacement.as_str())
                            && actual["lineage"].as_str() == Some(&replacement[..32]),
                        "replacement identity changed before its refusal audit"
                    );
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        ensure!(
            snapshot::follower(directory.path(), &original)? == retained,
            "failed recovery changed acknowledged follower proof"
        );
        ensure!(
            snapshot::bucket(&domain, &bucket, &scope).await? == before,
            "failed recovery changed retained bucket proof"
        );
        Ok::<_, anyhow::Error>((successor, url, id, failures, observed))
    }
    .await;
    let restored = domain
        .run(async {
            let mut root = original.clone();
            root["expires_ms"] = json!(0);
            bucket
                .put("nodes/recovery-owner.json", serde_json::to_vec(&root)?)
                .await?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    observer.observing.store(false, Ordering::Release);
    let (successor, url, id, failures, observed) = cold?;
    restored??;
    let recovered = super::proof::recovery_read(client, &url, provider).await?;
    ensure!(
        recovered == json!({"value":baseline.to_string()}),
        "restored metadata lost acknowledged log-only state: {recovered}"
    );
    let restored_root = snapshot::root(&domain, &bucket).await?;
    ensure!(
        restored_root["generation"] == original["generation"]
            && restored_root["lineage"] == original["lineage"],
        "successful recovery changed the restored proof identity"
    );
    ensure!(
        app.audit
            .since(0)
            .iter()
            .filter(|visit| visit.method == "example.counter.v1.Counter/Increment")
            .count()
            == 0,
        "a refused mutation was replayed after metadata restoration"
    );
    let factories = app.audit.factories();
    ensure!(
        factories.len() == 1
            && factories[0].node == "recovery-successor"
            && factories[0].class.as_deref() == Some(class)
            && factories[0].scope.as_deref() == Some(scope.as_str())
            && factories[0].epoch.is_some_and(|after| after > epoch),
        "recovery used the wrong provider factory or epoch: {factories:?}"
    );
    ensure!(
        request(
            client,
            &url,
            &format!("/{caller}/increment?target={provider}&delta={large}&id=recovery-seed")
        )
        .await?
            == json!({"value":baseline.to_string()}),
        "acknowledged receipt was lost"
    );
    let expected = baseline + 1;
    for _ in 0..2 {
        ensure!(
            request(
                client,
                &url,
                &format!("/{caller}/increment?target={provider}&delta=1&id={id}")
            )
            .await?
                == json!({"value":expected.to_string()}),
            "explicit recovery retry did not deduplicate"
        );
    }
    ensure!(
        request(
            client,
            &url,
            &format!("/{caller}/increment?target={provider}&delta=2&id={id}")
        )
        .await?
            == json!({"error":"operation_conflict"}),
        "recovery receipt conflict disappeared"
    );
    ensure!(
        request(client, &url, &format!("/rust/read?target={provider}")).await?
            == json!({"value":expected.to_string()})
    );
    fleet::stop(successor, ShutdownMode::Preserve).await?;
    fleet::stop(witness, ShutdownMode::Preserve).await?;
    domain.shutdown().await;
    Ok(
        json!({"point":point.name(),"caller":caller,"provider":provider,"scope":scope,
        "epoch_before":epoch,"epoch_after":factories[0].epoch,
        "baseline_count":baseline.to_string(),"final_count":expected.to_string(),
        "failure_count":failures.len(),"failures":failures,"metadata_fault_reads":observed,
        "factories_before_restore":0,"handlers_before_restore":0,"live_successor_preserved":true,
        "original_root_restored":true,"bucket_proof_preserved":true,"follower_proof_preserved":true,
        "log_only_acknowledgement_verified":true,"automatic_replays":0,
        "peer_append_acks":peer_acks,"bucket_publications_held":data_held,
        "retained_follower_batches":retained.iter().filter(|(name,_)|name.ends_with(".batch")).count(),
        "acknowledged_receipt_replayed":true,"pending_receipt_replayed":true,"receipt_conflict_verified":true,
        "owner_process_group_exited":true,"node_worker_group_exited":true}),
    )
}

#[cfg(unix)]
pub async fn run(app: Arc<Composite>) -> Result<Value> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(12))
        .build()?;
    let mut cases = Vec::new();
    let mut pairs = BTreeSet::new();
    for point in [Point::Missing, Point::Replaced] {
        for (caller, _, _) in fleet::CLASSES {
            for (provider, _, class) in fleet::CLASSES {
                eprintln!("recovery admission: {} {caller}->{provider}", point.name());
                cases.push(
                    case(app.clone(), &client, point, caller, provider, class)
                        .await
                        .with_context(|| {
                            format!("recovery admission {} {caller}->{provider}", point.name())
                        })?,
                );
                pairs.insert(format!("{caller}->{provider}"));
            }
        }
    }
    evidence::validate(&cases)?;
    Ok(
        json!({"format":1,"contract_digest":app.contract.digest(),"topology":"three-node-authenticated-recovery",
        "pairs":pairs,"pair_count":pairs.len(),"case_count":cases.len(),"cases":cases,
        "failed_recovery_admission_verified":true,"live_recovery_verified":true,"remote_fleet_qualified":false}),
    )
}

#[cfg(unix)]
pub async fn probe(app: Arc<Composite>) -> Result<Value> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(12))
        .build()?;
    let observed = case(
        app,
        &client,
        Point::Missing,
        "rust",
        "python",
        "PythonCounter",
    )
    .await?;
    Ok(json!({"format":1,"qualified":false,"diagnostic":"recovery-admission","case":observed}))
}

#[cfg(not(unix))]
pub async fn run(_: Arc<Composite>) -> Result<Value> {
    anyhow::bail!("recovery qualification requires Unix process groups")
}

#[cfg(not(unix))]
pub async fn probe(_: Arc<Composite>) -> Result<Value> {
    anyhow::bail!("recovery qualification requires Unix process groups")
}
