// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Withhold real durability I/O; verify output gates and original owner fencing.
mod evidence;
mod fence;
mod live;
mod native_live;
mod recovery;
mod wait;
mod wait_evidence;
pub use native_live::Live;
use recovery::read;
pub(crate) use recovery::read as recovery_read;
pub use wait::run as waits;
pub use wait::smoke as wait_smoke;

// Reuse the owning SDK harness's real peer/object-store faults. Its lost-ack
// variant is outside this suite; no runtime hooks or fake proof tickets exist.
#[allow(dead_code)]
#[path = "../../../../crates/cellar-python/tests/support/fault_gates.rs"]
mod faults;

// The SDK bridge executable uses harness=false. Run the shared fault adapter's
// unit contracts here, under an actual Rust test harness.
#[cfg(test)]
#[path = "../../../../crates/cellar-python/tests/support/fault_gates_tests.rs"]
mod fault_tests;

use anyhow::{ensure, Context as _, Result};
use cellar::{bucket::Bucket, sdk::*};
use faults::{Gate, Peer, Release, Store};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use super::{
    application::{Composite, Returned},
    fleet,
};

#[derive(Clone, Default)]
pub struct Controls {
    peers: Arc<Gate>,
    data: Arc<Gate>,
}

impl Controls {
    pub async fn hold_data(&self) {
        self.data.hold_and_drain().await;
    }

    pub fn open_data(&self) {
        self.data.open();
    }

    pub fn snapshot(&self) -> Value {
        json!({"peer_members":self.peers.members.lock().unwrap().clone(),
            "peer_held":self.peers.is_held(),"data_held":self.data.is_held(),
            "peer_completed":self.peers.completed.load(Ordering::Acquire),
            "data_entered":self.data.entered.load(Ordering::Acquire),
            "data_completed":self.data.completed.load(Ordering::Acquire)})
    }

    pub fn install(self, fleet: &mut NodeFleet) -> Result<()> {
        let store = Arc::new(Store {
            inner: fleet.bucket.store.clone(),
            gate: self.data,
        });
        fleet.replica_storage = ReplicaStorage::new(
            store.clone(),
            ReplicaStorageOptions {
                prefix: fleet.bucket.prefix.clone(),
                ..Default::default()
            },
        )?;
        fleet.bucket.store = store.clone();
        fleet.bucket.cas_store = store;
        fleet.log_transport = Arc::new(Peer {
            inner: fleet.log_transport.clone(),
            gate: self.peers,
        });
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Point {
    Release,
    Fail,
    FenceOutput,
    FenceSync,
}

impl Point {
    fn name(self) -> &'static str {
        match self {
            Self::Release => "release",
            Self::Fail => "fail",
            Self::FenceOutput => "fence_output",
            Self::FenceSync => "fence_sync",
        }
    }

    fn fenced(self) -> bool {
        matches!(self, Self::FenceOutput | Self::FenceSync)
    }
}

/// Abort the original HTTP future on a failed assertion; the supervised native
/// and guest runtimes still own and drain the submitted SDK work.
struct Pending(tokio::task::JoinHandle<Result<Value>>);

impl Drop for Pending {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn request(client: &reqwest::Client, url: &str, path: &str) -> Result<Value> {
    let response = client.get(format!("{url}{path}")).send().await?;
    let status = response.status();
    let text = response.text().await?;
    ensure!(
        status.is_success(),
        "typed proof fixture {path} failed: {status}: {text}"
    );
    Ok(serde_json::from_str(&text)?)
}

async fn returned(
    app: &Composite,
    cursor: usize,
    controls: &Controls,
    peer_before: usize,
    data_before: usize,
) -> Result<Returned> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let visits = app.audit.returned_since(cursor);
            ensure!(
                visits.len() <= 1,
                "a held proof command was replayed before delivery"
            );
            if let Some(visit) = visits.into_iter().next() {
                if controls.peers.entered.load(Ordering::Acquire) > peer_before
                    && controls.data.entered.load(Ordering::Acquire) > data_before
                {
                    return Ok::<_, anyhow::Error>(visit);
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("typed handler did not return while real proof paths were held")?
}

async fn warm(
    client: &reqwest::Client,
    owner_url: &str,
    provider: &str,
    controls: &Controls,
    baseline: &mut i64,
    members: usize,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        for _ in 0..32 {
            *baseline += 1;
            ensure!(
                request(
                    client,
                    owner_url,
                    &format!("/rust/increment?target={provider}&delta=1&id=proof-warm-{baseline}")
                )
                .await?
                    == json!({"value":baseline.to_string()}),
                "proof warmup count diverged"
            );
            if controls.peers.members.lock().unwrap().len() >= members {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        anyhow::bail!(
            "proof suite did not establish {members} real replica members: {:?}",
            controls.peers.members.lock().unwrap()
        )
    })
    .await
    .context("proof warmup did not finish within discovery budget")?
}

async fn case(
    app: Arc<Composite>,
    client: &reqwest::Client,
    point: Point,
    caller: &'static str,
    provider: &'static str,
    class: &'static str,
) -> Result<Value> {
    // Both proof runners are sequential. The previous case (or preceding
    // suite in --suite all) joins every native node before returning, so its
    // captured evidence no longer needs live cursors into this bounded log.
    app.audit.reset_after_node_drain();
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let controls = Controls::default();
    let (mut owner, owner_url) = fleet::start_controlled(
        &directory.path().join("owner"),
        app.clone(),
        "proof-owner",
        bucket.clone(),
        Some(controls.clone()),
    )
    .await?;
    ensure!(
        request(client, &owner_url, &format!("/rust/read?target={provider}")).await?
            == json!({"value":"0"}),
        "proof owner was not empty"
    );
    let (ingress, ingress_url) = fleet::start_controlled(
        &directory.path().join("ingress"),
        app.clone(),
        "proof-ingress",
        bucket.clone(),
        Some(controls.clone()),
    )
    .await?;
    // Certify the existing one-follower ensemble before starting a third node.
    // This reproduces late recruitment through the real background driver.
    let mut baseline = 0_i64;
    warm(client, &owner_url, provider, &controls, &mut baseline, 1).await?;
    let initial_members = controls.peers.members.lock().unwrap().clone();
    ensure!(
        initial_members == BTreeSet::from(["proof-ingress".into()]),
        "first replica was not the ingress node"
    );
    let (replica, _) = fleet::start_controlled(
        &directory.path().join("replica"),
        app.clone(),
        "proof-replica",
        bucket.clone(),
        Some(controls.clone()),
    )
    .await?;
    // Each warmup is a distinct acknowledged command, not a retry of a failed
    // mutation. Establish both actual peer members before withholding proof.
    warm(client, &owner_url, provider, &controls, &mut baseline, 2).await?;
    let members = controls.peers.members.lock().unwrap().clone();
    ensure!(
        members == BTreeSet::from(["proof-ingress".into(), "proof-replica".into()]),
        "proof replica set differs: {members:?}"
    );
    let delta = 9_007_199_254_740_993_i64;
    let expected = baseline
        .checked_add(delta)
        .context("proof fixture count overflow")?;
    let id = if point == Point::FenceSync {
        format!("proof-fence-sync-{caller}-{provider}")
    } else {
        format!("proof-no-sync-{}-{caller}-{provider}", point.name())
    };
    let peer_before = controls.peers.entered.load(Ordering::Acquire);
    let data_before = controls.data.entered.load(Ordering::Acquire);
    let returned_cursor = app.audit.returned_cursor();
    let invoked_cursor = app.audit.cursor();
    // Drop this guard before the node handles on every failed assertion.
    let _release = Release(controls.peers.clone(), controls.data.clone());
    controls.peers.hold();
    controls.data.hold();
    let (pending_client, pending_url, pending_id) =
        (client.clone(), ingress_url.clone(), id.clone());
    let mut pending = Pending(tokio::spawn(async move {
        request(
            &pending_client,
            &pending_url,
            &format!("/{caller}/increment?target={provider}&delta={delta}&id={pending_id}"),
        )
        .await
    }));
    let watch = live::Watch {
        app: &app,
        client,
        url: &ingress_url,
        provider,
        class,
        id: &id,
        expected,
        delta,
        invoked_cursor,
        returned_cursor,
    };
    let (observed, live_before) = if point == Point::FenceSync {
        watch.waiting(&controls, peer_before, data_before).await?
    } else {
        (
            returned(&app, returned_cursor, &controls, peer_before, data_before).await?,
            Value::Null,
        )
    };
    ensure!(
        observed.visit.node == "proof-owner"
            && observed.visit.class.as_deref() == Some(class)
            && observed.visit.method == "example.counter.v1.Counter/Increment"
            && observed.visit.scope.is_some()
            && observed.visit.epoch.is_some(),
        "proof command returned in the wrong execution domain"
    );
    ensure!(
        observed.count_matching_storage.as_deref() == Some(expected.to_string().as_str())
            && (point == Point::FenceSync
                || observed.transaction_open == Some(false)
                    && observed.write_position.is_some_and(|position| position > 0)),
        "handler did not return a typed result matching its local commit"
    );
    let peer_held = controls.peers.entered.load(Ordering::Acquire) - peer_before;
    let data_held = controls.data.entered.load(Ordering::Acquire) - data_before;
    ensure!(
        tokio::time::timeout(Duration::from_millis(150), &mut pending.0)
            .await
            .is_err(),
        "successful or failed response escaped before proof was released/failed"
    );
    let mut fenced = Value::Null;
    match point {
        Point::Release => {
            controls.peers.open();
            controls.data.open();
        }
        Point::Fail => {
            controls.peers.fail();
            controls.data.fail();
        }
        Point::FenceOutput | Point::FenceSync => {
            fenced = fence::owner(&bucket, &mut owner).await?;
            fence::refuses_new_request(client, &owner_url, provider).await?;
        }
    }
    let result = tokio::time::timeout(Duration::from_secs(15), &mut pending.0)
        .await
        .context("proof command did not finish after release/failure")???;
    if point == Point::Release {
        ensure!(
            result == json!({"value":expected.to_string()}),
            "released proof did not deliver the original typed result: {result}"
        );
    } else {
        ensure!(
            result["outcome"] == "unknown"
                && matches!(
                    result["code"].as_str(),
                    Some("Unavailable" | "DeadlineExceeded" | "OwnershipLost" | "ContextExpired")
                ),
            "failed output proof claimed success or rollback: {result}"
        );
    }
    ensure!(
        app.audit
            .since(invoked_cursor)
            .iter()
            .filter(|visit| visit.method == "example.counter.v1.Counter/Increment")
            .count()
            == 1,
        "failed proof automatically replayed the mutation"
    );
    controls.peers.open();
    controls.data.open();
    let live_after = if point == Point::FenceSync {
        watch.resume().await?
    } else {
        Value::Null
    };
    let recovered = read(client, &ingress_url, provider).await?;
    let recovered_count = recovered["value"]
        .as_str()
        .context("recovery count missing")?
        .parse::<i64>()?;
    ensure!(
        recovered_count == expected || point != Point::Release && recovered_count == baseline,
        "recovery exposed an impossible committed count: {recovered}"
    );
    let recovered_visit = app
        .audit
        .returned_since(returned_cursor)
        .into_iter()
        .rfind(|returned| returned.visit.method == "example.counter.v1.Counter/GetCount")
        .context("recovery read has no native execution observation")?
        .visit;
    let mut acknowledged_receipt = false;
    if point.fenced() {
        ensure!(
            matches!(
                recovered_visit.node.as_str(),
                "proof-ingress" | "proof-replica"
            ) && recovered_visit.scope == observed.visit.scope
                && recovered_visit
                    .epoch
                    .zip(observed.visit.epoch)
                    .is_some_and(|(after, before)| after > before),
            "fence recovery did not advance ownership on the surviving cell"
        );
        ensure!(
            request(
                client,
                &ingress_url,
                &format!("/rust/increment?target={provider}&delta=1&id=proof-warm-{baseline}")
            )
            .await?
                == json!({"value":baseline.to_string()}),
            "new owner lost the acknowledged warmup receipt"
        );
        ensure!(
            read(client, &ingress_url, provider).await? == recovered,
            "acknowledged receipt replay changed recovered state"
        );
        acknowledged_receipt = true;
    }
    for _ in 0..2 {
        ensure!(
            request(
                client,
                &ingress_url,
                &format!("/{caller}/increment?target={provider}&delta={delta}&id={id}")
            )
            .await?
                == json!({"value":expected.to_string()}),
            "explicit proof retry duplicated a mutation or lost its receipt"
        );
    }
    ensure!(
        request(
            client,
            &ingress_url,
            &format!("/rust/increment?target={provider}&delta=1&id={id}")
        )
        .await?
            == json!({"error":"operation_conflict"}),
        "proof recovery lost receipt conflict semantics"
    );
    ensure!(
        read(client, &ingress_url, provider).await? == json!({"value":expected.to_string()}),
        "proof retry count is not exact"
    );
    ensure!(
        app.audit
            .since(invoked_cursor)
            .iter()
            .filter(|visit| visit.class.is_some())
            .all(|visit| visit.scope == observed.visit.scope),
        "proof recovery changed the named cell scope"
    );
    if point.fenced() {
        ensure!(
            app.audit
                .since(invoked_cursor)
                .iter()
                .filter(|visit| visit.class.is_some() && visit.node != "proof-owner")
                .all(|visit| visit.node == recovered_visit.node
                    && visit.epoch == recovered_visit.epoch),
            "fence receipt recovery crossed surviving owners or epochs"
        );
        drop(owner);
    } else {
        fleet::stop(owner, ShutdownMode::Preserve).await?;
    }
    fleet::stop(ingress, ShutdownMode::Preserve).await?;
    fleet::stop(replica, ShutdownMode::Preserve).await?;
    Ok(
        json!({"point":point.name(),"caller":caller,"provider":provider,
        "node":observed.visit.node,"class":class,"scope":observed.visit.scope,"epoch":observed.visit.epoch,
        "handler_returned":point != Point::FenceSync,"transaction_open":observed.transaction_open,"write_position":observed.write_position,
        "live_before":live_before,"live_after":live_after,
        "local_count":observed.count_matching_storage,"baseline_count":baseline.to_string(),"expected_count":expected.to_string(),
        "peer_proof_held":peer_held,"bucket_proof_held":data_held,"response_escaped_while_held":false,
        "replica_members":members,
        "initial_replica_members":initial_members,
        "fence":fenced,"fenced_new_request_refused":point.fenced(),
        "recovered_node":recovered_visit.node,"recovered_epoch":recovered_visit.epoch,
        "acknowledged_receipt_replayed":acknowledged_receipt,
        "result":result,"count_after_recovery":recovered_count.to_string(),"final_count":expected.to_string(),
        "automatic_replays":0,"receipt_replayed":true,"scope_preserved":true}),
    )
}

pub async fn run(app: Arc<Composite>) -> Result<Value> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let mut cases = Vec::new();
    let mut pairs = BTreeSet::new();
    for point in [
        Point::Release,
        Point::Fail,
        Point::FenceOutput,
        Point::FenceSync,
    ] {
        for (caller, _, _) in fleet::CLASSES {
            for (provider, _, class) in fleet::CLASSES {
                eprintln!(
                    "output proof qualification: {} {caller}->{provider}",
                    point.name()
                );
                cases.push(
                    case(app.clone(), &client, point, caller, provider, class)
                        .await
                        .with_context(|| {
                            format!("output proof {} {caller}->{provider}", point.name())
                        })?,
                );
                pairs.insert(format!("{caller}->{provider}"));
            }
        }
    }
    evidence::validate(&cases)?;
    Ok(
        json!({"format":1,"contract_digest":app.contract.digest(),"topology":"authenticated-three-node-fleet",
        "pair_count":pairs.len(),"pairs":pairs,"case_count":cases.len(),"cases":cases,
        "handler_sync_omitted_for":["release","fail","fence_output"],"handler_sync_awaited_for":["fence_sync"],
        "release_matrix_verified":true,"failure_matrix_verified":true,
        "fenced_output_matrix_verified":true,
        "live_handler_context_fencing_verified":true,
        "runtimes_drained_before_publication":true,"remote_fleet_qualified":false}),
    )
}

pub async fn fences(app: Arc<Composite>) -> Result<Value> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let mut cases = Vec::new();
    for point in [Point::FenceOutput, Point::FenceSync] {
        for (caller, _, _) in fleet::CLASSES {
            for (provider, _, class) in fleet::CLASSES {
                eprintln!(
                    "fenced qualification: {} {caller}->{provider}",
                    point.name()
                );
                cases.push(
                    case(app.clone(), &client, point, caller, provider, class)
                        .await
                        .with_context(|| format!("{} {caller}->{provider}", point.name()))?,
                );
            }
        }
    }
    evidence::validate_fences(&cases)?;
    Ok(
        json!({"format":1,"contract_digest":app.contract.digest(),"topology":"authenticated-three-node-fleet",
        "case_count":cases.len(),"pair_count":16,"cases":cases,"fenced_output_matrix_verified":true,
        "live_handler_context_fencing_verified":true,"remote_fleet_qualified":false}),
    )
}

/// A deterministic real-driver regression loop. This diagnostic never claims
/// to qualify the full language or failure matrix.
pub async fn recruitment(app: Arc<Composite>) -> Result<Value> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let case = case(app, &client, Point::Release, "rust", "rust", "RustCounter").await?;
    Ok(json!({"format":1,"diagnostic":"late-replica-recruitment","qualified":false,"case":case}))
}
