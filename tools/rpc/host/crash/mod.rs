//! Abrupt process-loss qualification, separate from graceful fleet handoff.
mod evidence;
mod gate;
#[cfg(unix)]
pub(crate) mod process;
pub(crate) mod sqlite;

pub use gate::{Counter, Gate, Point};

use anyhow::{ensure, Context as _, Result};
use cellar::{bucket::Bucket, sdk::ShutdownMode};
use serde_json::{json, Value};
use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};

use super::{application::Composite, fleet};

pub struct Server {
    pub name: String,
    pub data: PathBuf,
    pub bucket: PathBuf,
    pub ready: PathBuf,
}

pub async fn serve(app: Arc<Composite>, server: Server) -> Result<Value> {
    // Only the disposable crash-owner process installs this observer. Native
    // applications and the qualification coordinator keep their SQLite hooks.
    sqlite::install()?;
    app.crash
        .evidence_file(server.ready.with_file_name("barrier.json"));
    let bucket = Bucket::open_dev(&server.bucket)?;
    let controls = app.durability.clone();
    let (mut node, url) =
        fleet::start_controlled(&server.data, app, &server.name, bucket, Some(controls)).await?;
    let temporary = server.ready.with_extension("pending");
    std::fs::write(
        &temporary,
        serde_json::to_vec(&json!({
            "format":1,"pid":std::process::id(),"node":node.info().node,"url":url,
        }))?,
    )?;
    std::fs::rename(temporary, server.ready)?;
    let outcome = node.finished().await?;
    anyhow::bail!("crash owner stopped before the coordinator killed it: {outcome:?}")
}

async fn get(client: &reqwest::Client, base: &str, path: &str) -> Result<Value> {
    let response = client.get(format!("{base}{path}")).send().await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(
        status.is_success(),
        "crash fixture {path} failed: {status}: {body}"
    );
    Ok(serde_json::from_str(&body)?)
}

#[cfg(unix)]
async fn recovered_read(client: &reqwest::Client, base: &str, target: &str) -> Result<Value> {
    // Only a read is retried while the lost owner's lease expires and native
    // recovery proves admission. A successful but incorrect count fails at once.
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let response = client
                .get(format!("{base}/rust/read?target={target}"))
                .send()
                .await?;
            if response.status().is_success() {
                return Ok::<_, anyhow::Error>(response.json().await?);
            }
            let status = response.status();
            let body = response.text().await?;
            ensure!(
                status.is_server_error()
                    && [
                        "Cellar RPC Unavailable (Unknown)",
                        "Cellar RPC DeadlineExceeded (Unknown)"
                    ]
                    .iter()
                    .any(|message| body.contains(message)),
                "recovery read failed outside a transient RPC routing refusal: {status}: {body}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("surviving node did not recover the cell for read admission")?
}

#[cfg(unix)]
async fn case(
    app: Arc<Composite>,
    client: &reqwest::Client,
    point: Point,
    caller: &'static str,
    target: &'static str,
    class: &'static str,
) -> Result<Value> {
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (mut owner, owner_url) = process::Owner::start(directory.path(), "crash-owner").await?;
    let worker = get(client, &owner_url, "/node/process").await?;
    owner.worker(
        worker["pid"]
            .as_u64()
            .context("Node worker PID is missing")?,
    )?;
    ensure!(
        get(client, &owner_url, &format!("/rust/read?target={target}"))
            .await
            .context("seed sole crash owner before peer joins")?
            == json!({"value":"0"}),
        "crash owner was not empty"
    );
    let (next, next_url) = fleet::start(
        &directory.path().join("next"),
        app.clone(),
        "crash-next",
        bucket,
    )
    .await?;
    let id = format!("crash-{}-{caller}-{target}", point.name());
    let delta = 9_007_199_254_740_993_i64;
    ensure!(
        get(
            client,
            &owner_url,
            &format!("/control/arm?point={}&class={class}&id={id}", point.name())
        )
        .await?
            == json!({"armed":true}),
        "crash gate did not arm"
    );
    let cursor = app.audit.cursor();
    let (pending_client, pending_url, pending_id) = (client.clone(), next_url.clone(), id.clone());
    let pending = tokio::spawn(async move {
        get(
            &pending_client,
            &pending_url,
            &format!("/{caller}/increment?target={target}&delta={delta}&id={pending_id}"),
        )
        .await
    });
    let reached = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match std::fs::read(directory.path().join("barrier.json")) {
                Ok(bytes) => {
                    let view: Value = serde_json::from_slice(&bytes)?;
                    ensure!(view["ready"] == true, "incomplete crash barrier evidence");
                    return Ok::<_, anyhow::Error>(view);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("requested RPC never reached its crash barrier")??;
    ensure!(
        reached["node"] == "crash-owner"
            && reached["class"] == class
            && reached["point"] == point.name(),
        "crash reached the wrong execution domain"
    );
    let original_scope = reached["scope"].as_str().context("crash scope missing")?;
    let original_epoch = reached["epoch"].as_u64().context("crash epoch missing")?;
    let count_at_crash = if point == Point::BeforeHandler {
        "0".to_string()
    } else {
        delta.to_string()
    };
    ensure!(
        reached["count"] == count_at_crash,
        "observed storage does not match the crash point: {reached}"
    );
    if point != Point::AfterSyncedHandler {
        ensure!(
            reached["receipt"].is_null(),
            "pre-commit crash already has a receipt"
        );
    } else {
        ensure!(
            reached["receipt"] == json!({"delta":delta.to_string(),"value":delta.to_string()}),
            "synced handler did not store an atomic receipt"
        );
        ensure!(
            reached["typed_reply_held"] == true,
            "post-commit crash did not hold a validated typed reply"
        );
    }
    ensure!(
        reached["transaction_open"]
            .as_bool()
            .context("transaction observation missing")?
            == (point == Point::AfterCountBeforeReceipt),
        "crash transaction observation does not match its boundary"
    );
    let committed = if point == Point::AfterSyncedHandler {
        delta.to_string()
    } else {
        "0".into()
    };
    owner.kill().await?;
    eprintln!(
        "crash owner and Node worker exited: {} {caller}->{target}",
        point.name()
    );
    let failure = tokio::time::timeout(Duration::from_secs(20), pending).await???;
    ensure!(
        failure["outcome"] == "unknown"
            && matches!(
                failure["code"].as_str(),
                Some("Unavailable" | "DeadlineExceeded" | "OwnershipLost" | "ContextExpired")
            ),
        "lost owner reply claimed a result or rollback: {failure}"
    );
    ensure!(
        app.audit
            .since(cursor)
            .iter()
            .all(|visit| visit.class.is_none()),
        "lost admitted RPC was automatically replayed on the surviving node"
    );
    ensure!(
        recovered_read(client, &next_url, target).await? == json!({"value":committed}),
        "recovery lost synced state or admitted partial transaction work"
    );
    for _ in 0..2 {
        ensure!(
            get(
                client,
                &next_url,
                &format!("/{caller}/increment?target={target}&delta={delta}&id={id}")
            )
            .await?
                == json!({"value":delta.to_string()}),
            "same-ID retry duplicated a mutation or lost its receipt"
        );
    }
    ensure!(
        get(
            client,
            &next_url,
            &format!("/rust/increment?target={target}&delta=1&id={id}")
        )
        .await?
            == json!({"error":"operation_conflict"}),
        "crash recovery lost receipt semantics"
    );
    ensure!(
        get(client, &next_url, &format!("/rust/read?target={target}")).await?
            == json!({"value":delta.to_string()}),
        "crash retry total is not exact"
    );
    let visits = app.audit.since(cursor);
    let executed: Vec<_> = visits
        .iter()
        .filter(|visit| visit.class.is_some())
        .collect();
    ensure!(
        executed.len() == 5
            && executed.iter().all(|visit| visit.node == "crash-next"
                && visit.class.as_deref() == Some(class)
                && visit.scope.as_deref() == Some(original_scope)
                && visit.epoch.is_some_and(|epoch| epoch > original_epoch)),
        "crash recovery crossed an owner, scope or epoch"
    );
    let epoch = executed[0].epoch.unwrap();
    ensure!(
        executed.iter().all(|visit| visit.epoch == Some(epoch)),
        "recovered owner epoch changed during receipt replay"
    );
    fleet::stop(next, ShutdownMode::Preserve).await?;
    Ok(
        json!({"caller":caller,"provider":target,"point":point.name(),"first_failure":failure,
        "owner_signal":"SIGKILL","owner_process_group_exited":true,
        "typed_reply_held":reached["typed_reply_held"],
        "count_at_crash":count_at_crash,"count_after_recovery":committed,
        "receipt_at_crash":reached["receipt"],
        "transaction_open":reached["transaction_open"],
        "transaction_observer":reached["transaction_observer"],
        "sqlite_checkpoint":reached["sqlite_checkpoint"],
        "final_count":delta.to_string(),
        "epoch_before":original_epoch,"epoch_after":epoch,"scope_preserved":true,
        "receipt_replayed":true,"duplicate_mutations":0}),
    )
}

#[cfg(unix)]
pub async fn run(app: Arc<Composite>) -> Result<Value> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(25))
        .build()?;
    let mut cases = Vec::new();
    let mut pairs = BTreeSet::new();
    for point in [
        Point::BeforeHandler,
        Point::AfterSyncedHandler,
        Point::AfterCountBeforeReceipt,
    ] {
        for (caller, _, _) in fleet::CLASSES {
            for (target, _, class) in fleet::CLASSES {
                eprintln!("crash qualification: {} {caller}->{target}", point.name());
                cases.push(
                    case(app.clone(), &client, point, caller, target, class)
                        .await
                        .with_context(|| {
                            format!("crash qualification {} {caller}->{target}", point.name())
                        })?,
                );
                pairs.insert(format!("{caller}->{target}"));
            }
        }
    }
    ensure!(
        cases.len() == 48 && pairs.len() == 16,
        "crash matrix incomplete"
    );
    evidence::validate(&cases)?;
    Ok(
        json!({"format":1,"contract_digest":app.contract.digest(),"topology":"two-process-authenticated-fleet",
        "pairs":pairs,"pair_count":pairs.len(),"case_count":cases.len(),"cases":cases,
        "crash_points":["before_handler","after_synced_handler","after_count_before_receipt"],
        "point_pair_counts":{"before_handler":16,"after_synced_handler":16,"after_count_before_receipt":16},
        "owner_processes_killed":48,"crash_matrix_verified":true,
        "rust_transaction_checkpoint_pair_count":4,"rust_transaction_checkpoint_verified":true,
        "guest_transaction_checkpoint_pair_count":12,"guest_transaction_checkpoint_verified":true,
        "all_provider_transaction_checkpoints_verified":true,
        "owner_worker_groups_exited":true,"receipt_recovery_verified":true,
        "abrupt_crash_qualified":false,"remote_fleet_qualified":false}),
    )
}

#[cfg(not(unix))]
pub async fn run(_: Arc<Composite>) -> Result<Value> {
    anyhow::bail!("the crash qualification requires Unix SIGKILL and process groups")
}
