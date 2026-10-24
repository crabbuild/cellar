//! A refused append's only stream change is its clock. Require real output
//! proof, remove the old owner's disk, and audit recovery before receipt reuse.

use super::event_clock_retention::{history, number, owner, pending, wall};
use super::event_root_read::{follower_proof, RootReadCut};
use super::event_settlement_race::{fetch, settle, status};
use super::event_unproven_ack::PublicationFault;
use super::*;
use base64::Engine;
use serde_json::{json, Value};
use std::sync::atomic::AtomicI64;
use std::time::Instant;

pub(super) async fn replicas(
    observer: &ExecutionDomain,
    bucket: &cellar::bucket::Bucket,
) -> Result<Vec<(String, Vec<u8>)>> {
    observer
        .run(async {
            let prefix = format!("cells/{}/ltx/", stream_scope()?);
            let mut objects = bucket.list(&prefix).await?;
            objects.extend(bucket.list("log/refusal-destination/").await?);
            ensure!(
                objects.len() <= 100,
                "fixture replica inventory exceeded its bound"
            );
            objects.sort_by(|a, b| a.location.cmp(&b.location));
            let mut total = 0usize;
            let mut result = Vec::new();
            for object in objects {
                let path = object.location.to_string();
                let (bytes, _) = bucket
                    .get(&path)
                    .await?
                    .context("replica vanished during audit")?;
                total = total
                    .checked_add(bytes.len())
                    .context("replica audit overflow")?;
                ensure!(
                    total <= 16 << 20,
                    "fixture replica bytes exceeded their bound"
                );
                result.push((path, bytes.to_vec()));
            }
            Ok::<_, anyhow::Error>(result)
        })
        .await?
}

pub(super) async fn expire_joined_owner(
    observer: &ExecutionDomain,
    bucket: &cellar::bucket::Bucket,
) -> Result<Value> {
    observer
        .run(async {
            let key = "nodes/refusal-destination.json";
            let (bytes, token) = bucket.get(key).await?.context("joined old lease missing")?;
            let prior: Value = serde_json::from_slice(&bytes)?;
            let mut expired = prior.clone();
            expired["expires_ms"] = json!(wall() - 1);
            ensure!(
                bucket
                    .put_cas(key, serde_json::to_vec(&expired)?, Some(&token))
                    .await?
                    .is_some(),
                "old lease changed after domain join"
            );
            let (actual, _) = bucket
                .get(key)
                .await?
                .context("expired old lease missing")?;
            let mut actual: Value = serde_json::from_slice(&actual)?;
            ensure!(actual["expires_ms"].as_i64().context("expiry missing")? < wall());
            // A real survivor may already be claiming/sealing this expired log.
            // Its state/certificate may advance; proof identity and authority must
            // remain the same. Our CAS payload changed only the expiry field.
            for field in ["epoch", "ensemble", "may_have_fleet_acks"] {
                ensure!(
                    actual["log"][field] == prior["log"][field],
                    "recovery changed proof identity {field}"
                );
            }
            actual["expires_ms"] = prior["expires_ms"].clone();
            if let Some(log) = prior.get("log") {
                actual["log"] = log.clone();
            } else {
                ensure!(
                    actual.get("log").is_none(),
                    "liveness fault added a log record"
                );
            }
            ensure!(
                actual == prior,
                "liveness fault changed authority fields: {prior} -> {actual}"
            );
            Ok::<_, anyhow::Error>(prior)
        })
        .await?
}

pub(super) async fn recovered_status(
    client: &reqwest::Client,
    node: &Node,
    expires: i64,
) -> Result<(Value, usize)> {
    read_status(client, node, expires, false).await
}

async fn read_status(
    client: &reqwest::Client,
    node: &Node,
    expires: i64,
    recovery_pending: bool,
) -> Result<(Value, usize)> {
    let mut read_retries = 0;
    let recovered = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            ensure!(
                wall() < expires,
                "ordinary UTC reached expiry before the read-only audit"
            );
            match status(client, node).await {
                Ok(value) => return Ok::<_, anyhow::Error>(value),
                Err(error)
                    if error.to_string().contains("503 Service Unavailable")
                        && (error
                            .to_string()
                            .contains("Ambiguous: event RPC did not return a proven result")
                            || error
                                .to_string()
                                .contains("Unavailable: event RPC deadline elapsed")
                            || recovery_pending
                                && error.to_string().contains(
                                    "Unavailable: event RPC did not return a proven result",
                                )) =>
                {
                    // A standalone read can safely repeat after old-owner
                    // transport loss, an unsubmitted RPC's deadline, or an
                    // explicit recovery-pending refusal during startup. The
                    // enclosing audit budget still wins; no mutation, fetch,
                    // or receipt is replayed.
                    read_retries += 1;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .context("first successor read-only clock audit timed out")??;
    Ok((recovered, read_retries))
}

async fn restore_metadata(
    observer: &ExecutionDomain,
    bucket: &cellar::bucket::Bucket,
    old: &Value,
    replace: bool,
) -> Result<()> {
    observer
        .run(async {
            let key = "nodes/refusal-destination.json";
            let token = if replace {
                let (_, token) = bucket
                    .get(key)
                    .await?
                    .context("joined fresh lineage missing")?;
                Some(token)
            } else {
                ensure!(
                    bucket.get(key).await?.is_none(),
                    "refusal fabricated a node record"
                );
                None
            };
            let mut restored = old.clone();
            restored["expires_ms"] = json!(wall() - 1);
            ensure!(
                bucket
                    .put_cas(key, serde_json::to_vec(&restored)?, token.as_deref())
                    .await?
                    .is_some(),
                "node metadata changed during controlled return"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_clock_bucket_proof_survives_rollback_and_forced_owner_loss() -> Result<()> {
    run(false, false, MetadataLoss::None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_clock_follower_proof_survives_rollback_without_owner_disk_or_upload() -> Result<()>
{
    run(true, false, MetadataLoss::None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_clock_cold_followers_recover_before_any_receipt_replay() -> Result<()> {
    run(true, true, MetadataLoss::None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_follower_audit_survives_one_read_deadline_without_receipt_replay() -> Result<()> {
    run(true, true, MetadataLoss::DelayedColdRead).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_destination_lease_refuses_admission_until_proof_metadata_returns() -> Result<()> {
    run(true, false, MetadataLoss::BeforeForeign).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_name_restart_cannot_recover_from_a_fresh_lineage_after_root_loss() -> Result<()> {
    run(true, false, MetadataLoss::BeforeSameName).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_loss_after_lineage_capture_refuses_boot_until_original_proof_returns() -> Result<()> {
    run(true, false, MetadataLoss::AfterCapture).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_replacement_after_lineage_capture_refuses_boot_until_original_proof_returns(
) -> Result<()> {
    run(true, false, MetadataLoss::ReplacedAfterCapture).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_loss_after_recovery_refuses_lease_install_until_sealed_metadata_returns() -> Result<()>
{
    run(true, false, MetadataLoss::AfterRecovery).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_replacement_after_recovery_refuses_lease_install_until_sealed_metadata_returns(
) -> Result<()> {
    run(true, false, MetadataLoss::ReplacedAfterRecovery).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_loss_during_recovery_refuses_boot_until_claimed_metadata_returns() -> Result<()> {
    run(true, false, MetadataLoss::DuringRecovery).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_replacement_during_recovery_refuses_boot_until_claimed_metadata_returns() -> Result<()>
{
    run(true, false, MetadataLoss::ReplacedDuringRecovery).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreign_root_loss_during_recovery_refuses_takeover_until_claimed_metadata_returns(
) -> Result<()> {
    run(true, false, MetadataLoss::ForeignDuringRecovery).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreign_root_replacement_during_recovery_refuses_takeover_until_claimed_metadata_returns(
) -> Result<()> {
    run(true, false, MetadataLoss::ReplacedForeignDuringRecovery).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_foreign_recovery_resumes_after_missing_claimed_metadata_returns() -> Result<()> {
    run(true, false, MetadataLoss::LiveForeignDuringRecovery).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_foreign_recovery_resumes_after_replaced_claimed_metadata_returns() -> Result<()> {
    run(true, false, MetadataLoss::ReplacedLiveForeignDuringRecovery).await
}

#[derive(Clone, Copy, Debug)]
enum MetadataLoss {
    None,
    DelayedColdRead,
    BeforeForeign,
    BeforeSameName,
    AfterCapture,
    ReplacedAfterCapture,
    AfterRecovery,
    ReplacedAfterRecovery,
    DuringRecovery,
    ReplacedDuringRecovery,
    ForeignDuringRecovery,
    ReplacedForeignDuringRecovery,
    LiveForeignDuringRecovery,
    ReplacedLiveForeignDuringRecovery,
}

fn original_startup_error(
    directory: &tempfile::TempDir,
    role: &str,
    error: anyhow::Error,
) -> anyhow::Error {
    let path = directory.path();
    error.context(format!(
        "start original {role} for clock refusal; retained fixture {}",
        path.display()
    ))
}

// Keep exact observations outside the databases that recovery may modify.
fn save_clock_evidence(directory: &tempfile::TempDir, name: &str, value: &Value) -> Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(directory.path().join(name))?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.sync_all()?;
    Ok(())
}

async fn cold_root_snapshot(
    observer: &ExecutionDomain,
    bucket: &cellar::bucket::Bucket,
) -> Result<Value> {
    let keys = [
        "nodes/refusal-destination.json".to_owned(),
        "nodes/refusal-source.json".to_owned(),
        "nodes/refusal-witness.json".to_owned(),
        "nodes/refusal-replacement.json".to_owned(),
        format!("cells/{}/own.json", stream_scope()?),
    ];
    observer
        .run(async {
            let mut rows = Vec::new();
            for key in keys {
                let observation = match bucket.get(&key).await {
                    Ok(Some((bytes, _))) => json!({
                        "bytes": base64::engine::general_purpose::STANDARD.encode(&bytes),
                        "metadata": serde_json::from_slice::<Value>(&bytes).ok(),
                    }),
                    Ok(None) => json!({"missing":true}),
                    Err(error) => json!({"error":format!("{error:#}")}),
                };
                rows.push(json!({"key":key,"observation":observation}));
            }
            json!(rows)
        })
        .await
        .map_err(Into::into)
}

async fn require_recovering_root_cut(
    observer: &ExecutionDomain,
    bucket: &cellar::bucket::Bucket,
    captured: &Value,
    original_owner: &Value,
) -> Result<()> {
    let current = observer
        .run(async {
            let (bytes, _) = bucket
                .get("nodes/refusal-destination.json")
                .await?
                .context("claimed root vanished before the controlled fault")?;
            Ok::<_, anyhow::Error>(serde_json::from_slice::<Value>(&bytes)?)
        })
        .await??;
    for field in [
        "/node",
        "/lineage",
        "/generation",
        "/log/state",
        "/log/claimant",
        "/log/epoch",
        "/log/ensemble",
        "/log/may_have_fleet_acks",
    ] {
        ensure!(
            current.pointer(field) == captured.pointer(field),
            "recovery advanced before controlled root fault at {field}: {current}"
        );
    }
    ensure!(
        owner(observer, bucket).await? == *original_owner,
        "cell ownership advanced before controlled root fault"
    );
    Ok(())
}

async fn replacement_inputs(
    directory: &std::path::Path,
    core: &Executor,
    bucket: cellar::bucket::Bucket,
    same_name: bool,
) -> Result<(NodeConfig, NodeServices, DeploymentGraph)> {
    let (mut config, services) = inputs(
        if same_name {
            "refusal-destination"
        } else {
            "refusal-replacement"
        },
        &directory.join("replacement"),
        core,
        Some(bucket),
        0,
    )
    .await?;
    config.actor.lease_ttl_ms = 120_000;
    config.startup.recovery_retry_backoff = Duration::from_secs(120);
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    external.primary.options.stream_bindings.clear();
    Ok((config, services, external))
}

async fn delivery_proxy(
    config: &mut NodeConfig,
    services: &NodeServices,
) -> Result<peer_reply_fault::Proxy> {
    let proxy = peer_reply_fault::Proxy::start(services.internal_listener.local_addr()?).await?;
    config.identity.advertise = proxy.address.to_string();
    Ok(proxy)
}

async fn run(followers: bool, cold_followers: bool, loss: MetadataLoss) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let result = run_case(&directory, followers, cold_followers, loss).await;
    if let Err(error) = result {
        let saved = save_clock_evidence(
            &directory,
            "failure.json",
            &json!({"format":1,"followers":followers,"cold_followers":cold_followers,
                "loss":format!("{loss:?}"),"error":format!("{error:#}")}),
        );
        let retained = directory.keep();
        return Err(error).with_context(|| {
            format!(
                "clock refusal followers={followers} cold={cold_followers} loss={loss:?}; failure_evidence={saved:?}; retained fixture {}",
                retained.display()
            )
        });
    }
    Ok(())
}

async fn run_case(
    directory: &tempfile::TempDir,
    followers: bool,
    cold_followers: bool,
    loss: MetadataLoss,
) -> Result<()> {
    let missing_metadata = matches!(
        loss,
        MetadataLoss::BeforeForeign | MetadataLoss::BeforeSameName
    );
    let same_name = matches!(
        loss,
        MetadataLoss::BeforeSameName
            | MetadataLoss::AfterCapture
            | MetadataLoss::ReplacedAfterCapture
            | MetadataLoss::AfterRecovery
            | MetadataLoss::ReplacedAfterRecovery
            | MetadataLoss::DuringRecovery
            | MetadataLoss::ReplacedDuringRecovery
    );
    let after_capture = matches!(
        loss,
        MetadataLoss::AfterCapture | MetadataLoss::ReplacedAfterCapture
    );
    let replaced_after_capture = matches!(loss, MetadataLoss::ReplacedAfterCapture);
    let after_recovery = matches!(
        loss,
        MetadataLoss::AfterRecovery | MetadataLoss::ReplacedAfterRecovery
    );
    let replaced_after_recovery = matches!(loss, MetadataLoss::ReplacedAfterRecovery);
    let during_recovery = matches!(
        loss,
        MetadataLoss::DuringRecovery
            | MetadataLoss::ReplacedDuringRecovery
            | MetadataLoss::ForeignDuringRecovery
            | MetadataLoss::ReplacedForeignDuringRecovery
            | MetadataLoss::LiveForeignDuringRecovery
            | MetadataLoss::ReplacedLiveForeignDuringRecovery
    );
    let foreign_recovery = matches!(
        loss,
        MetadataLoss::ForeignDuringRecovery
            | MetadataLoss::ReplacedForeignDuringRecovery
            | MetadataLoss::LiveForeignDuringRecovery
            | MetadataLoss::ReplacedLiveForeignDuringRecovery
    );
    let live_recovery = matches!(
        loss,
        MetadataLoss::LiveForeignDuringRecovery | MetadataLoss::ReplacedLiveForeignDuringRecovery
    );
    let replaced_during_recovery = matches!(
        loss,
        MetadataLoss::ReplacedDuringRecovery
            | MetadataLoss::ReplacedForeignDuringRecovery
            | MetadataLoss::ReplacedLiveForeignDuringRecovery
    );
    let replaced_root =
        replaced_after_capture || replaced_after_recovery || replaced_during_recovery;
    let destination_data = directory.path().join("destination");
    let destination_core = Executor::new("refusal-clock-destination")?;
    let source_core = Executor::new("refusal-clock-source")?;
    let witness_core = Executor::new("refusal-clock-witness")?;
    let replacement_core = Executor::new("refusal-clock-replacement")?;
    let observer = ExecutionDomain::new(tokio::runtime::Handle::current());
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    // Select the replacement node as the measured successor. Other real peers
    // retain their read, lease, follower and recovery services; only their
    // conditional writes to this cell's owner key are refused during the cut.
    let peer_claim = PublicationFault::with_prefixes(
        bucket.cas_store.clone(),
        format!("{}cells/{}/own.json", bucket.prefix, stream_scope()?),
        if during_recovery {
            // Select the recovering process as well as the eventual cell owner.
            // Other peers retain their leases, reads and actual follower data.
            vec![format!("{}nodes/refusal-destination.json", bucket.prefix)]
        } else {
            Vec::new()
        },
    );
    let outage = PublicationFault::with_prefixes(
        bucket.store.clone(),
        format!("{}cells/{}/ltx/", bucket.prefix, stream_scope()?),
        vec![format!("{}log/refusal-destination/", bucket.prefix)],
    );
    let mut replica_bucket = bucket.clone();
    replica_bucket.store = outage.clone();
    let offset = Arc::new(AtomicI64::new(0));
    let selected = offset.clone();
    let (mut config, mut services) = inputs(
        "refusal-destination",
        &destination_data,
        &destination_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    config.actor.lease_ttl_ms = 120_000;
    config.startup.recovery_retry_backoff = Duration::from_secs(120);
    services.wall_clock = Some(Arc::new(move || wall() + selected.load(Ordering::SeqCst)));
    let fleet = services
        .fleet
        .as_mut()
        .context("destination fleet missing")?;
    fleet.fleet_durability = followers;
    fleet.bucket = replica_bucket.clone();
    fleet.replica_storage = cellar::replica_storage::ReplicaStorage::new(
        replica_bucket.store.clone(),
        cellar::replica_storage::ReplicaStorageOptions {
            prefix: bucket.prefix.clone(),
            ..Default::default()
        },
    )?;
    let mut proxy =
        peer_reply_fault::Proxy::start(services.internal_listener.local_addr()?).await?;
    config.identity.advertise = proxy.address.to_string();
    let mut definition = graph(1, false);
    definition.primary.event_streams[0].retention = Retention::Required;
    definition.primary.event_streams[0].max_records = 1;
    definition.primary.event_streams[0].subscriptions[0].required = true;
    let destination =
        match Node::start(config, services, definition, applications("destination")?).await {
            Ok(node) => node,
            Err(error) => return Err(original_startup_error(directory, "destination", error)),
        };
    super::super::ready(&destination).await?;
    let first_owner = owner(&observer, &bucket).await?;
    ensure!(first_owner["node"] == "refusal-destination");
    let (mut config, mut services) = inputs(
        "refusal-source",
        &directory.path().join("source"),
        &source_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    services
        .fleet
        .as_mut()
        .context("source fleet missing")?
        .bucket
        .cas_store = peer_claim.clone();
    config.actor.lease_ttl_ms = 120_000;
    config.startup.recovery_retry_backoff = Duration::from_secs(120);
    let operation_budget = Duration::from_millis(config.actor.core.operation_deadline_ms.unwrap());
    ensure!(operation_budget == Duration::from_secs(3));
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let mut source = match Node::start(config, services, external, applications("source")?).await {
        Ok(node) => node,
        Err(error) => return Err(original_startup_error(directory, "source", error)),
    };
    super::super::ready(&source).await?;
    let mut witness = if followers {
        let (mut config, mut services) = inputs(
            "refusal-witness",
            &directory.path().join("witness"),
            &witness_core,
            Some(bucket.clone()),
            0,
        )
        .await?;
        services
            .fleet
            .as_mut()
            .context("witness fleet missing")?
            .bucket
            .cas_store = peer_claim.clone();
        config.actor.lease_ttl_ms = 120_000;
        config.startup.recovery_retry_backoff = Duration::from_secs(120);
        let mut external = graph(1, false);
        external.primary.event_streams.clear();
        let node = match Node::start(config, services, external, applications("witness")?).await {
            Ok(node) => node,
            Err(error) => return Err(original_startup_error(directory, "witness", error)),
        };
        super::super::ready(&node).await?;
        Some(node)
    } else {
        None
    };
    let client = client()?;
    for cell in ["producer", "projection"] {
        ensure!(
            call(
                &client,
                &source,
                reqwest::Method::GET,
                &format!("/?source={cell}")
            )
            .await?
                == "source:0"
        );
    }
    let original_write = call(
        &client,
        &source,
        reqwest::Method::POST,
        "/write?source=producer",
    )
    .await?;
    ensure!(original_write == "source:1");
    let mut initial_pending = Value::Null;
    let initial_settlement = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            initial_pending = pending(&client, &source)
                .await
                .context("initial source settlement inspection")?;
            if initial_pending["pending_rows"] == "0" {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if initial_settlement.is_err() {
        let saved = save_clock_evidence(
            directory,
            "initial-settlement-failure.json",
            &json!({"format":1,"source_response":original_write,
                "last_source":initial_pending,"cutoff_utc_ms":wall()}),
        );
        anyhow::bail!("settle original obligation before clock-only cut: original five-second deadline elapsed; last_source={initial_pending}; evidence={saved:?}");
    }
    initial_settlement.context("settle original obligation before clock-only cut")??;
    let original = fetch(
        &client,
        &source,
        "000000000000000000000000000000e1",
        "projection",
    )
    .await?;
    ensure!(
        original["processed"] == 1,
        "initial consumer fetch did not expose one proven record: {original}"
    );
    let receipt = Receipt::decode(
        original["batch"]["receipt"]
            .as_str()
            .context("original receipt missing")?,
    )?;
    let initial = status(&client, &destination)
        .await
        .context("initial destination lease inspection")?;
    let accepted = history(&client, &destination)
        .await
        .context("initial accepted history")?;
    ensure!(initial["checkpoint"] == "0" && initial["observation"]["lease_active"] == true);
    ensure!(accepted.as_array().is_some_and(|rows| rows.len() == 1));
    let expires = number(&original["batch"]["expires_ms"])?;
    ensure!(expires - number(&original["batch"]["issued_ms"])? == 30_000);
    let expiry_cut = expires + 1_000;

    // In fleet mode only actual authenticated followers can prove the new
    // write. Lease/catalog/store clients remain untouched. Compare every byte
    // of the bounded private stream replica prefix before recovery can upload.
    if followers {
        outage.block_and_drain().await;
    }
    let baseline = replicas(&observer, &bucket).await?;
    let original_roots = cold_root_snapshot(&observer, &bucket).await?;
    save_clock_evidence(
        directory,
        "accepted-prefix.json",
        &json!({"format":1,"source_response":original_write,"original_batch":original,
            "original_roots":original_roots,"original_owner":first_owner,
            "initial":initial,"accepted":accepted,"replicas":baseline.iter().map(|(key,bytes)|
                json!({"key":key,"bytes":base64::engine::general_purpose::STANDARD.encode(bytes)}))
                .collect::<Vec<_>>() }),
    )?;
    proxy.capture.hold_requests(&["__streamAppend"]);
    proxy.capture.delay_append();
    let submitted = Instant::now();
    let pending_write = call(
        &client,
        &source,
        reqwest::Method::POST,
        "/write?source=producer",
    )
    .await?;
    ensure!(pending_write == "source:2");
    tokio::time::timeout(
        Duration::from_secs(5),
        proxy.capture.request_entered.acquire(),
    )
    .await??
    .forget();
    let obligation = pending(&client, &source)
        .await
        .context("inspect source while append submission held")?;
    ensure!(
        obligation["pending_rows"] == "1"
            && obligation["lanes"][0]["revision"]["next_sequence"] == "3"
            && obligation["lanes"][0]["revision"]["settled_through"] == "1"
    );
    // Keep the producer separate from the selected stream owner, including
    // concurrent cold startup. Co-located dispatch is valid but bypasses the
    // peer submission boundary this fixture measures.
    let producer_scope = obligation["lanes"][0]["revision"]["source"]
        .as_str()
        .context("original producer scope missing")?
        .to_owned();
    let producer_claim = PublicationFault::with_prefixes(
        bucket.cas_store.clone(),
        format!("{}cells/{producer_scope}/own.json", bucket.prefix),
        Vec::new(),
    );
    producer_claim.block();
    let mut replacement_bucket = bucket.clone();
    replacement_bucket.cas_store = producer_claim;
    offset.store(expiry_cut - wall(), Ordering::SeqCst);
    proxy.capture.request_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), proxy.capture.entered.acquire())
        .await
        .context("initial clock-refusal capture did not reach the typed response boundary within five seconds")??
        .forget();
    offset.store(0, Ordering::SeqCst);
    let captured = proxy
        .capture
        .exchanges
        .lock()
        .unwrap()
        .iter()
        .filter(|item| item.delayed)
        .cloned()
        .collect::<Vec<_>>();
    ensure!(
        captured.len() == 1
            && captured[0].method == "__streamAppend"
            && !captured[0].dropped
            && captured[0].scope == stream_scope()?
    );
    let request = cellar::application::decode_rpc(&cellar::engine_api::RpcData::Structured(
        captured[0].request.clone().into(),
    ))?
    .to_json()?;
    let reply = cellar::application::decode_rpc_result(&cellar::engine_api::RpcData::Structured(
        captured[0].response.clone().into(),
    ))?
    .to_json()?;
    ensure!(
        reply["error"]["code"] == "StreamFull",
        "refusal was not proven: {reply}"
    );
    let event: Value = serde_json::from_slice(
        &base64::engine::general_purpose::STANDARD.decode(
            request[0]["records"][0]
                .as_str()
                .context("pending envelope missing")?,
        )?,
    )?;
    ensure!(event["sequence"] == "2" && event["payload"] == json!({"value":2}));
    ensure!(
        event["source_incarnation"] == obligation["incarnation"]
            && event["lane"] == obligation["lanes"][0]["revision"]["lane"]
    );
    let rolled = status(&client, &destination)
        .await
        .context("read-only old-owner clock audit after rollback")?;
    ensure!(
        number(&rolled["observation"]["observed_ms"])? >= expiry_cut
            && rolled["observation"]["lease_active"] == false
            && rolled["checkpoint"] == "0"
            && rolled["identity"] == initial["identity"]
            && rolled["lease"] == initial["lease"],
        "clock-only cut was lost or expiry maintenance changed the original lease: {rolled}"
    );
    ensure!(history(&client, &destination).await? == accepted);
    if followers {
        // Fleet proof can release the response before the independent bucket
        // sync task attempts its blocked PUT. Observe that real attempt within
        // the original call budget instead of assuming a particular poll order.
        let publication_wait = Instant::now();
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(submitted + operation_budget),
            async {
                while outage.refused() == 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            },
        )
        .await
        .context("no actual destination publication was refused within the original budget")?;
        eprintln!(
            "clock refusal bucket attempt observed after {}us",
            publication_wait.elapsed().as_micros()
        );
        ensure!(
            replicas(&observer, &bucket).await? == baseline,
            "clock reached bucket before follower-only audit"
        );
    }
    save_clock_evidence(
        directory,
        "refusal-prefix.json",
        &json!({"format":1,"source_response":pending_write,"obligation":obligation,
            "event":event,"request_bytes":base64::engine::general_purpose::STANDARD.encode(&captured[0].request),
            "response_bytes":base64::engine::general_purpose::STANDARD.encode(&captured[0].response),
            "rolled":rolled,"expiry_cut":expiry_cut}),
    )?;
    ensure!(
        submitted.elapsed() < operation_budget,
        "refusal capture/audit exceeded the original operation budget"
    );
    proxy.capture.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), proxy.wait_for_release())
        .await
        .context("captured clock-refusal response was not released within five seconds")??;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = pending(&client, &source)
                .await
                .context("consume captured StreamFull refusal")?;
            if state["pending_rows"] == "1"
                && state["lanes"][0]["last_error"]
                    .as_str()
                    .is_some_and(|text| text.starts_with("StreamFull:"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("source must consume the typed refusal")??;

    // Forbid later destination publication from repairing the measured prefix.
    // Forced join precedes discarding only this fixture's old-owner directory.
    outage.block();
    proxy.capture.expect_upstream_close();
    let old_services = destination.services();
    let outcome = tokio::time::timeout(Duration::from_secs(10), destination.cancel())
        .await
        .context("clock-refusal destination did not join cancellation within ten seconds")??;
    ensure!(outcome == NodeOutcome::Cancelled);
    ensure!(old_services
        .configure_metrics(Arc::new(HostMetricsSample::default))
        .is_err());
    drop(old_services);
    std::fs::remove_dir_all(&destination_data)?;
    peer_claim.block();
    if followers {
        ensure!(
            replicas(&observer, &bucket).await? == baseline,
            "shutdown repaired the follower-only prefix"
        );
    }
    // Retained-proof cuts change only expiry after the old domain joins.
    // The separate metadata-loss cut deletes that proof root deliberately;
    // admission must remain blocked until its original fields return.
    let mut cold_replacement = None;
    let mut cold_read_retries = 0;
    let old_lease = if missing_metadata {
        observer
            .run(async {
                let key = "nodes/refusal-destination.json";
                let (bytes, _) = bucket
                    .get(key)
                    .await?
                    .context("old lease missing before loss")?;
                let saved: Value = serde_json::from_slice(&bytes)?;
                bucket.delete(key).await?;
                ensure!(bucket.get(key).await?.is_none());
                Ok::<_, anyhow::Error>(saved)
            })
            .await??
    } else if after_capture {
        observer
            .run(async {
                let (bytes, _) = bucket
                    .get("nodes/refusal-destination.json")
                    .await?
                    .context("retained root missing before capture")?;
                Ok::<_, anyhow::Error>(serde_json::from_slice::<Value>(&bytes)?)
            })
            .await??
    } else if cold_followers {
        let old_source_services = source.services();
        stopped(source)
            .await
            .context("join original source/follower before cold recovery")?;
        ensure!(old_source_services
            .configure_metrics(Arc::new(HostMetricsSample::default))
            .is_err());
        let old_witness = witness.take().context("cold witness missing")?;
        let old_witness_services = old_witness.services();
        stopped(old_witness)
            .await
            .context("join original witness before cold recovery")?;
        ensure!(old_witness_services
            .configure_metrics(Arc::new(HostMetricsSample::default))
            .is_err());
        drop(old_source_services);
        drop(old_witness_services);
        ensure!(
            replicas(&observer, &bucket).await? == baseline,
            "follower shutdown repaired the old-owner payload prefix"
        );
        let prior = expire_joined_owner(&observer, &bucket).await?;
        let (mut source_config, mut source_services) = inputs(
            "refusal-source",
            &directory.path().join("source"),
            &source_core,
            Some(bucket.clone()),
            0,
        )
        .await?;
        source_config.actor.lease_ttl_ms = 120_000;
        source_config.startup.recovery_retry_backoff = Duration::from_secs(120);
        let (mut witness_config, mut witness_services) = inputs(
            "refusal-witness",
            &directory.path().join("witness"),
            &witness_core,
            Some(bucket.clone()),
            0,
        )
        .await?;
        witness_config.actor.lease_ttl_ms = 120_000;
        witness_config.startup.recovery_retry_backoff = Duration::from_secs(120);
        source_services
            .fleet
            .as_mut()
            .context("cold source fleet missing")?
            .bucket
            .cas_store = peer_claim.clone();
        witness_services
            .fleet
            .as_mut()
            .context("cold witness fleet missing")?
            .bucket
            .cas_store = peer_claim.clone();
        let mut source_graph = graph(1, false);
        source_graph.primary.event_streams.clear();
        let mut witness_graph = graph(1, false);
        witness_graph.primary.event_streams.clear();
        if matches!(loss, MetadataLoss::DelayedColdRead) {
            // Isolate the read-audit deadline from the separate binding
            // activation deadline. These real cold peers still serve their
            // retained follower proofs; restore their bindings after recovery.
            source_graph.primary.options.stream_bindings.clear();
            witness_graph.primary.options.stream_bindings.clear();
        }
        // Start both cold follower servers and the selected successor together.
        // Binding activation needs that successor; only it may claim the stream.
        // No original domain survives and no activation deadline is changed.
        let (mut config, mut services, external) = replacement_inputs(
            directory.path(),
            &replacement_core,
            replacement_bucket.clone(),
            false,
        )
        .await?;
        let successor_proxy = delivery_proxy(&mut config, &services).await?;
        let read_cut = matches!(loss, MetadataLoss::DelayedColdRead).then(|| {
            RootReadCut::all_reads(
                replacement_bucket.store.clone(),
                format!("{}nodes/refusal-destination.json", bucket.prefix),
            )
        });
        if let Some(cut) = &read_cut {
            let fleet = services
                .fleet
                .as_mut()
                .context("cold replacement fleet missing")?;
            fleet.bucket.store = cut.clone();
            fleet.lease_bucket.store = cut.clone();
        }
        let release_read = async {
            if let Some(cut) = &read_cut {
                tokio::time::timeout(Duration::from_secs(5), cut.entered.acquire())
                    .await
                    .context("cold read never reached real root store")??
                    .forget();
                // The first ordinary RPC must expire. Keep the independent
                // five-second read audit and every mutation deadline unchanged.
                let held = cut.snapshot()?;
                let held_started = Instant::now();
                tokio::time::sleep(operation_budget + Duration::from_millis(100)).await;
                cut.release.add_permits(1);
                save_clock_evidence(
                    directory,
                    "delayed-cold-read.json",
                    &json!({
                        "format":1,"original_root":held,"held_ms":held_started.elapsed().as_millis(),
                        "rpc_budget_ms":operation_budget.as_millis(),"audit_budget_ms":5000
                    }),
                )?;
            }
            Ok::<_, anyhow::Error>(())
        };
        let cold_started = Instant::now();
        let mut cold_node = None;
        let start_replacement = Box::pin(async {
            cold_node =
                Some(Node::start(config, services, external, applications("replacement")?).await?);
            let node = cold_node.as_ref().unwrap();
            // The CAS-only fault prevents both peers from owning the partition.
            // It cannot create ownership on the permitted successor: send its
            // read-only request while the peers are still resolving bindings.
            // The original post-startup state audit below remains independent.
            // The retained followers are still starting. An unproven read can
            // be refused before they serve recovery evidence; keep this audit
            // within its original five-second bound instead of cancelling the
            // successor that their binding activation also needs.
            match read_status(&client, node, expires, true).await {
                Ok((_, read_retries)) => Ok(read_retries),
                Err(error) => {
                    let read_error_elapsed_ms = cold_started.elapsed().as_millis();
                    // Match the old error path's immediate domain closure. Keep
                    // the handle only long enough to join its owned work.
                    let joined = cold_node.take().unwrap().cancel().await;
                    let root_observation = cold_root_snapshot(&observer, &bucket).await;
                    let saved = save_clock_evidence(
                        directory,
                        "cold-read-failure.json",
                        &json!({
                            "format":1,"elapsed_ms":read_error_elapsed_ms,"error":format!("{error:#}"),
                            "roots_after_replacement_join":root_observation.unwrap_or_else(|e|json!({"error":format!("{e:#}")})),
                            "replacement_cancel_join":format!("{joined:?}")
                        }),
                    );
                    Err(error).with_context(||format!("cold successor read-only recovery audit; diagnostic={saved:?}; cancel_join={joined:?}"))
                }
            }
        });
        let start_source = Box::pin(Node::start(
            source_config,
            source_services,
            source_graph,
            applications("source")?,
        ));
        let start_witness = Box::pin(Node::start(
            witness_config,
            witness_services,
            witness_graph,
            applications("witness")?,
        ));
        let (source_result, witness_result, replacement_result, released) =
            tokio::join!(start_source, start_witness, start_replacement, release_read);
        if source_result.is_err()
            || witness_result.is_err()
            || replacement_result.is_err()
            || released.is_err()
        {
            let startup_elapsed_ms = cold_started.elapsed().as_millis();
            let before = cold_root_snapshot(&observer, &bucket).await;
            let observations = json!({"source":source_result.as_ref().err().map(|e|format!("{e:#}")),
                "witness":witness_result.as_ref().err().map(|e|format!("{e:#}")),
                "replacement":replacement_result.as_ref().err().map(|e|format!("{e:#}")),
                "elapsed_ms":startup_elapsed_ms});
            let mut primary = replacement_result
                .err()
                .map(|e| e.context("start cold recovery successor"))
                .or_else(|| {
                    released
                        .err()
                        .map(|e| e.context("release original cold read"))
                });
            let mut joined = Vec::new();
            for (name, result) in [("source", source_result), ("witness", witness_result)] {
                match result {
                    Ok(node) => joined.push(
                        json!({"node":name,"cancel_join":format!("{:?}",node.cancel().await)}),
                    ),
                    Err(error) => {
                        primary.get_or_insert_with(|| {
                            error.context(format!("restart retained cold {name}"))
                        });
                    }
                }
            }
            if let Some(node) = cold_node.take() {
                joined.push(
                    json!({"node":"replacement","cancel_join":format!("{:?}",node.cancel().await)}),
                );
            }
            let after = cold_root_snapshot(&observer, &bucket).await;
            observer.shutdown().await;
            let evidence = json!({"format":1,"stage":"cold startup before receipt replay",
                "startup":observations,"returned_nodes":joined,
                "predecessor_proxy_failure":proxy.capture.failure(),
                "successor_proxy_failure":successor_proxy.capture.failure(),
                "before_cancel":before.unwrap_or_else(|e|json!({"error":format!("{e:#}")})),
                "after_cancel":after.unwrap_or_else(|e|json!({"error":format!("{e:#}")}))});
            let saved = save_clock_evidence(directory, "cold-start-failure.json", &evidence);
            return Err(primary.unwrap()).with_context(|| {
                format!("cold restart observations={observations}; evidence={saved:?}")
            });
        }
        cold_read_retries = replacement_result?;
        if read_cut.is_some() {
            ensure!(
                cold_read_retries > 0,
                "delayed original root read did not exercise RPC expiry"
            );
        }
        cold_replacement = Some((cold_node.take().unwrap(), successor_proxy));
        source = source_result.context("restart retained cold source/follower")?;
        let mut node = witness_result.context("restart retained cold witness")?;
        if read_cut.is_some() {
            for (name, peer) in [("source", &mut source), ("witness", &mut node)] {
                let mut definition = graph(1, false);
                definition.primary.event_streams.clear();
                if let Err(error) = peer.adopt(definition, applications(name)?).await {
                    let elapsed_ms = cold_started.elapsed().as_millis();
                    // Observe only after failure. These roots are diagnostic
                    // samples, not a proof of their state at the deadline.
                    let roots = cold_root_snapshot(&observer, &bucket).await;
                    let saved = save_clock_evidence(
                        directory,
                        "cold-adopt-failure.json",
                        &json!({"format":1,"node":name,"elapsed_ms":elapsed_ms,
                            "error":format!("{error:#}"),"roots_after_failure":
                            roots.unwrap_or_else(|e|json!({"error":format!("{e:#}")}))}),
                    );
                    return Err(error).context(format!(
                        "restore cold {name} bindings after {elapsed_ms}ms; evidence={saved:?}"
                    ));
                }
            }
        }
        super::super::ready(&source).await?;
        super::super::ready(&node).await?;
        witness = Some(node);
        prior
    } else {
        expire_joined_owner(&observer, &bucket).await?
    };
    if followers {
        ensure!(
            old_lease["log"]["state"] == "open"
                && old_lease["log"]["may_have_fleet_acks"] == true
                && old_lease["log"]["ensemble"]
                    .as_array()
                    .is_some_and(|members| !members.is_empty()),
            "follower evidence not retained: {old_lease}"
        );
    }
    ensure!(
        wall() < expires,
        "ordinary UTC could repair the future clock before recovery audit"
    );
    let mut live_identity = None;
    let (mut replacement_node, mut successor_proxy) = if let Some(pair) = cold_replacement {
        pair
    } else {
        let (mut config, mut services, external) = replacement_inputs(
            directory.path(),
            &replacement_core,
            replacement_bucket.clone(),
            same_name,
        )
        .await?;
        let recovering_node = config.identity.node.clone();
        let cut = (after_capture || after_recovery || during_recovery).then(|| {
            let key = format!("{}nodes/refusal-destination.json", bucket.prefix);
            if during_recovery {
                RootReadCut::after_claim(bucket.store.clone(), key, config.identity.node.clone())
            } else if after_recovery {
                // Identity capture reads once. The next lease-pool GET is the
                // core's acquisition read after recover_self has completed.
                RootReadCut::after_reads(bucket.store.clone(), key, 1)
            } else {
                RootReadCut::new(bucket.store.clone(), key)
            }
        });
        if let Some(cut) = cut.as_ref() {
            let fleet = services
                .fleet
                .as_mut()
                .context("replacement fleet missing")?;
            if during_recovery {
                // The recovery reader uses the ordinary bucket; identity
                // capture and the core's lease CAS retain their original pool.
                fleet.bucket.store = cut.clone();
            } else {
                fleet.lease_bucket.store = cut.clone();
            }
            config.startup.recovery_attempts = 1;
        }
        let mut successor_proxy = delivery_proxy(&mut config, &services).await?;
        let boot = Box::pin(Node::start(
            config,
            services,
            external,
            applications("replacement")?,
        ));
        let replacement_node = if let Some(cut) = cut.as_ref() {
            let mut live_recoverer = None;
            let proof_before = follower_proof(directory.path(), &old_lease)?;
            let proof_at_cut = std::sync::Mutex::new((baseline.clone(), proof_before));
            let replacement_root = replaced_root.then(|| {
                let mut root = old_lease.clone();
                root["lineage"] = json!("ffffffffffffffffffffffffffffffff");
                root.as_object_mut().unwrap().remove("log");
                root
            });
            let erase = async {
                tokio::time::timeout(Duration::from_secs(5), cut.entered.acquire())
                    .await
                    .context("root recovery cut was not entered within five seconds")??
                    .forget();
                let captured = cut.snapshot()?;
                if during_recovery {
                    ensure!(
                        captured["node"] == old_lease["node"]
                            && captured["lineage"] == old_lease["lineage"]
                            && captured["generation"] == old_lease["generation"]
                            && captured["log"]["state"] == "recovering"
                            && captured["log"]["claimant"] == recovering_node
                            && captured["log"]["epoch"] == old_lease["log"]["epoch"]
                            && captured["log"]["ensemble"] == old_lease["log"]["ensemble"]
                            && captured["log"]["may_have_fleet_acks"] == true,
                        "recovery did not capture its actual claimed predecessor: {captured}"
                    );
                    *proof_at_cut.lock().unwrap() = (
                        replicas(&observer, &bucket).await?,
                        follower_proof(directory.path(), &captured)?,
                    );
                } else if after_recovery {
                    ensure!(
                        captured["node"] == old_lease["node"]
                            && captured["lineage"] == old_lease["lineage"]
                            && captured["generation"] == old_lease["generation"]
                            && captured["log"]["state"] == "sealed"
                            && captured["log"]["epoch"] == old_lease["log"]["epoch"],
                        "lease acquisition did not capture the recovered predecessor: {captured}"
                    );
                    let payload = replicas(&observer, &bucket).await?;
                    ensure!(!payload.is_empty(), "sealed recovery published no payload");
                    *proof_at_cut.lock().unwrap() =
                        (payload, follower_proof(directory.path(), &captured)?);
                } else {
                    ensure!(
                        captured == old_lease,
                        "startup captured another root than the retained proof"
                    );
                }
                if during_recovery {
                    require_recovering_root_cut(&observer, &bucket, &captured, &first_owner)
                        .await?;
                }
                observer
                    .run(async {
                        let key = "nodes/refusal-destination.json";
                        if let Some(replacement_root) = replacement_root.as_ref() {
                            let (_, token) = bucket
                                .get(key)
                                .await?
                                .context("captured root disappeared before replacement")?;
                            ensure!(
                                bucket
                                    .put_cas(
                                        key,
                                        serde_json::to_vec(replacement_root)?,
                                        Some(&token)
                                    )
                                    .await?
                                    .is_some(),
                                "captured root changed before controlled replacement"
                            );
                        } else {
                            bucket.delete(key).await?;
                            ensure!(bucket.get(key).await?.is_none());
                        }
                        Ok::<_, anyhow::Error>(())
                    })
                    .await??;
                cut.release.add_permits(1);
                Ok::<_, anyhow::Error>(())
            };
            let error = if foreign_recovery {
                // A foreign node starts under its own root. The old owner's
                // recovery is required by the cold route, not its own boot.
                // Recovery may begin before readiness. Poll the controller
                // alongside startup so its held root reads cannot deadlock the
                // readiness check that used to precede the controller.
                let attempt = async {
                    let node = boot.await.context("start foreign recovery node")?;
                    super::super::ready(&node)
                        .await
                        .context("foreign recovery readiness with active root controller")?;
                    if live_recovery {
                        let root = observer
                            .run(async {
                                let (bytes, _) = bucket
                                    .get("nodes/refusal-replacement.json")
                                    .await?
                                    .context("live recoverer's own root missing")?;
                                Ok::<_, anyhow::Error>(serde_json::from_slice::<Value>(&bytes)?)
                            })
                            .await??;
                        ensure!(root["generation"] == node.info().lease_generation);
                        live_identity = Some((node.services(), root));
                    }
                    let routed = Instant::now();
                    let result = status(&client, &node).await;
                    Ok::<_, anyhow::Error>((node, result, routed))
                };
                let (attempt, erased) = tokio::join!(attempt, erase);
                erased?;
                let (node, result, routed) = attempt?;
                let elapsed = routed.elapsed();
                if live_recovery {
                    // Seed another real resolution after the first interlock
                    // finishes. Neither a cached refusal nor a stuck claim may
                    // require restarting this node when valid metadata returns.
                    let again = Instant::now();
                    let failure = status(&client, &node)
                        .await
                        .err()
                        .context("live recoverer admitted a second read without metadata")?;
                    ensure!(
                        again.elapsed() < operation_budget
                            && format!("{failure:#}").contains(
                                "Unavailable: event RPC did not return a proven result"
                            )
                            && !node.is_closed(),
                        "second live recovery refusal changed certainty or closed its node: {failure:#}"
                    );
                    live_recoverer = Some(node);
                } else {
                    successor_proxy.capture.expect_upstream_close();
                    stopped(node)
                        .await
                        .context("join foreign recovery attempt before returning metadata")?;
                }
                ensure!(
                    elapsed < operation_budget,
                    "foreign recovery did not refuse within the original route budget"
                );
                result
                    .err()
                    .context("foreign recovery admitted the routed read")?
            } else {
                let (result, erased) = tokio::join!(boot, erase);
                erased?;
                match result {
                    Err(error) => error,
                    Ok(node) => {
                        let view = status(&client, &node).await;
                        let actual = owner(&observer, &bucket).await?;
                        stopped(node).await?;
                        anyhow::bail!("root loss admitted a successor (after_recovery={after_recovery}): {view:?}; {first_owner} -> {actual}");
                    }
                }
            };
            let boundary = if foreign_recovery {
                "Unavailable: event RPC did not return a proven result"
            } else if replaced_during_recovery {
                "node lineage changed; recovery is unproven"
            } else if during_recovery {
                "missing node lease for refusal-destination/"
            } else if after_recovery {
                "node authority was not established before startup deadline"
            } else if replaced_after_capture {
                "own node lineage changed before recovery"
            } else {
                "missing retained node lease after lineage capture"
            };
            ensure!(
                format!("{error:#}").contains(boundary),
                "recovery refused at another boundary: {error:#}"
            );
            if let Some(replacement_root) = replacement_root.as_ref() {
                observer
                    .run(async {
                        let (bytes, _) = bucket
                            .get("nodes/refusal-destination.json")
                            .await?
                            .context("replacement root disappeared")?;
                        ensure!(
                            serde_json::from_slice::<Value>(&bytes)? == *replacement_root,
                            "startup overwrote the replacement root"
                        );
                        Ok::<_, anyhow::Error>(())
                    })
                    .await??;
            }
            ensure!(
                owner(&observer, &bucket).await? == first_owner,
                "failed startup replaced the original cell owner"
            );
            let payload_after = replicas(&observer, &bucket).await?;
            let followers_after = follower_proof(directory.path(), &old_lease)?;
            if during_recovery {
                // The stale response can legitimately let an in-flight gather
                // seal followers and publish per-cell copies before its final
                // root read refuses. That work cannot remove accepted bytes.
                let before = proof_at_cut.lock().unwrap();
                ensure!(
                    before.0.iter().all(|entry| payload_after.contains(entry)),
                    "failed recovery removed or changed retained bucket proof"
                );
                let batches = |proof: &[(String, Vec<u8>)]| {
                    proof
                        .iter()
                        .filter(|(name, _)| name.ends_with(".batch"))
                        .cloned()
                        .collect::<Vec<_>>()
                };
                ensure!(
                    batches(&before.1) == batches(&followers_after),
                    "failed recovery lost or changed the acknowledged follower batches"
                );
            } else {
                ensure!(
                    (payload_after, followers_after) == *proof_at_cut.lock().unwrap(),
                    "failed startup changed payload or follower proof after the cut"
                );
            }
            if (after_recovery || during_recovery) && !replaced_root {
                observer
                    .run(async {
                        ensure!(
                            bucket
                                .get("nodes/refusal-destination.json")
                                .await?
                                .is_none(),
                            "refusal fabricated a missing retained root"
                        );
                        Ok::<_, anyhow::Error>(())
                    })
                    .await??;
            }
            let returned = if after_recovery || during_recovery {
                cut.snapshot()?
            } else {
                old_lease.clone()
            };
            restore_metadata(&observer, &bucket, &returned, replaced_root).await?;
            if let Some(node) = live_recoverer {
                node
            } else {
                successor_proxy.capture.expect_upstream_close();
                successor_proxy.stop().await?;
                let (mut config, services, external) = replacement_inputs(
                    directory.path(),
                    &replacement_core,
                    replacement_bucket.clone(),
                    same_name,
                )
                .await?;
                successor_proxy = delivery_proxy(&mut config, &services).await?;
                Node::start(config, services, external, applications("replacement")?).await?
            }
        } else {
            boot.await?
        };
        (replacement_node, successor_proxy)
    };
    super::super::ready(&replacement_node).await?;
    if missing_metadata {
        let refused = status(&client, &replacement_node).await;
        let unchanged = owner(&observer, &bucket).await?;
        ensure!(
            refused.is_err() && unchanged == first_owner,
            "missing folded metadata admitted a successor: {refused:?}; {first_owner} -> {unchanged}"
        );
        ensure!(
            replicas(&observer, &bucket).await? == baseline,
            "missing-metadata refusal changed the bucket payload prefix"
        );
        if same_name {
            observer
                .run(async {
                    let (bytes, _) = bucket
                        .get("nodes/refusal-destination.json")
                        .await?
                        .context("fresh lineage lease missing")?;
                    let fresh: Value = serde_json::from_slice(&bytes)?;
                    ensure!(
                        fresh["lineage"] != old_lease["lineage"],
                        "missing root reused the old lineage"
                    );
                    Ok::<_, anyhow::Error>(())
                })
                .await??;
            successor_proxy.capture.expect_upstream_close();
            stopped(replacement_node)
                .await
                .context("join refused same-name node before returning old metadata")?;
            restore_metadata(&observer, &bucket, &old_lease, true).await?;
            successor_proxy.stop().await?;
            let (mut config, services, external) = replacement_inputs(
                directory.path(),
                &replacement_core,
                replacement_bucket.clone(),
                true,
            )
            .await?;
            successor_proxy = delivery_proxy(&mut config, &services).await?;
            replacement_node =
                Node::start(config, services, external, applications("replacement")?).await?;
            super::super::ready(&replacement_node).await?;
        } else {
            restore_metadata(&observer, &bucket, &old_lease, false).await?;
        }
    }
    let recovery_read = Instant::now();
    let (recovered, mut read_retries) = recovered_status(&client, &replacement_node, expires)
        .await
        .context("audit current replacement clock")?;
    read_retries += cold_read_retries;
    if let Some((services, prior)) = &live_identity {
        ensure!(
            read_retries == 0
                && recovery_read.elapsed() < operation_budget
                && !replacement_node.is_closed()
                && Arc::ptr_eq(services, &replacement_node.services())
                && prior["generation"] == replacement_node.info().lease_generation,
            "live recovery changed its process or required another audit attempt"
        );
        observer
            .run(async {
                let (bytes, _) = bucket
                    .get("nodes/refusal-replacement.json")
                    .await?
                    .context("resumed recoverer's own root missing")?;
                let root: Value = serde_json::from_slice(&bytes)?;
                ensure!(
                    root["generation"] == prior["generation"]
                        && root["lineage"] == prior["lineage"],
                    "recovery replaced the live node's persisted identity"
                );
                Ok::<_, anyhow::Error>(())
            })
            .await??;
    }
    let next_owner = owner(&observer, &bucket).await?;
    ensure!(
        next_owner["node"]
            == if same_name {
                "refusal-destination"
            } else {
                "refusal-replacement"
            },
        "recovery did not use the measured successor: {next_owner}"
    );
    ensure!(
        (if same_name {
            next_owner["node"] == first_owner["node"]
        } else {
            next_owner["node"] != first_owner["node"]
        }) && (!same_name || next_owner["lineage"] == first_owner["lineage"])
            && next_owner["epoch"].as_u64().context("new epoch missing")?
                > first_owner["epoch"].as_u64().context("old epoch missing")?,
        "recovery used the wrong successor: {first_owner} -> {next_owner}"
    );
    ensure!(
        wall() < expires
            && number(&recovered["observation"]["observed_ms"])? >= expiry_cut
            && recovered["checkpoint"] == "0"
            && recovered["identity"] == initial["identity"]
            && recovered["observation"]["lease_active"] == false,
        "successor lost the proven refusal clock before receipt reuse: {recovered}"
    );
    if same_name {
        // A same-name successor also changes its endpoint/generation. A
        // standalone read may refresh a closed predecessor route; never use
        // ordinary mutation replay to repair the measured recovery prefix.
        let (view, retries) = read_status(&client, &source, expires, true)
            .await
            .context("refresh source read-only route after same-name recovery")?;
        read_retries += retries;
        ensure!(
            view["checkpoint"] == "0"
                && view["identity"] == initial["identity"]
                && number(&view["observation"]["observed_ms"])? >= expiry_cut
                && view["observation"]["lease_active"] == false
        );
    }
    ensure!(
        history(&client, &source)
            .await
            .context("audit recovered history through original source")?
            == accepted
    );
    let retained = pending(&client, &source)
        .await
        .context("retained successor source obligation")?;
    ensure!(
        retained["pending_rows"] == "1"
            && retained["incarnation"] == obligation["incarnation"]
            && retained["lanes"][0]["revision"]["lane"]
                == obligation["lanes"][0]["revision"]["lane"]
            && retained["lanes"][0]["revision"]["next_sequence"] == "3"
            && retained["lanes"][0]["revision"]["settled_through"] == "1"
    );
    let stale = settle(&client, &source, "projection", "ack", &receipt, 0).await?;
    ensure!(
        matches!(
            stale["error"].as_str(),
            Some("LeaseExpired" | "StaleReceipt")
        ),
        "old receipt advanced after recovery: {stale}"
    );
    let producer_owner = observer
        .run(async {
            let (bytes, _) = bucket
                .get(&format!("cells/{producer_scope}/own.json"))
                .await?
                .context("recovered producer owner missing")?;
            Ok::<_, anyhow::Error>(serde_json::from_slice::<Value>(&bytes)?)
        })
        .await??;
    ensure!(
        producer_owner["node"] != next_owner["node"],
        "co-located dispatch cannot qualify a peer submission: {producer_owner}"
    );
    // Arm before resuming predecessor traffic: a resumed append can fail and
    // retry on the successor immediately. Arming afterwards can miss that
    // submission and spend the phase budget waiting for another random retry.
    // Select the next automatic submission before obtaining a fresh lease;
    // no source row, retry policy, clock or ordinary call budget is changed.
    successor_proxy.capture.hold_requests(&["__streamAppend"]);
    let retry_wait = Instant::now();
    proxy.capture.resume_appends();
    let arrival = tokio::time::timeout(
        Duration::from_secs(60) + operation_budget,
        successor_proxy.capture.request_entered.acquire(),
    )
    .await;
    if arrival.is_err() {
        let cutoff_utc = wall();
        let source_view = pending(&client, &source).await;
        let destination_view = status(&client, &replacement_node).await;
        let actual_owner = owner(&observer, &bucket).await;
        anyhow::bail!("next automatic append did not reach the recovered owner by the original phase deadline; source={source_view:?}; destination={destination_view:?}; owner={actual_owner:?}; cutoff_utc={cutoff_utc}");
    }
    arrival??.forget();
    let retry_wait_ms = retry_wait.elapsed().as_millis();
    let retry_held = Instant::now();
    let held = successor_proxy
        .capture
        .held_requests
        .lock()
        .unwrap()
        .clone();
    ensure!(
        held.len() == 1
            && held[0].method == "__streamAppend"
            && held[0].scope == stream_scope()?
            && cellar::application::decode_rpc(&cellar::engine_api::RpcData::Structured(
                held[0].body.clone().into(),
            ))?
            .to_json()?
                == request,
        "automatic recovery retry changed the original append"
    );
    let replay = fetch(
        &client,
        &source,
        "000000000000000000000000000000e2",
        "projection",
    )
    .await?;
    ensure!(
        replay["processed"] == 1
            && replay["batch"]["records"] == original["batch"]["records"]
            && number(&replay["batch"]["issued_ms"])? >= expiry_cut
            && replay["batch"]["receipt"] != original["batch"]["receipt"],
        "recovered lease replay changed records, processing or receipt identity: original={original}; replay={replay}; expiry_cut={expiry_cut}"
    );
    let replacement = Receipt::decode(
        replay["batch"]["receipt"]
            .as_str()
            .context("replacement receipt missing")?,
    )?;
    let acknowledgment = settle(&client, &source, "projection", "ack", &replacement, 0).await?;
    if acknowledgment != json!({"status":"Acknowledged"}) {
        let saved = save_clock_evidence(
            directory,
            "settlement-failure.json",
            &json!({"format":1,"stage":"replacement receipt","response":acknowledgment,
                "fetch":replay,"expiry_cut":expiry_cut,"cutoff_utc_ms":wall(),
                "held_append_ms":retry_held.elapsed().as_millis(),
                "predecessor_proxy_failure":proxy.capture.failure(),
                "successor_proxy_failure":successor_proxy.capture.failure()}),
        );
        anyhow::bail!(
            "replacement receipt acknowledgment refused: {acknowledgment}; evidence={saved:?}"
        );
    }
    let retry_held_ms = retry_held.elapsed().as_millis();
    ensure!(
        retry_held.elapsed() < operation_budget,
        "fresh fetch and acknowledgment exceeded the original pending append budget"
    );
    successor_proxy.capture.request_release.add_permits(1);
    let mut delivery_observation = Value::Null;
    let delivered = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            delivery_observation = pending(&client, &source).await?;
            if delivery_observation["pending_rows"] == "0" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await;
    if delivered.is_err() {
        let cutoff_utc = wall();
        let destination_view = status(&client, &replacement_node).await;
        anyhow::bail!("pending original obligation must deliver after acknowledgment: original five-second deadline elapsed; last_source={delivery_observation}; destination={destination_view:?}; cutoff_utc={cutoff_utc}");
    }
    delivered??;
    let second = fetch(
        &client,
        &source,
        "000000000000000000000000000000e3",
        "projection",
    )
    .await?;
    save_clock_evidence(
        directory,
        "post-recovery-fetch.json",
        &json!({"format":1,"source_pending":delivery_observation,
            "fetch":second,"original_record":request[0]["records"][0]}),
    )?;
    ensure!(
        second["processed"] == 3
            && second["batch"]["records"][0]["offset"] == "2"
            && second["batch"]["records"][0]["envelope"] == request[0]["records"][0]
    );
    let receipt = Receipt::decode(
        second["batch"]["receipt"]
            .as_str()
            .context("second receipt missing")?,
    )?;
    let acknowledgment = settle(&client, &source, "projection", "ack", &receipt, 0).await?;
    if acknowledgment != json!({"status":"Acknowledged"}) {
        let saved = save_clock_evidence(
            directory,
            "settlement-failure.json",
            &json!({"format":1,"stage":"delivered second record","response":acknowledgment,
                "fetch":second,"expiry_cut":expiry_cut,"cutoff_utc_ms":wall()}),
        );
        anyhow::bail!("second record acknowledgment refused: {acknowledgment}; evidence={saved:?}");
    }
    let finished = pending(&client, &source).await?;
    ensure!(
        finished["pending_rows"] == "0"
            && finished["incarnation"] == obligation["incarnation"]
            && finished["lanes"][0]["revision"]["next_sequence"] == "3"
            && finished["lanes"][0]["revision"]["settled_through"] == "2"
            && status(&client, &source).await?["checkpoint"] == "2"
    );
    eprintln!(
        "CLOCK_REFUSAL_EVIDENCE {}",
        json!({"proof":if followers {"followers"}else{"bucket"},"old_owner":first_owner,"successor":next_owner,
        "cold_followers":cold_followers,"missing_metadata":missing_metadata,"same_name":same_name,"after_capture":after_capture,"after_recovery":after_recovery,"during_recovery":during_recovery,"foreign_recovery":foreign_recovery,"live_recovery":live_recovery,"replaced_during_recovery":replaced_during_recovery,"replaced_after_recovery":replaced_after_recovery,"replaced_after_capture":replaced_after_capture,"read_retries":read_retries,"retry_wait_ms":retry_wait_ms,"retry_held_ms":retry_held_ms,"expiry_cut":expiry_cut,"recovered_clock":recovered["observation"]["observed_ms"],"checkpoint":"2","processed":3,"settled_through":"2"})
    );
    // All proof and state assertions are complete. Either a caller/follower
    // or the owner can close the upgraded log connections during teardown.
    successor_proxy.capture.expect_upstream_close();
    stopped(source)
        .await
        .context("join recovered refusal source")?;
    if let Some(node) = witness {
        stopped(node).await.context("join refusal witness")?;
    }
    stopped(replacement_node)
        .await
        .context("join refusal replacement")?;
    successor_proxy
        .stop()
        .await
        .context("join successor delivery proxy after complete clock audit")?;
    proxy
        .stop()
        .await
        .context("join predecessor delivery proxy after complete clock audit")?;
    observer.shutdown().await;
    for executor in [
        &destination_core,
        &source_core,
        &witness_core,
        &replacement_core,
    ] {
        ensure!(executor.handle().spawn(async { 7 }).await? == 7);
    }
    Ok(())
}
