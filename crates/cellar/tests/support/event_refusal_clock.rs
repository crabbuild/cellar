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
    lineage_return: bool,
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
                            || lineage_return
                                && error.to_string().contains(
                                    "Unavailable: event RPC did not return a proven result",
                                )) =>
                {
                    // A standalone read can safely repeat after an old-owner
                    // transport fails. No mutation/fetch/receipt is replayed.
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

#[derive(Clone, Copy)]
enum MetadataLoss {
    None,
    BeforeForeign,
    BeforeSameName,
    AfterCapture,
    ReplacedAfterCapture,
}

async fn run(followers: bool, cold_followers: bool, loss: MetadataLoss) -> Result<()> {
    let missing_metadata = matches!(
        loss,
        MetadataLoss::BeforeForeign | MetadataLoss::BeforeSameName
    );
    let same_name = matches!(
        loss,
        MetadataLoss::BeforeSameName
            | MetadataLoss::AfterCapture
            | MetadataLoss::ReplacedAfterCapture
    );
    let after_capture = matches!(
        loss,
        MetadataLoss::AfterCapture | MetadataLoss::ReplacedAfterCapture
    );
    let replaced_after_capture = matches!(loss, MetadataLoss::ReplacedAfterCapture);
    let directory = tempfile::tempdir()?;
    let destination_data = directory.path().join("destination");
    let destination_core = Executor::new("refusal-clock-destination")?;
    let source_core = Executor::new("refusal-clock-source")?;
    let witness_core = Executor::new("refusal-clock-witness")?;
    let replacement_core = Executor::new("refusal-clock-replacement")?;
    let observer = ExecutionDomain::new(tokio::runtime::Handle::current());
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    // Select the same-name node as the measured successor. Other real peers
    // retain their read, lease, follower and recovery services; only their
    // conditional writes to this cell's owner key are refused during the cut.
    let peer_claim = PublicationFault::with_prefixes(
        bucket.cas_store.clone(),
        format!("{}cells/{}/own.json", bucket.prefix, stream_scope()?),
        Vec::new(),
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
        Node::start(config, services, definition, applications("destination")?).await?;
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
    if same_name {
        services
            .fleet
            .as_mut()
            .context("source fleet missing")?
            .bucket
            .cas_store = peer_claim.clone();
    }
    config.actor.lease_ttl_ms = 120_000;
    config.startup.recovery_retry_backoff = Duration::from_secs(120);
    let operation_budget = Duration::from_millis(config.actor.core.operation_deadline_ms.unwrap());
    ensure!(operation_budget == Duration::from_secs(3));
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let mut source = Node::start(config, services, external, applications("source")?).await?;
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
        if same_name {
            services
                .fleet
                .as_mut()
                .context("witness fleet missing")?
                .bucket
                .cas_store = peer_claim.clone();
        }
        config.actor.lease_ttl_ms = 120_000;
        config.startup.recovery_retry_backoff = Duration::from_secs(120);
        let mut external = graph(1, false);
        external.primary.event_streams.clear();
        let node = Node::start(config, services, external, applications("witness")?).await?;
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
    ensure!(
        call(
            &client,
            &source,
            reqwest::Method::POST,
            "/write?source=producer"
        )
        .await?
            == "source:1"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while pending(&client, &source)
            .await
            .context("initial source settlement inspection")?["pending_rows"]
            != "0"
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("settle original obligation before clock-only cut")??;
    let original = fetch(
        &client,
        &source,
        "000000000000000000000000000000e1",
        "projection",
    )
    .await?;
    ensure!(original["processed"] == 1);
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
        outage.block();
    }
    let baseline = replicas(&observer, &bucket).await?;
    proxy.capture.hold_requests(&["__streamAppend"]);
    proxy.capture.delay_append();
    let submitted = Instant::now();
    ensure!(
        call(
            &client,
            &source,
            reqwest::Method::POST,
            "/write?source=producer"
        )
        .await?
            == "source:2"
    );
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
    offset.store(expiry_cut - wall(), Ordering::SeqCst);
    proxy.capture.request_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), proxy.capture.entered.acquire())
        .await??
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
        ensure!(
            outage.refused() > 0,
            "no actual destination publication was refused"
        );
        ensure!(
            replicas(&observer, &bucket).await? == baseline,
            "clock reached bucket before follower-only audit"
        );
    }
    ensure!(
        submitted.elapsed() < operation_budget,
        "refusal capture/audit exceeded the original operation budget"
    );
    proxy.capture.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), proxy.wait_for_release()).await??;
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
    let outcome = tokio::time::timeout(Duration::from_secs(10), destination.cancel()).await??;
    ensure!(outcome == NodeOutcome::Cancelled);
    ensure!(old_services
        .configure_metrics(Arc::new(HostMetricsSample::default))
        .is_err());
    drop(old_services);
    std::fs::remove_dir_all(&destination_data)?;
    if same_name {
        peer_claim.block();
    }
    if followers {
        ensure!(
            replicas(&observer, &bucket).await? == baseline,
            "shutdown repaired the follower-only prefix"
        );
    }
    // Retained-proof cuts change only expiry after the old domain joins.
    // The separate metadata-loss cut deletes that proof root deliberately;
    // admission must remain blocked until its original fields return.
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
        let (mut source_config, source_services) = inputs(
            "refusal-source",
            &directory.path().join("source"),
            &source_core,
            Some(bucket.clone()),
            0,
        )
        .await?;
        source_config.actor.lease_ttl_ms = 120_000;
        source_config.startup.recovery_retry_backoff = Duration::from_secs(120);
        let (mut witness_config, witness_services) = inputs(
            "refusal-witness",
            &directory.path().join("witness"),
            &witness_core,
            Some(bucket.clone()),
            0,
        )
        .await?;
        witness_config.actor.lease_ttl_ms = 120_000;
        witness_config.startup.recovery_retry_backoff = Duration::from_secs(120);
        let mut source_graph = graph(1, false);
        source_graph.primary.event_streams.clear();
        let mut witness_graph = graph(1, false);
        witness_graph.primary.event_streams.clear();
        // Both cold follower servers must exist before either startup can wait
        // for a binding's predecessor recovery. No original domain survives.
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
        let (source_result, witness_result) = tokio::join!(start_source, start_witness);
        source = source_result.context("restart retained cold source/follower")?;
        let node = witness_result.context("restart retained cold witness")?;
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
    let (mut config, mut services) = inputs(
        if same_name {
            "refusal-destination"
        } else {
            "refusal-replacement"
        },
        &directory.path().join("replacement"),
        &replacement_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    config.actor.lease_ttl_ms = 120_000;
    config.startup.recovery_retry_backoff = Duration::from_secs(120);
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    external.primary.options.stream_bindings.clear();
    let cut = after_capture.then(|| {
        RootReadCut::new(
            bucket.store.clone(),
            format!("{}nodes/refusal-destination.json", bucket.prefix),
        )
    });
    if let Some(cut) = cut.as_ref() {
        services
            .fleet
            .as_mut()
            .context("replacement fleet missing")?
            .lease_bucket
            .store = cut.clone();
        config.startup.recovery_attempts = 1;
    }
    let boot = Box::pin(Node::start(
        config,
        services,
        external,
        applications("replacement")?,
    ));
    let mut replacement_node = if let Some(cut) = cut.as_ref() {
        let proof_before = follower_proof(directory.path(), &old_lease)?;
        let replacement_root = replaced_after_capture.then(|| {
            let mut root = old_lease.clone();
            root["lineage"] = json!("ffffffffffffffffffffffffffffffff");
            root.as_object_mut().unwrap().remove("log");
            root
        });
        let erase = async {
            tokio::time::timeout(Duration::from_secs(5), cut.entered.acquire())
                .await??
                .forget();
            ensure!(
                cut.snapshot()? == old_lease,
                "startup captured another root than the retained proof"
            );
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
                                .put_cas(key, serde_json::to_vec(replacement_root)?, Some(&token))
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
        let (result, erased) = tokio::join!(boot, erase);
        erased?;
        let error = match result {
            Err(error) => error,
            Ok(node) => {
                let view = status(&client, &node).await;
                let actual = owner(&observer, &bucket).await?;
                stopped(node).await?;
                anyhow::bail!("root loss after capture admitted a successor: {view:?}; {first_owner} -> {actual}");
            }
        };
        let boundary = if replaced_after_capture {
            "own node lineage changed before recovery"
        } else {
            "missing retained node lease after lineage capture"
        };
        ensure!(
            format!("{error:#}").contains(boundary),
            "startup refused at another boundary: {error:#}"
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
        ensure!(
            replicas(&observer, &bucket).await? == baseline,
            "failed startup changed the original payload prefix"
        );
        ensure!(
            follower_proof(directory.path(), &old_lease)? == proof_before,
            "failed startup changed retained follower proof"
        );
        restore_metadata(&observer, &bucket, &old_lease, replaced_after_capture).await?;
        let (mut config, services) = inputs(
            "refusal-destination",
            &directory.path().join("replacement"),
            &replacement_core,
            Some(bucket.clone()),
            0,
        )
        .await?;
        config.actor.lease_ttl_ms = 120_000;
        config.startup.recovery_retry_backoff = Duration::from_secs(120);
        let mut external = graph(1, false);
        external.primary.event_streams.clear();
        external.primary.options.stream_bindings.clear();
        Node::start(config, services, external, applications("replacement")?).await?
    } else {
        boot.await?
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
            stopped(replacement_node)
                .await
                .context("join refused same-name node before returning old metadata")?;
            restore_metadata(&observer, &bucket, &old_lease, true).await?;
            let (mut config, services) = inputs(
                "refusal-destination",
                &directory.path().join("replacement"),
                &replacement_core,
                Some(bucket.clone()),
                0,
            )
            .await?;
            config.actor.lease_ttl_ms = 120_000;
            config.startup.recovery_retry_backoff = Duration::from_secs(120);
            let mut external = graph(1, false);
            external.primary.event_streams.clear();
            external.primary.options.stream_bindings.clear();
            replacement_node =
                Node::start(config, services, external, applications("replacement")?).await?;
            super::super::ready(&replacement_node).await?;
        } else {
            restore_metadata(&observer, &bucket, &old_lease, false).await?;
        }
    }
    let (recovered, mut read_retries) = recovered_status(&client, &replacement_node, expires)
        .await
        .context("audit current replacement clock")?;
    let next_owner = owner(&observer, &bucket).await?;
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
    proxy.capture.resume_appends();
    let stale = settle(&client, &source, "projection", "ack", &receipt, 0).await?;
    ensure!(
        matches!(
            stale["error"].as_str(),
            Some("LeaseExpired" | "StaleReceipt")
        ),
        "old receipt advanced after recovery: {stale}"
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
            && replay["batch"]["receipt"] != original["batch"]["receipt"]
    );
    let replacement = Receipt::decode(
        replay["batch"]["receipt"]
            .as_str()
            .context("replacement receipt missing")?,
    )?;
    ensure!(
        settle(&client, &source, "projection", "ack", &replacement, 0).await?
            == json!({"status":"Acknowledged"})
    );
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
    ensure!(
        settle(&client, &source, "projection", "ack", &receipt, 0).await?
            == json!({"status":"Acknowledged"})
    );
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
        "cold_followers":cold_followers,"missing_metadata":missing_metadata,"same_name":same_name,"after_capture":after_capture,"replaced_after_capture":replaced_after_capture,"read_retries":read_retries,"expiry_cut":expiry_cut,"recovered_clock":recovered["observation"]["observed_ms"],"checkpoint":"2","processed":3,"settled_through":"2"})
    );
    stopped(source)
        .await
        .context("join recovered refusal source")?;
    if let Some(node) = witness {
        stopped(node).await.context("join refusal witness")?;
    }
    stopped(replacement_node)
        .await
        .context("join refusal replacement")?;
    proxy.stop().await?;
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
