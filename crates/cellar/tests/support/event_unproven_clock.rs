//! A local clock commit is not a proven refusal. Hold real publication, then
//! force closure without allowing shutdown or replay to supply missing proof.

use super::event_clock_retention::{history, number, owner, pending, wall};
use super::event_refusal_clock::{expire_joined_owner, recovered_status, replicas};
use super::event_settlement_race::{fetch, settle, status};
use super::event_unproven_ack::PublicationFault;
use super::*;
use futures_util::FutureExt;
use serde_json::json;
use std::sync::atomic::AtomicI64;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unproven_clock_refusal_cannot_escape_and_does_not_revoke_the_original_lease() -> Result<()>
{
    let directory = tempfile::tempdir()?;
    let data = directory.path().join("destination");
    let destination_core = Executor::new("unproven-clock-destination")?;
    let source_core = Executor::new("unproven-clock-source")?;
    let replacement_core = Executor::new("unproven-clock-replacement")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let observer = ExecutionDomain::new(tokio::runtime::Handle::current());
    let fault = PublicationFault::with_prefixes(
        bucket.store.clone(),
        format!("{}cells/{}/ltx/", bucket.prefix, stream_scope()?),
        vec![format!("{}log/refusal-destination/", bucket.prefix)],
    );
    let mut protected = bucket.clone();
    protected.store = fault.clone();
    let offset = Arc::new(AtomicI64::new(0));
    let selected = offset.clone();
    let (mut config, mut services) = inputs(
        "refusal-destination",
        &data,
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
    fleet.fleet_durability = false;
    fleet.bucket = protected.clone();
    fleet.replica_storage = cellar::replica_storage::ReplicaStorage::new(
        protected.store.clone(),
        cellar::replica_storage::ReplicaStorageOptions {
            prefix: bucket.prefix.clone(),
            ..Default::default()
        },
    )?;
    let mut declaration = graph(1, false);
    declaration.primary.event_streams[0].retention = Retention::Required;
    declaration.primary.event_streams[0].subscriptions[0].required = true;
    let destination =
        Node::start(config, services, declaration, applications("destination")?).await?;
    super::super::ready(&destination).await?;
    let first_owner = owner(&observer, &bucket).await?;
    let epoch = first_owner["epoch"].as_u64().context("old epoch missing")?;
    let (mut config, services) = inputs(
        "unproven-clock-source",
        &directory.path().join("source"),
        &source_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    config.actor.lease_ttl_ms = 120_000;
    config.startup.recovery_retry_backoff = Duration::from_secs(120);
    ensure!(config.actor.core.operation_deadline_ms == Some(3_000));
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let source = Node::start(config, services, external, applications("source")?).await?;
    super::super::ready(&source).await?;
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
        while pending(&client, &source).await?["pending_rows"] != "0" {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let original = fetch(
        &client,
        &source,
        "000000000000000000000000000000f1",
        "projection",
    )
    .await?;
    ensure!(original["processed"] == 1);
    let receipt = Receipt::decode(
        original["batch"]["receipt"]
            .as_str()
            .context("original receipt missing")?,
    )?;
    let initial = status(&client, &destination).await?;
    let accepted = history(&client, &destination).await?;
    let expires = number(&original["batch"]["expires_ms"])?;
    ensure!(expires - number(&original["batch"]["issued_ms"])? == 30_000);
    ensure!(initial["checkpoint"] == "0" && initial["observation"]["lease_active"] == true);
    let baseline = replicas(&observer, &bucket).await?;

    fault.hold();
    let expiry_cut = expires + 1_000;
    offset.store(expiry_cut - wall(), Ordering::SeqCst);
    let mut extending = Box::pin(settle(
        &client,
        &source,
        "projection",
        "extend",
        &receipt,
        expires + 60_000,
    ));
    cellar::asyncrt::select! {
        result = &mut extending => anyhow::bail!("clock refusal escaped before publication: {result:?}"),
        entered = tokio::time::timeout(Duration::from_secs(5),fault.wait_for_publication()) => entered.context("hold clock-only replica publication")??,
    }
    offset.store(0, Ordering::SeqCst);
    let local = data
        .join(stream_scope()?)
        .join("ltx")
        .join(format!("e{epoch}"))
        .join("db.sqlite");
    let forensic = tokio::task::spawn_blocking(move || -> Result<(i64, i64, i64)> {
        // Read-only local evidence is not an externally proven observation.
        let db = rusqlite::Connection::open_with_flags(
            local,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        Ok((
            db.query_row(
                "SELECT logical_ms FROM __sys_stream WHERE singleton=1",
                [],
                |row| row.get(0),
            )?,
            db.query_row(
                "SELECT checkpoint FROM __sys_stream_subscriptions WHERE name='projection'",
                [],
                |row| row.get(0),
            )?,
            db.query_row("SELECT count(*) FROM __sys_stream_leases", [], |row| {
                row.get(0)
            })?,
        ))
    })
    .await??;
    ensure!(
        forensic.0 >= expiry_cut && forensic.1 == 0 && forensic.2 == 1,
        "local clock cut was not isolated: {forensic:?}"
    );
    ensure!(replicas(&observer, &bucket).await? == baseline);
    ensure!(
        extending.as_mut().now_or_never().is_none(),
        "unproven clock refusal became visible"
    );
    let result = tokio::time::timeout(Duration::from_secs(5), &mut extending)
        .await
        .context("unproven clock caller outcome")??;
    drop(extending);
    ensure!(
        result == json!({"error":"Ambiguous"}),
        "unproven expired receipt returned {result}"
    );
    ensure!(!source.is_closed());
    let old_services = destination.services();
    let mut closing = Box::pin(destination.cancel());
    let completed = closing.as_mut().now_or_never();
    fault.block();
    let outcome = match completed {
        Some(result) => result?,
        None => tokio::time::timeout(Duration::from_secs(10), closing)
            .await
            .context("join unproven clock owner")??,
    };
    ensure!(outcome == NodeOutcome::Cancelled);
    ensure!(old_services
        .configure_metrics(Arc::new(HostMetricsSample::default))
        .is_err());
    drop(old_services);
    ensure!(
        replicas(&observer, &bucket).await? == baseline,
        "shutdown published unproven clock"
    );
    std::fs::remove_dir_all(&data)?;
    expire_joined_owner(&observer, &bucket).await?;
    let (mut config, services) = inputs(
        "unproven-clock-replacement",
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
    let replacement = Node::start(config, services, external, applications("replacement")?).await?;
    super::super::ready(&replacement).await?;
    let (recovered, read_retries) = recovered_status(&client, &replacement, expires).await?;
    let successor = owner(&observer, &bucket).await?;
    ensure!(
        successor["node"] != first_owner["node"]
            && successor["epoch"].as_u64().context("new epoch missing")? > epoch
    );
    ensure!(
        wall() < expires
            && number(&recovered["observation"]["observed_ms"])? < expiry_cut
            && recovered["observation"]["lease_active"] == true
            && recovered["checkpoint"] == "0"
            && recovered["identity"] == initial["identity"]
            && recovered["lease"] == initial["lease"],
        "unproven refusal changed recovered authority before receipt reuse: {recovered}"
    );
    ensure!(history(&client, &replacement).await? == accepted);
    ensure!(
        call(
            &client,
            &source,
            reqwest::Method::GET,
            "/processed?source=projection"
        )
        .await?
            == "1"
    );
    ensure!(
        settle(&client, &source, "projection", "ack", &receipt, 0).await?
            == json!({"status":"Acknowledged"})
    );
    ensure!(
        settle(&client, &source, "projection", "ack", &receipt, 0).await?
            == json!({"status":"AlreadyAcknowledged"})
    );
    ensure!(status(&client, &replacement).await?["checkpoint"] == "1");
    let finished = pending(&client, &source).await?;
    ensure!(
        finished["pending_rows"] == "0"
            && finished["lanes"][0]["revision"]["next_sequence"] == "2"
            && finished["lanes"][0]["revision"]["settled_through"] == "1"
    );
    eprintln!(
        "UNPROVEN_CLOCK_EVIDENCE {}",
        json!({"local_clock":forensic.0,"expiry_cut":expiry_cut,
        "recovered_clock":recovered["observation"]["observed_ms"],"old_owner":first_owner,"successor":successor,
        "refusal":"Ambiguous","checkpoint":"1","processed":1,"read_retries":read_retries})
    );
    stopped(source)
        .await
        .context("join unproven clock source")?;
    stopped(replacement)
        .await
        .context("join unproven clock replacement")?;
    observer.shutdown().await;
    for executor in [&destination_core, &source_core, &replacement_core] {
        ensure!(executor.handle().spawn(async { 7 }).await? == 7);
    }
    Ok(())
}
