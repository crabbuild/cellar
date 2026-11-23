// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retain an old native CAS through readback and real OwnLog publication.
use super::*;
use anyhow::Result;
use futures_util::stream::BoxStream;
use futures_util::StreamExt as _;
use object_store::{path::Path as ObjectPath, *};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct StampRace {
    store: Arc<dyn ObjectStore>,
    key: String,
    armed: AtomicBool,
    attempts: AtomicUsize,
    entered: Semaphore,
    release: Arc<Semaphore>,
    committed: Arc<Semaphore>,
    first_committed: Arc<AtomicBool>,
    discovery: Arc<Semaphore>,
    first_without_log: AtomicBool,
    next_with_log: AtomicBool,
    rejected: AtomicBool,
    physical: Mutex<Option<cellar::asyncrt::TaskHandle<()>>>,
    observations: Mutex<Vec<(usize, u8, bool, u64)>>,
    gated_scans: AtomicUsize,
    unarmed_applied: AtomicUsize,
}

impl std::fmt::Debug for StampRace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StampRace").finish_non_exhaustive()
    }
}

impl std::fmt::Display for StampRace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("late native lease CAS across OwnLog publication")
    }
}

fn channel_error(error: impl std::error::Error + Send + Sync + 'static) -> object_store::Error {
    object_store::Error::Generic {
        store: "lease-stamp-fixture",
        source: Box::new(error),
    }
}

#[async_trait::async_trait]
impl ObjectStore for StampRace {
    async fn put_opts(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let attempt = (path.as_ref() == self.key && self.armed.load(Ordering::Acquire))
            .then(|| self.attempts.fetch_add(1, Ordering::AcqRel));
        if let Some(attempt) = attempt {
            let bytes: Vec<u8> = payload
                .iter()
                .flat_map(|chunk| chunk.iter().copied())
                .collect();
            let wire: serde_json::Value = serde_json::from_slice(&bytes).map_err(channel_error)?;
            {
                let mut observations = self.observations.lock().unwrap();
                if observations.len() < 16 {
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    observations.push((
                        attempt,
                        match wire["log"]["state"].as_str() {
                            None => 0,
                            Some("open") => 1,
                            Some("recovering") => 2,
                            Some("sealed") => 3,
                            Some(_) => 4,
                        },
                        matches!(&options.mode, PutMode::Update(_)),
                        wire["expires_ms"]
                            .as_u64()
                            .unwrap_or(0)
                            .saturating_sub(now_ms),
                    ));
                }
            }
            if attempt == 0 {
                self.first_without_log
                    .store(wire["log"].is_null(), Ordering::Release);
                let store = self.store.clone();
                let path = path.clone();
                let release = self.release.clone();
                let committed = self.committed.clone();
                let first_committed = self.first_committed.clone();
                let task = cellar::asyncrt::try_spawn(async move {
                    // Dropping the caller does not cancel this owned physical
                    // write. Its original bytes and guard remain unchanged.
                    if let Ok(permit) = release.acquire().await {
                        permit.forget();
                        let result = store.put_opts(&path, payload, options).await;
                        first_committed.store(result.is_ok(), Ordering::Release);
                        committed.add_permits(1);
                    }
                })
                .map_err(channel_error)?;
                assert!(self.physical.lock().unwrap().replace(task).is_none());
                self.entered.add_permits(1);
                return Err(channel_error(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "fixture lost the original CAS response",
                )));
            }
            if attempt == 1 {
                self.next_with_log
                    .store(wire["log"]["state"] == "open", Ordering::Release);
                // The actor cannot issue this next write until the ambiguous
                // first attempt's readback has returned the old root. Let that
                // first native CAS win the guard before executing this real CAS.
                self.release.add_permits(1);
                self.committed
                    .acquire()
                    .await
                    .map_err(channel_error)?
                    .forget();
            }
        }
        let result = self.store.put_opts(path, payload, options).await;
        if path.as_ref() == self.key && attempt.is_none() && result.is_ok() {
            self.unarmed_applied.fetch_add(1, Ordering::AcqRel);
        }
        if attempt == Some(1) {
            self.rejected.store(
                matches!(&result, Err(object_store::Error::Precondition { .. })),
                Ordering::Release,
            );
        }
        result
    }

    async fn get_opts(
        &self,
        path: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.store.get_opts(path, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.store.put_multipart_opts(path, options).await
    }

    async fn delete(&self, path: &ObjectPath) -> object_store::Result<()> {
        self.store.delete(path).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if prefix.is_some_and(|prefix| prefix.as_ref().trim_end_matches('/').ends_with("nodes")) {
            self.gated_scans.fetch_add(1, Ordering::AcqRel);
            let discovery = self.discovery.clone();
            let store = self.store.clone();
            let prefix = prefix.cloned();
            // Hold the first real recruitment scan, rather than race the
            // one-second posture-repair tick against the one-second lease.
            // The follower is present before this original scan executes.
            return Box::pin(
                futures_util::stream::once(async move {
                    let permit = discovery.acquire().await.expect("fixture discovery gate");
                    drop(permit);
                    store.list(prefix.as_ref())
                })
                .flatten(),
            );
        }
        self.store.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.store.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.store.copy_if_not_exists(from, to).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_late_cas_survives_real_log_publication() -> Result<()> {
    let observer = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = observer.run(original_late_cas_case()).await;
    observer.shutdown().await;
    result??;
    Ok(())
}

async fn original_late_cas_case() -> Result<()> {
    // Keep the original database and identities even if the mutation has an
    // unknown outcome. The test never reissues that mutation.
    let directory = tempfile::Builder::new()
        .prefix("cellar-lease-stamp-")
        .tempdir()?
        .keep();
    let core = Executor::new("lease-stamp-race")?;
    let bucket = Bucket::open_dev(&directory.join("fleet.sqlite"))?;
    let mut owner_bucket = bucket.clone();
    let race = Arc::new(StampRace {
        store: bucket.cas_store.clone(),
        key: format!("{}nodes/stamp-owner.json", bucket.prefix),
        armed: AtomicBool::new(false),
        attempts: AtomicUsize::new(0),
        entered: Semaphore::new(0),
        release: Arc::new(Semaphore::new(0)),
        committed: Arc::new(Semaphore::new(0)),
        first_committed: Arc::new(AtomicBool::new(false)),
        discovery: Arc::new(Semaphore::new(0)),
        first_without_log: AtomicBool::new(false),
        next_with_log: AtomicBool::new(false),
        rejected: AtomicBool::new(false),
        physical: Mutex::new(None),
        observations: Mutex::new(Vec::new()),
        gated_scans: AtomicUsize::new(0),
        unarmed_applied: AtomicUsize::new(0),
    });
    owner_bucket.store = race.clone();
    owner_bucket.cas_store = race.clone();
    let owner = start(
        "stamp-owner",
        "stamp",
        &directory.join("owner"),
        &core,
        Some(owner_bucket),
        None,
        0,
    )
    .await?;
    let mut follower = None;
    let result = async {
        // Prepare the follower while recruitment remains behind its gate.
        // Ordinary owner renewals continue; startup time does not consume the
        // particular held renewal's remaining authority window.
        follower = Some(
            start(
                "stamp-follower",
                "follower",
                &directory.join("follower"),
                &core,
                Some(bucket.clone()),
                None,
                0,
            )
            .await?,
        );
        ready(follower.as_ref().unwrap()).await?;
        race.armed.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(3), race.entered.acquire())
            .await
            .context("old renewal was not captured")??
            .forget();
        // The captured body has no ensemble. This original recruitment scan
        // now discovers the ready follower and publishes Open during the CAS.
        race.discovery.add_permits(1);
        ready(&owner).await?;
        let client = client()?;
        let write_result = call(&client, &owner, reqwest::Method::POST, "/write").await;
        ensure!(
            race.first_without_log.load(Ordering::Acquire),
            "old CAS had a log"
        );
        ensure!(
            race.next_with_log.load(Ordering::Acquire),
            "next CAS did not publish Open"
        );
        ensure!(
            race.rejected.load(Ordering::Acquire),
            "late CAS did not win the original guard"
        );
        ensure!(
            race.first_committed.load(Ordering::Acquire),
            "original physical CAS failed"
        );
        ensure!(write_result? == "stamp:1");
        // Cross the entire original TTL: adoption alone cannot keep this
        // process live unless its subsequent ordinary renewals also work.
        tokio::time::sleep(Duration::from_millis(1_050)).await;
        ensure!(!owner.is_closed(), "owner discarded its provable late CAS");
        ensure!(call(&client, &owner, reqwest::Method::GET, "/read").await? == "stamp:1");
        let (body, _) = bucket
            .get("nodes/stamp-owner.json")
            .await?
            .context("owner lease")?;
        let wire: serde_json::Value = serde_json::from_slice(&body)?;
        ensure!(wire["generation"] == owner.info().lease_generation);
        ensure!(wire["log"]["state"] == "open");
        ensure!(wire["log"]["may_have_fleet_acks"] == true);
        ensure!(wire["log"]["ensemble"]
            .as_array()
            .is_some_and(|members| !members.is_empty()));
        Ok::<_, anyhow::Error>(())
    }
    .await;
    eprintln!(
        "lease_stamp_observation={}",
        serde_json::json!({
            "first_without_log": race.first_without_log.load(Ordering::Acquire),
            "next_with_log": race.next_with_log.load(Ordering::Acquire),
            "rejected": race.rejected.load(Ordering::Acquire),
        "first_committed": race.first_committed.load(Ordering::Acquire),
            "owner_closed": owner.is_closed(),
            "attempts": race.attempts.load(Ordering::Acquire),
            "gated_scans": race.gated_scans.load(Ordering::Acquire),
        "unarmed_applied": race.unarmed_applied.load(Ordering::Acquire),
            "stamps": *race.observations.lock().unwrap(),
        })
    );
    // Join the native write and both nodes even on the first failed oracle.
    race.release.add_permits(1);
    race.discovery.add_permits(1);
    let physical = { race.physical.lock().unwrap().take() };
    let joined = match physical {
        Some(task) => task.await.context("join original physical CAS"),
        None => Ok(()),
    };
    let owner_stopped = stopped(owner).await;
    let follower_stopped = match follower {
        Some(node) => stopped(node).await,
        None => Ok(()),
    };
    result?;
    joined?;
    owner_stopped?;
    follower_stopped?;
    ensure!(core.handle().spawn(async { 7 }).await? == 7);
    Ok(())
}
