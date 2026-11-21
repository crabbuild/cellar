// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Conditional control metadata only; these fixtures grant no archive coverage.
use super::*;
use crate::asyncrt::ExecutionDomain;
use crate::raft::catalog::{Catalog, NamespaceConfig};
use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId};
use futures_util::{stream::BoxStream, TryStreamExt as _};
use object_store::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const SLOT: &str = "transition-source-progress/unit-acquire/0";

#[derive(Default)]
struct Store {
    inner: object_store::memory::InMemory,
    registry_puts: AtomicUsize,
    slot_puts: AtomicUsize,
    defer_registry: AtomicBool,
    lose_slot: AtomicBool,
    deny_registry_read: AtomicBool,
    deny_slot_read: AtomicBool,
    deferred: Mutex<Option<(Path, PutPayload, PutOptions)>>,
    supersede: Mutex<Option<(CellGroupArchiveRegistry, crate::asyncrt::DomainToken)>>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("source-acquire-test")
    }
}

fn lost() -> object_store::Error {
    object_store::Error::Generic {
        store: "source-acquire-test",
        source: std::io::Error::from(std::io::ErrorKind::ConnectionReset).into(),
    }
}

impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("source-acquire-test")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Store {
    async fn put_opts(
        &self,
        key: &Path,
        body: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if key.as_ref().ends_with("/registry") {
            self.registry_puts.fetch_add(1, Ordering::AcqRel);
            if self.defer_registry.swap(false, Ordering::AcqRel) {
                *self.deferred.lock().unwrap() = Some((key.clone(), body, options));
                self.deny_registry_read.store(true, Ordering::Release);
                return Err(lost());
            }
        }
        let result = self.inner.put_opts(key, body, options).await?;
        if key.as_ref().contains("transition-source-progress/") {
            self.slot_puts.fetch_add(1, Ordering::AcqRel);
            let supersede = self.supersede.lock().unwrap().take();
            if let Some((registry, domain)) = supersede {
                // Consume the successor revision through a real independent
                // allocation before its first delivery, rather than editing
                // the registry's state or simulating a CAS result.
                async {
                    let intent = registry.prepare_upload(&domain).await?;
                    registry.reserve(&domain, &intent).await?;
                    Ok::<_, CellGroupArchiveError>(())
                }
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "source-acquire-test",
                    source: Box::new(error),
                })?;
            }
            if self.lose_slot.swap(false, Ordering::AcqRel) {
                self.deny_slot_read.store(true, Ordering::Release);
                return Err(lost());
            }
        }
        Ok(result)
    }

    async fn get_opts(&self, key: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        if (key.as_ref().ends_with("/registry")
            && self.deny_registry_read.swap(false, Ordering::AcqRel))
            || (key.as_ref().contains("transition-source-progress/")
                && self.deny_slot_read.swap(false, Ordering::AcqRel))
        {
            return Err(lost());
        }
        self.inner.get_opts(key, options).await
    }

    async fn put_multipart_opts(
        &self,
        key: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(key, options).await
    }

    async fn delete(&self, key: &Path) -> object_store::Result<()> {
        self.inner.delete(key).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

struct Fixture {
    domain: ExecutionDomain,
    directory: RangeDirectory,
    registry: CellGroupArchiveRegistry,
    store: Arc<Store>,
    registry_key: Path,
    root: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> anyhow::Result<Self> {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let store = Arc::new(Store::default());
        let (directory, registry, registry_key) = domain
            .run(async {
                let catalog = Catalog::new(store.clone(), Default::default())?;
                let namespace = catalog
                    .open_namespace(NamespaceConfig::new(
                        ClusterId::new([1; 16])?,
                        NamespaceId::new([2; 16])?,
                        cellar_logic::NamespaceMode::Raft,
                        [6; 32],
                    ))
                    .await?;
                let directory = RangeDirectory::new(namespace, Default::default())?;
                let route = GroupRoute {
                    history: GroupHistory {
                        cluster: ClusterId::new([1; 16])?,
                        namespace: NamespaceId::new([2; 16])?,
                        group: GroupId::new([3; 16])?,
                        incarnation: IncarnationId::new([4; 16])?,
                    },
                    range: CellRange::all(),
                    epoch: RouteEpoch::new(1)?,
                    birth_digest: [5; 32],
                    schema_digest: [6; 32],
                };
                let archive = CellGroupArchivePool::new(Default::default())?.for_group(
                    store.clone(),
                    Path::from("acquire-control"),
                    route.history,
                )?;
                let registry = archive
                    .prepare_registry_creation(&domain, route.clone())
                    .await?
                    .commit(&domain)
                    .await?;
                let identity = serde_json::to_value(registry.creation())?;
                let registry_key = store
                    .inner
                    .list(None)
                    .try_collect::<Vec<_>>()
                    .await?
                    .into_iter()
                    .find(|entry| entry.location.as_ref().ends_with("/registry"))
                    .ok_or_else(|| anyhow::anyhow!("registry fixture key"))?
                    .location;
                // The helper allocates references, not physical coverage. Seed
                // the same checked control-only descriptor as ownership tests.
                let state = serde_json::json!({
                    "domain": "cellar-cell-group-archive-ownership-1",
                    "identity": identity,
                    "revision": 3,
                    "pending": [], "holds": [],
                    "selected": {
                        "object": {"registry": identity["id"], "generation": 2,
                            "digest": vec![8u8; 32], "length": 128},
                        "marker": GroupManifest::genesis(route)?,
                    },
                });
                let metadata = store.inner.head(&registry_key).await?;
                store
                    .inner
                    .put_opts(
                        &registry_key,
                        serde_json::to_vec(&state)?.into(),
                        PutOptions::from(PutMode::Update(UpdateVersion {
                            e_tag: metadata.e_tag,
                            version: metadata.version,
                        })),
                    )
                    .await?;
                // Loading through the actual registry validates the fixture.
                registry.prepare_acquire(&domain).await?;
                Ok::<_, anyhow::Error>((directory, registry, registry_key))
            })
            .await??;
        let mut root = tempfile::Builder::new()
            .prefix("cellar-acquire-control-")
            .tempdir()?;
        root.disable_cleanup(true);
        eprintln!("retained acquire control: {}", root.path().display());
        Ok(Self {
            domain,
            directory,
            registry,
            store,
            registry_key,
            root,
        })
    }

    async fn stage(&self) -> anyhow::Result<(CellGroupArchiveIntent, UpdateVersion)> {
        let intent = self.registry.prepare_acquire(&self.domain).await?;
        let version = self
            .directory
            .write_archive_slot(SLOT, None, &ArchiveSlot::Acquiring(intent.clone()))
            .await?;
        Ok((intent, version))
    }

    async fn consume_revision(&self) -> anyhow::Result<()> {
        let intent = self.registry.prepare_upload(&self.domain).await?;
        self.registry.reserve(&self.domain, &intent).await?;
        Ok(())
    }

    async fn selected(&self) -> anyhow::Result<(CellGroupArchiveIntent, UpdateVersion)> {
        match self.directory.read_archive_slot(SLOT).await? {
            Some((ArchiveSlot::Acquiring(intent), version)) => Ok((intent, version)),
            _ => anyhow::bail!("expected operation-owned acquiring slot"),
        }
    }

    async fn holds(&self) -> anyhow::Result<usize> {
        let bytes = self
            .store
            .inner
            .get(&self.registry_key)
            .await?
            .bytes()
            .await?;
        let state: serde_json::Value = serde_json::from_slice(&bytes)?;
        Ok(state["holds"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("holds"))?
            .len())
    }

    async fn finish(self, result: anyhow::Result<()>) -> anyhow::Result<()> {
        drop(self.store.supersede.lock().unwrap().take());
        let retained = self
            .domain
            .run(async {
                let mut objects = self.store.inner.list(None).try_collect::<Vec<_>>().await?;
                objects.sort_by(|a, b| a.location.cmp(&b.location));
                let mut snapshot = Vec::new();
                for object in objects {
                    let bytes = self
                        .store
                        .inner
                        .get(&object.location)
                        .await?
                        .bytes()
                        .await?;
                    snapshot.push((object.location.to_string(), bytes.to_vec()));
                }
                let root = self.root.path().to_path_buf();
                self.domain
                    .blocking(move || -> anyhow::Result<()> {
                        let filesystem = crate::asyncrt::fs();
                        let mut manifest = Vec::new();
                        for (index, (key, bytes)) in snapshot.into_iter().enumerate() {
                            let file = format!("{index:06}.bin");
                            filesystem.write(&root.join(&file), &bytes)?;
                            manifest.push(serde_json::json!({"object":key,"file":file}));
                        }
                        filesystem.write(
                            &root.join("manifest.json"),
                            &serde_json::to_vec_pretty(&manifest)?,
                        )?;
                        Ok(())
                    })?
                    .await?
            })
            .await;
        self.domain.shutdown().await;
        result?;
        retained??;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn superseded_source_acquire_finishes_confirmed_replacement() -> anyhow::Result<()> {
    let f = Fixture::new().await?;
    let result = f
        .domain
        .run(async {
            let staged = f.stage().await?;
            let original = staged.0.clone();
            f.consume_revision().await?;
            anyhow::ensure!(matches!(
                f.registry.acquire_outcome(&f.domain, &original).await?,
                CellGroupArchiveAcquireOutcome::Superseded
            ));
            let (hold, replacement) = f
                .directory
                .acquire_transition_archive_hold(&f.domain, &f.registry, SLOT, Some(staged), |_| {
                    Ok(())
                })
                .await?;
            anyhow::ensure!(replacement != original && f.holds().await? == 1);
            let selected = f.selected().await?;
            anyhow::ensure!(selected.0 == replacement);
            let (again, same) = f
                .directory
                .acquire_transition_archive_hold(
                    &f.domain,
                    &f.registry,
                    SLOT,
                    Some(selected),
                    |_| Ok(()),
                )
                .await?;
            anyhow::ensure!(again == hold && same == replacement && f.holds().await? == 1);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    f.finish(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_source_acquire_keeps_original_after_registry_advances() -> anyhow::Result<()> {
    let f = Fixture::new().await?;
    let result = f
        .domain
        .run(async {
            let staged = f.stage().await?;
            let original = staged.0.clone();
            let held = f.registry.acquire(&f.domain, &original).await?;
            f.consume_revision().await?;
            let (hold, same) = f
                .directory
                .acquire_transition_archive_hold(&f.domain, &f.registry, SLOT, Some(staged), |_| {
                    Ok(())
                })
                .await?;
            anyhow::ensure!(hold == held && same == original && f.holds().await? == 1);
            anyhow::ensure!(f.selected().await?.0 == original);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    f.finish(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_source_acquire_retains_exact_late_allocation() -> anyhow::Result<()> {
    let f = Fixture::new().await?;
    let result = f
        .domain
        .run(async {
            let staged = f.stage().await?;
            let original = staged.0.clone();
            let puts = f.store.registry_puts.load(Ordering::Acquire);
            f.store.defer_registry.store(true, Ordering::Release);
            let unknown = f
                .directory
                .acquire_transition_archive_hold(&f.domain, &f.registry, SLOT, Some(staged), |_| {
                    Ok(())
                })
                .await;
            anyhow::ensure!(matches!(
                unknown,
                Err(CellGroupArchiveBindingError::Archive(
                    CellGroupArchiveError::Unconfirmed
                ))
            ));
            anyhow::ensure!(f.selected().await?.0 == original && f.holds().await? == 0);
            let (key, body, options) = f
                .store
                .deferred
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| anyhow::anyhow!("original physical allocation"))?;
            f.store.inner.put_opts(&key, body, options).await?;
            let (held, same) = f
                .directory
                .acquire_transition_archive_hold(
                    &f.domain,
                    &f.registry,
                    SLOT,
                    Some(f.selected().await?),
                    |_| Ok(()),
                )
                .await?;
            anyhow::ensure!(same == original && f.holds().await? == 1);
            anyhow::ensure!(
                matches!(f.registry.acquire_outcome(&f.domain, &original).await?,
            CellGroupArchiveAcquireOutcome::Held(observed) if *observed == held)
            );
            anyhow::ensure!(f.store.registry_puts.load(Ordering::Acquire) == puts + 1);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    f.finish(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unconfirmed_successor_slot_refuses_physical_allocation() -> anyhow::Result<()> {
    let f = Fixture::new().await?;
    let result = f
        .domain
        .run(async {
            let staged = f.stage().await?;
            let original = staged.0.clone();
            f.consume_revision().await?;
            let puts = f.store.registry_puts.load(Ordering::Acquire);
            f.store.lose_slot.store(true, Ordering::Release);
            let unknown = f
                .directory
                .acquire_transition_archive_hold(&f.domain, &f.registry, SLOT, Some(staged), |_| {
                    Ok(())
                })
                .await;
            anyhow::ensure!(matches!(
                unknown,
                Err(CellGroupArchiveBindingError::Catalog(
                    CatalogError::Storage(_)
                ))
            ));
            anyhow::ensure!(f.store.registry_puts.load(Ordering::Acquire) == puts);
            anyhow::ensure!(f.holds().await? == 0);
            let selected = f.selected().await?;
            anyhow::ensure!(selected.0 != original);
            let replacement = selected.0.clone();
            let (_, same) = f
                .directory
                .acquire_transition_archive_hold(
                    &f.domain,
                    &f.registry,
                    SLOT,
                    Some(selected),
                    |_| Ok(()),
                )
                .await?;
            anyhow::ensure!(same == replacement && f.holds().await? == 1);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    f.finish(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_supersession_stops_after_one_confirmed_successor() -> anyhow::Result<()> {
    let f = Fixture::new().await?;
    let result = f
        .domain
        .run(async {
            let staged = f.stage().await?;
            let original = staged.0.clone();
            f.consume_revision().await?;
            let writes = f.store.slot_puts.load(Ordering::Acquire);
            *f.store.supersede.lock().unwrap() = Some((
                f.registry.clone(),
                f.domain
                    .services()
                    .domain_token()
                    .ok_or_else(|| anyhow::anyhow!("fixture domain closed"))?,
            ));
            let unknown = f
                .directory
                .acquire_transition_archive_hold(&f.domain, &f.registry, SLOT, Some(staged), |_| {
                    Ok(())
                })
                .await;
            anyhow::ensure!(matches!(
                unknown,
                Err(CellGroupArchiveBindingError::Catalog(
                    CatalogError::Unconfirmed
                ))
            ));
            anyhow::ensure!(f.store.supersede.lock().unwrap().is_none());
            anyhow::ensure!(f.store.slot_puts.load(Ordering::Acquire) == writes + 1);
            let selected = f.selected().await?.0;
            anyhow::ensure!(selected != original && f.holds().await? == 0);
            anyhow::ensure!(matches!(
                f.registry.acquire_outcome(&f.domain, &selected).await?,
                CellGroupArchiveAcquireOutcome::Superseded
            ));
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    f.finish(result).await
}
