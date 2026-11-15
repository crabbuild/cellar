// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete external-byte checks before a replayed image can become coverage.

use super::*;
use crate::storage::raft_payloads::{self, Dependency};
use futures_util::TryStreamExt;
use object_store::{PutMode, UpdateVersion};
use std::{collections::BTreeMap, sync::atomic::AtomicBool, time::Duration};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Namespace {
    cluster: [u8; 16],
    namespace: [u8; 16],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Namespaces {
    format: u8,
    registry: [u8; 32],
    namespaces: std::collections::BTreeSet<Namespace>,
}

pub(super) struct VerifiedDependency {
    pub dependency: Dependency,
    pub local: bool,
}

type PayloadIdentity = (
    [u8; 16],
    [u8; 16],
    String,
    super::super::CellGroupPayloadReference,
);

fn payload_identity(dependency: &Dependency) -> PayloadIdentity {
    (
        dependency.history.cluster.bytes(),
        dependency.history.namespace.bytes(),
        dependency.history.cell.as_str().to_owned(),
        dependency.reference.clone(),
    )
}

impl CellGroupArchive {
    pub(super) fn payload_store(
        &self,
        history: &cellar_logic::cell_group::CellHistory,
    ) -> Result<super::super::CellGroupPayloadStore> {
        self.payload_namespace(history.cluster, history.namespace)
    }

    fn payload_namespace(
        &self,
        cluster: cellar_logic::consensus::ClusterId,
        namespace: cellar_logic::consensus::NamespaceId,
    ) -> Result<super::super::CellGroupPayloadStore> {
        let payloads = self
            .payloads
            .as_ref()
            .ok_or(CellGroupArchiveError::Invalid(
                "external payload admission is not configured",
            ))?;
        payloads
            .resolve(cluster, namespace)
            .map_err(CellGroupArchiveError::Payload)
    }

    pub(super) async fn verify_payloads(
        &self,
        domain: &impl CellGroupArchiveExecution,
        recovered: &mut VerifiedCellGroupArchiveRecovery,
        max_metadata_bytes: usize,
    ) -> Result<()> {
        let store = recovered.checkpoint().checkpoint().store().clone();
        let manifest = recovered.manifest().clone();
        let deadline = crate::asyncrt::mono_ms()
            .checked_add(self.limits.io_timeout_ms)
            .ok_or(CellGroupArchiveError::Timeout)?;
        let dependencies = domain
            .blocking(move || -> Result<Vec<VerifiedDependency>> {
                let mut dependencies: BTreeMap<Vec<u8>, VerifiedDependency> = BTreeMap::new();
                manifest
                    .images
                    .visit(&mut |_, leaf, pin, _, _| -> Result<()> {
                        if matches!(pin, super::super::CellImagePin::Empty) {
                            return Ok(());
                        }
                        let database = receipt::open(
                            &store,
                            pin.clone(),
                            deadline,
                            Arc::new(AtomicBool::new(false)),
                        )?;
                        raft_payloads::visit(
                            database.connection(),
                            &leaf.version.history,
                            &mut |dependency| {
                                let key = serde_json::to_vec(&dependency)?;
                                let local = dependency.history.cluster
                                    == leaf.version.history.cluster
                                    && dependency.history.namespace
                                        == leaf.version.history.namespace
                                    && dependency.history.cell == leaf.version.history.cell;
                                if let Some(prior) = dependencies.get_mut(&key) {
                                    prior.dependency.confirmed |= dependency.confirmed;
                                    prior.local |= local;
                                } else {
                                    if dependencies.len() == raft_payloads::MAX_DEPENDENCIES
                                        || dependencies
                                            .len()
                                            .checked_add(1)
                                            .and_then(|n| n.checked_mul(4096))
                                            .is_none_or(|n| n > max_metadata_bytes)
                                    {
                                        anyhow::bail!("payload dependency metadata bound");
                                    }
                                    dependencies
                                        .insert(key, VerifiedDependency { dependency, local });
                                }
                                Ok(())
                            },
                        )
                        .map_err(|error| {
                            CellGroupArchiveError::Replay(crate::raft::execution::storage(error))
                        })?;
                        Ok(())
                    })?;
                Ok(dependencies.into_values().collect())
            })
            .map_err(|_| CellGroupArchiveError::Stopped)?
            .await
            .map_err(CellGroupArchiveError::Worker)??;
        // One original verification deadline covers the whole dependency set;
        // each body has the same length/digest/range/EOF and pool checks as SDK
        // reads. No cached descriptor or HEAD substitutes for actual bytes.
        let mut checked = std::collections::BTreeSet::new();
        for verified in &dependencies {
            let dependency = &verified.dependency;
            if !dependency.confirmed || !checked.insert(payload_identity(dependency)) {
                continue;
            }
            let remaining = deadline
                .checked_sub(crate::asyncrt::mono_ms())
                .filter(|n| *n != 0)
                .ok_or(CellGroupArchiveError::Timeout)?;
            let payloads = self.payload_store(&dependency.history)?;
            let bytes = payloads
                .get(
                    &dependency.history,
                    &dependency.reference,
                    Duration::from_millis(remaining),
                )
                .await
                .map_err(CellGroupArchiveError::Payload)?;
            drop(bytes);
        }
        recovered.payloads = dependencies;
        recovered.payload_archive = Some(self.clone());
        Ok(())
    }
}

impl VerifiedCellGroupArchiveRecovery {
    pub(in crate::raft::cell_groups) async fn verify_external_now(&self) -> Result<()> {
        let archive = self
            .payload_archive
            .as_ref()
            .ok_or(CellGroupArchiveError::Invalid(
                "missing verified payload binding",
            ))?;
        let deadline = crate::asyncrt::mono_ms()
            .checked_add(archive.limits.io_timeout_ms)
            .ok_or(CellGroupArchiveError::Timeout)?;
        let mut checked = std::collections::BTreeSet::new();
        for verified in &self.payloads {
            let dependency = &verified.dependency;
            if !dependency.confirmed || !checked.insert(payload_identity(dependency)) {
                continue;
            }
            let remaining = deadline
                .checked_sub(crate::asyncrt::mono_ms())
                .filter(|n| *n != 0)
                .ok_or(CellGroupArchiveError::Timeout)?;
            let store = archive.payload_store(&dependency.history)?;
            store
                .get(
                    &dependency.history,
                    &dependency.reference,
                    Duration::from_millis(remaining),
                )
                .await
                .map_err(CellGroupArchiveError::Payload)?;
        }
        Ok(())
    }
}

impl CellGroupArchiveRegistry {
    fn payload_index_path(&self) -> Path {
        self.archive
            .prefix
            .child("ownership")
            .child("payload-namespaces")
    }

    async fn payload_namespaces(&self) -> Result<Option<(Namespaces, UpdateVersion)>> {
        self.archive
            .io(async {
                let response = match self.archive.store.get(&self.payload_index_path()).await {
                    Ok(response) => response,
                    Err(object_store::Error::NotFound { .. }) => return Ok(None),
                    Err(error) => return Err(CellGroupArchiveError::Storage(error)),
                };
                let length = response.meta.size;
                if length > 64 * 1024 || response.range.start != 0 || response.range.end != length {
                    return Err(CellGroupArchiveError::Invalid(
                        "payload namespace index response",
                    ));
                }
                let version = UpdateVersion {
                    e_tag: response.meta.e_tag.clone(),
                    version: response.meta.version.clone(),
                };
                if version.e_tag.as_ref().is_some_and(String::is_empty)
                    || version.version.as_ref().is_some_and(String::is_empty)
                    || (version.e_tag.is_none() && version.version.is_none())
                    || version
                        .e_tag
                        .as_ref()
                        .map_or(0, String::len)
                        .saturating_add(version.version.as_ref().map_or(0, String::len))
                        > 8192
                {
                    return Err(CellGroupArchiveError::Invalid(
                        "payload namespace index version",
                    ));
                }
                let mut bytes = Vec::with_capacity(length as usize);
                let mut stream = response.into_stream();
                let mut chunks = 0;
                while let Some(chunk) = stream
                    .try_next()
                    .await
                    .map_err(CellGroupArchiveError::Storage)?
                {
                    chunks += 1;
                    if chunks > 16384
                        || bytes
                            .len()
                            .checked_add(chunk.len())
                            .is_none_or(|n| n > length as usize)
                    {
                        return Err(CellGroupArchiveError::Limit);
                    }
                    bytes.extend_from_slice(&chunk);
                }
                if bytes.len() as u64 != length {
                    return Err(CellGroupArchiveError::Invalid(
                        "payload namespace index EOF",
                    ));
                }
                let index: Namespaces = serde_json::from_slice(&bytes).map_err(|_| {
                    CellGroupArchiveError::Invalid("payload namespace index encoding")
                })?;
                if index.format != 1 || index.registry != self.id() || index.namespaces.len() > 256
                {
                    return Err(CellGroupArchiveError::Invalid(
                        "payload namespace index binding",
                    ));
                }
                for namespace in &index.namespaces {
                    cellar_logic::consensus::ClusterId::new(namespace.cluster)
                        .map_err(|_| CellGroupArchiveError::Invalid("payload namespace cluster"))?;
                    cellar_logic::consensus::NamespaceId::new(namespace.namespace).map_err(
                        |_| CellGroupArchiveError::Invalid("payload namespace identity"),
                    )?;
                }
                Ok(Some((index, version)))
            })
            .await
    }

    async fn track_payload_namespaces(&self, dependencies: &[VerifiedDependency]) -> Result<()> {
        if dependencies.is_empty() {
            return Ok(());
        }
        let requested: std::collections::BTreeSet<_> = dependencies
            .iter()
            .map(|item| Namespace {
                cluster: item.dependency.history.cluster.bytes(),
                namespace: item.dependency.history.namespace.bytes(),
            })
            .collect();
        for _ in 0..16 {
            let existing = self.payload_namespaces().await?;
            let (mut index, mode) = match existing {
                Some((index, version)) => (index, PutMode::Update(version)),
                None => (
                    Namespaces {
                        format: 1,
                        registry: self.id(),
                        namespaces: Default::default(),
                    },
                    PutMode::Create,
                ),
            };
            if requested.is_subset(&index.namespaces) {
                return Ok(());
            }
            index.namespaces.extend(requested.iter().cloned());
            if index.namespaces.len() > 256 {
                return Err(CellGroupArchiveError::Limit);
            }
            let bytes = serde_json::to_vec(&index).map_err(|_| CellGroupArchiveError::Limit)?;
            if bytes.len() > 64 * 1024 {
                return Err(CellGroupArchiveError::Limit);
            }
            let outcome = self
                .archive
                .io(async {
                    self.archive
                        .store
                        .put_opts(&self.payload_index_path(), bytes.into(), mode.into())
                        .await
                        .map(|_| ())
                        .map_err(CellGroupArchiveError::Storage)
                })
                .await;
            if self
                .payload_namespaces()
                .await?
                .is_some_and(|(observed, _)| requested.is_subset(&observed.namespaces))
            {
                return Ok(());
            }
            if !matches!(
                outcome,
                Err(CellGroupArchiveError::Storage(
                    object_store::Error::Precondition { .. }
                        | object_store::Error::AlreadyExists { .. }
                ))
            ) {
                return Err(CellGroupArchiveError::Unconfirmed);
            }
        }
        Err(CellGroupArchiveError::Conflict)
    }

    pub(super) async fn pin_payloads(
        &self,
        verified: &VerifiedCellGroupArchiveRecovery,
        descriptor: &super::ownership::Descriptor,
        adopt: bool,
    ) -> Result<()> {
        self.track_payload_namespaces(&verified.payloads).await?;
        for item in &verified.payloads {
            let dependency = &item.dependency;
            let store = self.archive.payload_store(&dependency.history)?;
            let owner = super::super::payload::PayloadRootOwner {
                scope: self.id(),
                identity: descriptor.object.digest,
                generation: descriptor.object.generation,
                confirmed: dependency.confirmed,
            };
            match store
                .pin_archive(
                    &dependency.history,
                    &dependency.reference,
                    owner,
                    adopt && item.local && dependency.confirmed,
                    Duration::from_millis(self.archive.limits.io_timeout_ms),
                )
                .await
            {
                Ok(()) => {
                    if adopt {
                        store
                            .adopt_command(
                                &dependency.reference,
                                dependency.guard,
                                Duration::from_millis(self.archive.limits.io_timeout_ms),
                            )
                            .await
                            .map_err(CellGroupArchiveError::Payload)?;
                    }
                }
                Err(
                    super::super::CellGroupPayloadError::Storage(object_store::Error::NotFound {
                        ..
                    })
                    | super::super::CellGroupPayloadError::Missing
                    | super::super::CellGroupPayloadError::Retired
                    | super::super::CellGroupPayloadError::Invalid(_),
                ) if !dependency.confirmed => (),
                Err(error) => return Err(CellGroupArchiveError::Payload(error)),
            }
        }
        Ok(())
    }

    /// Retire only this registry's unselected payload roots. The mark includes
    /// retained recovery cuts and pending parents; newer and pending generations
    /// cannot be swept by an older observation. Other registries' pins survive.
    pub async fn collect_payloads(
        &self,
        domain: &impl CellGroupArchiveExecution,
        limits: super::super::CellGroupPayloadCollectionLimits,
    ) -> Result<super::super::CellGroupPayloadCollectionReport> {
        if !(1..=65_536).contains(&limits.max_scan_objects)
            || limits.max_delete_objects > limits.max_scan_objects
        {
            return Err(CellGroupArchiveError::Limit);
        }
        domain
            .run(async {
                crate::asyncrt::timeout(
                    Duration::from_millis(self.archive.limits.io_timeout_ms),
                    async {
                        let _admission = self.archive.admit()?;
                        let Some((index, _)) = self.payload_namespaces().await? else {
                            return Ok(Default::default());
                        };
                        let protection = self.protection().await?;
                        let protected = protection
                            .roots
                            .iter()
                            .map(|root| (root.object.generation, root.object.digest))
                            .collect();
                        let pending = protection.pending_generations.into_iter().collect();
                        let mut report = super::super::CellGroupPayloadCollectionReport::default();
                        for namespace in index.namespaces {
                            let max_scan_objects = limits
                                .max_scan_objects
                                .checked_sub(report.scanned_objects)
                                .filter(|n| *n != 0)
                                .ok_or(CellGroupArchiveError::Limit)?;
                            let remaining = super::super::CellGroupPayloadCollectionLimits {
                                max_scan_objects,
                                max_delete_objects: limits
                                    .max_delete_objects
                                    .checked_sub(report.deleted_objects)
                                    .ok_or(CellGroupArchiveError::Limit)?
                                    .min(max_scan_objects),
                            };
                            let cluster =
                                cellar_logic::consensus::ClusterId::new(namespace.cluster)
                                    .map_err(|_| {
                                        CellGroupArchiveError::Invalid("payload namespace cluster")
                                    })?;
                            let namespace =
                                cellar_logic::consensus::NamespaceId::new(namespace.namespace)
                                    .map_err(|_| {
                                        CellGroupArchiveError::Invalid("payload namespace identity")
                                    })?;
                            let store = self.archive.payload_namespace(cluster, namespace)?;
                            let timeout = Duration::from_millis(self.archive.limits.io_timeout_ms);
                            store
                                .retire_archive_roots(
                                    self.id(),
                                    protection.revision,
                                    &pending,
                                    &protected,
                                    remaining.max_scan_objects,
                                    timeout,
                                )
                                .await
                                .map_err(CellGroupArchiveError::Payload)?;
                            let pass = store
                                .collect(remaining, timeout)
                                .await
                                .map_err(CellGroupArchiveError::Payload)?;
                            report.scanned_objects += pass.scanned_objects;
                            report.protected_objects += pass.protected_objects;
                            report.deleted_objects += pass.deleted_objects;
                            report.deleted_bytes = report
                                .deleted_bytes
                                .checked_add(pass.deleted_bytes)
                                .ok_or(CellGroupArchiveError::Limit)?;
                        }
                        Ok(report)
                    },
                )
                .await
                .map_err(|_| CellGroupArchiveError::Timeout)?
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }
}
