// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Permanent admission barriers outside the SQLite/replica subtree being erased.
use anyhow::{ensure, Context as _, Result};
use futures_util::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutMode, PutOptions};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Evidence {
    Parent {
        scope: String,
        incarnation: String,
    },
    Child {
        scope: String,
        identity_digest: String,
    },
}

impl Evidence {
    pub(crate) fn scope(&self) -> &str {
        match self {
            Self::Parent { scope, .. } | Self::Child { scope, .. } => scope,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::Parent { incarnation, .. } => ensure!(
                crate::application::sandbox_catalog::values::hex(incarnation, 32),
                "invalid parent retirement identity"
            ),
            Self::Child {
                identity_digest, ..
            } => ensure!(
                crate::application::sandbox_catalog::values::hex(identity_digest, 64),
                "invalid child retirement identity"
            ),
        }
        ensure!(
            cellar_logic::cell::valid_cell_scope(self.scope()) && self.scope().contains(':'),
            "invalid erased cell scope"
        );
        Ok(())
    }

    pub(crate) fn child(identity: &crate::runtime::sandbox_execution::Identity) -> Result<Self> {
        identity.validate()?;
        Ok(Self::Child {
            scope: identity.execution_scope.clone(),
            identity_digest: hex::encode(Sha256::digest(serde_json::to_vec(identity)?)),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Record {
    format: u32,
    pub evidence: Evidence,
}

impl Record {
    pub(crate) fn new(evidence: Evidence) -> Result<Self> {
        evidence.validate()?;
        Ok(Self {
            format: 1,
            evidence,
        })
    }

    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(self.format == 1, "unsupported cell retirement format");
        self.evidence.validate()
    }
}

pub(crate) fn key(prefix: &str, scope: &str) -> Path {
    Path::from(format!("{prefix}cell-retirement/records/{scope}.json"))
}

pub(crate) async fn read(
    store: &dyn ObjectStore,
    prefix: &str,
    scope: &str,
) -> Result<Option<Record>> {
    // Facet recovery and historical-log reclamation use the root's barrier.
    let root = scope.split('/').next().context("missing retirement root")?;
    match store.get(&key(prefix, root)).await {
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
        Ok(response) => {
            ensure!(
                response.meta.size <= 16 * 1024,
                "retirement record exceeds limit"
            );
            let record: Record = serde_json::from_slice(&response.bytes().await?)?;
            record.validate()?;
            ensure!(
                record.evidence.scope() == root,
                "retirement record belongs to another cell"
            );
            Ok(Some(record))
        }
    }
}

/// Fleet barriers share the caller's replica store. Local nodes retain a
/// separate metadata database; removing a cell never removes these records.
pub(crate) struct Store {
    store: tokio::sync::OnceCell<Arc<dyn ObjectStore>>,
    prefix: String,
    local: PathBuf,
    cursor: std::sync::Mutex<[Option<Path>; 2]>,
    audit: std::sync::atomic::AtomicBool,
}

impl Store {
    pub(crate) fn new(
        remote: Option<(Arc<dyn ObjectStore>, String)>,
        data: &std::path::Path,
    ) -> Self {
        let (store, prefix) = remote.map_or_else(
            || (tokio::sync::OnceCell::new(), String::new()),
            |(store, prefix)| (tokio::sync::OnceCell::new_with(Some(store)), prefix),
        );
        Self {
            store,
            prefix,
            local: data.join("cell-retirement.sqlite"),
            cursor: Default::default(),
            audit: Default::default(),
        }
    }

    async fn backend(&self) -> Result<&Arc<dyn ObjectStore>> {
        self.store
            .get_or_try_init(|| async {
                let path = self.local.clone();
                crate::asyncrt::try_blocking(move || {
                    crate::bucket::Bucket::open_dev(&path).map(|bucket| bucket.store)
                })?
                .await?
            })
            .await
    }

    pub(crate) async fn get(&self, scope: &str) -> Result<Option<Record>> {
        read(self.backend().await?.as_ref(), &self.prefix, scope).await
    }

    fn pending_key(&self, scope: &str) -> Path {
        Path::from(format!(
            "{}cell-retirement/pending/{scope}.json",
            self.prefix
        ))
    }

    fn completed_key(&self, scope: &str) -> Path {
        Path::from(format!(
            "{}cell-retirement/erased/{scope}.json",
            self.prefix
        ))
    }

    async fn immutable(&self, key: &Path, record: &Record) -> Result<()> {
        record.validate()?;
        let bytes = serde_json::to_vec(record)?;
        ensure!(bytes.len() <= 16 * 1024, "retirement record exceeds limit");
        let store = self.backend().await?;
        let result = store
            .put_opts(
                key,
                bytes.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await;
        // Read back even a lost response. Only byte-equivalent immutable
        // evidence permits continuation; no error is interpreted as success.
        let response = store.get(key).await?;
        ensure!(
            response.meta.size <= 16 * 1024,
            "retirement record exceeds limit"
        );
        let current = response.bytes().await?;
        let current: Record = serde_json::from_slice(&current)?;
        ensure!(&current == record, "cell retirement identity changed");
        let _ = result;
        Ok(())
    }

    pub(crate) async fn begin(&self, record: &Record) -> Result<()> {
        let scope = record.evidence.scope();
        // Discovery comes first. A crash before the admission barrier is
        // published leaves the full validated request available to finish it.
        self.immutable(&self.pending_key(scope), record).await?;
        self.immutable(&key(&self.prefix, scope), record).await
    }

    pub(crate) async fn next(&self) -> Result<Option<Record>> {
        // Alternate pending publication recovery and permanent-barrier audits.
        // Audits reclaim delayed object-store writes and caches on returning nodes.
        let audit = self
            .audit
            .fetch_xor(true, std::sync::atomic::Ordering::Relaxed);
        let index = usize::from(audit);
        let directory = if audit { "records" } else { "pending" };
        let prefix = Path::from(format!("{}cell-retirement/{directory}/", self.prefix));
        let cursor = self.cursor.lock().unwrap()[index].clone();
        let mut listed = match cursor {
            Some(cursor) => self
                .backend()
                .await?
                .list_with_offset(Some(&prefix), &cursor),
            None => self.backend().await?.list(Some(&prefix)),
        };
        let mut first = None;
        while let Some(entry) = listed.try_next().await? {
            if first
                .as_ref()
                .is_none_or(|prior: &object_store::ObjectMeta| entry.location < prior.location)
            {
                first = Some(entry);
            }
        }
        let Some(entry) = first else {
            *self.cursor.lock().unwrap().get_mut(index).unwrap() = None;
            return Ok(None);
        };
        self.cursor.lock().unwrap()[index] = Some(entry.location.clone());
        ensure!(entry.size <= 16 * 1024, "retirement request exceeds limit");
        let record: Record = serde_json::from_slice(
            &self
                .backend()
                .await?
                .get(&entry.location)
                .await?
                .bytes()
                .await?,
        )?;
        record.validate()?;
        let expected = if audit {
            key(&self.prefix, record.evidence.scope())
        } else {
            self.pending_key(record.evidence.scope())
        };
        ensure!(entry.location == expected, "retirement request key differs");
        if !audit {
            self.immutable(&key(&self.prefix, record.evidence.scope()), &record)
                .await?;
        }
        Ok(Some(record))
    }

    pub(crate) async fn complete(&self, record: &Record) -> Result<()> {
        self.immutable(&self.completed_key(record.evidence.scope()), record)
            .await?;
        match self
            .backend()
            .await?
            .delete(&self.pending_key(record.evidence.scope()))
            .await
        {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    pub(crate) async fn status(&self, scope: &str) -> Result<Option<&'static str>> {
        let Some(record) = self.get(scope).await? else {
            return Ok(None);
        };
        match self.backend().await?.get(&self.completed_key(scope)).await {
            Ok(response) => {
                ensure!(
                    response.meta.size <= 16 * 1024,
                    "erasure receipt exceeds limit"
                );
                ensure!(
                    serde_json::from_slice::<Record>(&response.bytes().await?)? == record,
                    "erasure receipt differs from its barrier"
                );
                Ok(Some("erased"))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Some("pending")),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
#[path = "retirement_tests.rs"]
mod tests;
