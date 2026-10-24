//! Bounded CAS coordination. These records never grant Raft membership or votes.
//! A retained mutation journal resolves ambiguous writes after later revisions.

mod index;
mod nodes;
mod permits;
mod records;

pub use nodes::{
    FleetHostAuthorization, FleetNodeEligibility, FleetNodeSpec, FleetNodeState,
    FleetRequestIdentity,
};
pub(crate) use nodes::{FleetTrustReader, FleetTrustSnapshot};
pub use permits::{FleetGroupAuthorization, FleetPermitAuthorization};

use records::{CommitRecord, Head, Pin};
pub use records::{FleetChange, FleetMutation, FleetTable};

use crate::asyncrt::{DomainToken, ExecutionDomain};
use crate::bucket::Bucket;
use cellar_logic::consensus::{ClusterId, IncarnationId};
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt, future::Future, sync::Arc, time::Duration};
use tokio::sync::Mutex;

const ROOT_LIMIT: usize = 1024 * 1024;
const REQUEST_LIMIT: usize = 256 * 1024;
const PAGE_LIMIT: usize = 64 * 1024;
const HEAD_KIND: u8 = 40;
const PAGE_KIND: u8 = 41;
const REQUEST_KIND: u8 = 42;
const PIN_KIND: u8 = 43;
const COMMIT_KIND: u8 = 44;

#[derive(Debug)]
pub enum FleetCatalogError {
    Invalid(&'static str),
    Corrupt(&'static str),
    Conflict,
    PayloadConflict,
    MissingRoot,
    Limit,
    Overloaded,
    Unconfirmed,
    Closed,
    Storage(anyhow::Error),
}

impl fmt::Display for FleetCatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fleet catalog: {self:?}")
    }
}

impl std::error::Error for FleetCatalogError {}

type Result<T> = std::result::Result<T, FleetCatalogError>;

#[derive(Clone)]
pub struct FleetCatalog {
    inner: Arc<Inner>,
}

struct Inner {
    bucket: Bucket,
    domain: DomainToken,
    prefix: String,
    cluster: [u8; 16],
    incarnation: [u8; 16],
    // A bounded turn has at most one request, root and index path in memory.
    // Contending callers receive backpressure rather than an unbounded queue.
    turn: Mutex<()>,
}

#[derive(Clone)]
pub struct FleetSnapshot {
    owner: Arc<Inner>,
    head: Head,
    token: String,
}

impl FleetSnapshot {
    pub fn revision(&self) -> u64 {
        self.head.revision
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetEntry {
    pub key: String,
    pub value: Vec<u8>,
}

/// Bound to a particular immutable index; a new snapshot cannot reinterpret it.
#[derive(Clone, Debug)]
pub struct FleetCursor {
    index: Option<[u8; 32]>,
    hash: [u8; 32],
    key: String,
}

pub struct FleetPage {
    pub entries: Vec<FleetEntry>,
    pub next: Option<FleetCursor>,
}

pub struct FleetCommit {
    pub snapshot: FleetSnapshot,
    /// The original applied revision, even when subsequent writers advanced it.
    pub applied_revision: u64,
}

impl FleetCatalog {
    pub fn new(
        domain: &ExecutionDomain,
        bucket: Bucket,
        cluster: ClusterId,
        incarnation: IncarnationId,
        prefix: &str,
    ) -> Result<Self> {
        let prefix = crate::replica_storage::normalize_prefix(prefix)
            .map_err(|_| FleetCatalogError::Invalid("prefix"))?;
        if prefix.len() > 1024 {
            return Err(FleetCatalogError::Limit);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                bucket,
                domain: domain
                    .services()
                    .domain_token()
                    .ok_or(FleetCatalogError::Closed)?,
                prefix: format!("{prefix}raft/{}/fleet/", hex::encode(cluster.bytes())),
                cluster: cluster.bytes(),
                incarnation: incarnation.bytes(),
                turn: Mutex::new(()),
            }),
        })
    }

    /// Explicit bootstrap only. A surviving pin forbids recreating a lost root.
    pub async fn initialize(&self, timeout: Duration) -> Result<FleetSnapshot> {
        self.run(timeout, async {
            let pin = self.inner.read::<Pin>("pin", PIN_KIND, PAGE_LIMIT).await?;
            let found = self.inner.read_head().await?;
            if pin.is_some() && found.is_none() {
                return Err(FleetCatalogError::MissingRoot);
            }
            if let Some((pin, _)) = pin {
                self.inner.validate_pin(&pin)?;
            }
            let genesis = Head::new(self.inner.cluster, self.inner.incarnation);
            let head = if let Some((head, token)) = found {
                self.inner.validate_head(&head)?;
                (head, token)
            } else {
                let bytes = encode(HEAD_KIND, &genesis, ROOT_LIMIT)?;
                self.inner.create_exact("head", &bytes, ROOT_LIMIT).await?;
                self.inner
                    .read_head()
                    .await?
                    .ok_or(FleetCatalogError::Unconfirmed)?
            };
            if self
                .inner
                .read::<Pin>("pin", PIN_KIND, PAGE_LIMIT)
                .await?
                .is_none()
            {
                // No mutation is admitted until the genesis pin is confirmed.
                if head.0 != genesis {
                    return Err(FleetCatalogError::Corrupt("missing non-genesis pin"));
                }
                let bytes = encode(PIN_KIND, &self.inner.pin()?, PAGE_LIMIT)?;
                self.inner.create_exact("pin", &bytes, PAGE_LIMIT).await?;
            }
            self.inner.snapshot(head.0, head.1)
        })
        .await
    }

    pub async fn read(&self, timeout: Duration) -> Result<FleetSnapshot> {
        self.run(timeout, self.inner.current()).await
    }

    pub async fn get(
        &self,
        snapshot: &FleetSnapshot,
        table: FleetTable,
        key: &str,
        timeout: Duration,
    ) -> Result<Option<Vec<u8>>> {
        self.check_snapshot(snapshot)?;
        records::validate_key(key)?;
        self.run(
            timeout,
            self.inner.lookup(snapshot.head.tables[table.slot()], key),
        )
        .await
    }

    pub async fn page(
        &self,
        snapshot: &FleetSnapshot,
        table: FleetTable,
        after: Option<&FleetCursor>,
        limit: usize,
        timeout: Duration,
    ) -> Result<FleetPage> {
        self.check_snapshot(snapshot)?;
        if !(1..=128).contains(&limit) {
            return Err(FleetCatalogError::Limit);
        }
        let index = snapshot.head.tables[table.slot()];
        if after.is_some_and(|cursor| cursor.index != index) {
            return Err(FleetCatalogError::Invalid("cursor index"));
        }
        self.run(timeout, self.inner.scan(index, after, limit))
            .await
    }

    /// Commit one exact mutation. Retry the same ID/payload after an unknown result.
    pub async fn commit(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        mutation.validate()?;
        self.run(timeout, self.commit_inner(snapshot, mutation))
            .await
    }

    fn check_snapshot(&self, snapshot: &FleetSnapshot) -> Result<()> {
        if !Arc::ptr_eq(&self.inner, &snapshot.owner) {
            return Err(FleetCatalogError::Invalid("snapshot owner"));
        }
        Ok(())
    }

    async fn run<T>(&self, timeout: Duration, work: impl Future<Output = Result<T>>) -> Result<T> {
        if timeout.is_zero() || timeout > Duration::from_secs(60) {
            return Err(FleetCatalogError::Invalid("timeout"));
        }
        let _turn = self
            .inner
            .turn
            .try_lock()
            .map_err(|_| FleetCatalogError::Overloaded)?;
        let deadline = self
            .inner
            .domain
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed)?
            .checked_add(timeout.as_millis() as u64)
            .ok_or(FleetCatalogError::Limit)?;
        let timer = self
            .inner
            .domain
            .sleep_until(deadline)
            .map_err(|_| FleetCatalogError::Closed)?;
        let result = crate::asyncrt::select! {
            _=timer=>Err(FleetCatalogError::Unconfirmed),
            result=work=>result,
        };
        self.inner
            .domain
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed)?;
        result
    }

    async fn commit_inner(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
    ) -> Result<FleetCommit> {
        let bytes = encode(REQUEST_KIND, mutation, REQUEST_LIMIT)?;
        let digest = hash(&bytes);
        let key = hex::encode(mutation.id);
        let current = self.inner.current().await?;
        if let Some(applied_revision) = self.inner.committed(&current.head, &key, digest).await? {
            return Ok(FleetCommit {
                snapshot: current,
                applied_revision,
            });
        }
        if current.token != snapshot.token || mutation.expected_revision != snapshot.revision() {
            return Err(FleetCatalogError::Conflict);
        }
        self.inner
            .create_exact(&format!("requests/{key}"), &bytes, REQUEST_LIMIT)
            .await?;
        let mut next = snapshot.head.clone();
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or(FleetCatalogError::Limit)?;
        for change in &mutation.changes {
            let slot = change.table.slot();
            next.tables[slot] = self
                .inner
                .update(next.tables[slot], &change.key, change.value.clone(), 0)
                .await?;
        }
        let record = CommitRecord {
            request_digest: digest,
            revision: next.revision,
        };
        next.journal = self
            .inner
            .update(
                next.journal,
                &key,
                Some(encode(COMMIT_KIND, &record, PAGE_LIMIT)?),
                0,
            )
            .await?;
        let bytes = encode(HEAD_KIND, &next, ROOT_LIMIT)?;
        let result = self
            .inner
            .bucket
            .put_cas(&self.inner.key("head"), bytes, Some(&snapshot.token))
            .await;
        // Always reconcile the journal, including a clean conflict caused by an
        // earlier identical submission. Never retry a CAS internally.
        let observed = self.inner.current().await?;
        if let Some(applied_revision) = self.inner.committed(&observed.head, &key, digest).await? {
            return Ok(FleetCommit {
                snapshot: observed,
                applied_revision,
            });
        }
        match result {
            Ok(None) => Err(FleetCatalogError::Conflict),
            Err(error) => Err(FleetCatalogError::Storage(error)),
            Ok(Some(_)) => Err(FleetCatalogError::Unconfirmed),
        }
    }
}

impl Inner {
    fn key(&self, tail: &str) -> String {
        format!("{}{tail}", self.prefix)
    }

    fn pin(&self) -> Result<Pin> {
        Ok(Pin {
            cluster: self.cluster,
            incarnation: self.incarnation,
            genesis: hash(&encode(
                HEAD_KIND,
                &Head::new(self.cluster, self.incarnation),
                ROOT_LIMIT,
            )?),
        })
    }

    fn validate_pin(&self, pin: &Pin) -> Result<()> {
        if pin != &self.pin()? {
            return Err(FleetCatalogError::Corrupt("fleet pin"));
        }
        Ok(())
    }

    fn validate_head(&self, head: &Head) -> Result<()> {
        if head.cluster != self.cluster
            || head.incarnation != self.incarnation
            || head.revision == 0
            || head.format != 1
        {
            return Err(FleetCatalogError::Corrupt("fleet identity/revision"));
        }
        Ok(())
    }

    fn snapshot(self: &Arc<Self>, head: Head, token: String) -> Result<FleetSnapshot> {
        self.validate_head(&head)?;
        Ok(FleetSnapshot {
            owner: self.clone(),
            head,
            token,
        })
    }

    async fn read<T: DeserializeOwned + Serialize>(
        &self,
        tail: &str,
        kind: u8,
        limit: usize,
    ) -> Result<Option<(T, String)>> {
        let value = self
            .bucket
            .get_bounded(&self.key(tail), limit)
            .await
            .map_err(FleetCatalogError::Storage)?;
        value
            .map(|(bytes, token)| Ok((decode(kind, &bytes, limit)?, token)))
            .transpose()
    }

    async fn read_head(&self) -> Result<Option<(Head, String)>> {
        self.read("head", HEAD_KIND, ROOT_LIMIT).await
    }

    async fn current(self: &Arc<Self>) -> Result<FleetSnapshot> {
        let (pin, _) = self
            .read::<Pin>("pin", PIN_KIND, PAGE_LIMIT)
            .await?
            .ok_or(FleetCatalogError::MissingRoot)?;
        self.validate_pin(&pin)?;
        let (head, token) = self
            .read_head()
            .await?
            .ok_or(FleetCatalogError::MissingRoot)?;
        self.snapshot(head, token)
    }

    async fn create_exact(&self, tail: &str, bytes: &[u8], limit: usize) -> Result<()> {
        let key = self.key(tail);
        let put = self.bucket.put_cas(&key, bytes.to_vec(), None).await;
        match self
            .bucket
            .get_bounded(&key, limit)
            .await
            .map_err(FleetCatalogError::Storage)?
        {
            Some((actual, _)) if actual.as_ref() == bytes => Ok(()),
            Some(_) => Err(FleetCatalogError::PayloadConflict),
            None => match put {
                Err(error) => Err(FleetCatalogError::Storage(error)),
                _ => Err(FleetCatalogError::Unconfirmed),
            },
        }
    }

    async fn committed(&self, head: &Head, key: &str, digest: [u8; 32]) -> Result<Option<u64>> {
        let Some(bytes) = self.lookup(head.journal, key).await? else {
            return Ok(None);
        };
        let record: CommitRecord = decode(COMMIT_KIND, &bytes, PAGE_LIMIT)?;
        if record.request_digest != digest {
            return Err(FleetCatalogError::PayloadConflict);
        }
        if record.revision <= 1 || record.revision > head.revision {
            return Err(FleetCatalogError::Corrupt("journal revision"));
        }
        Ok(Some(record.revision))
    }
}

fn encode(kind: u8, value: &impl Serialize, limit: usize) -> Result<Vec<u8>> {
    super::catalog_wire::encode(kind, value, limit).map_err(convert)
}

fn decode<T: DeserializeOwned + Serialize>(kind: u8, bytes: &[u8], limit: usize) -> Result<T> {
    super::catalog_wire::decode(kind, bytes, limit).map_err(convert)
}

fn convert(error: super::catalog::CatalogError) -> FleetCatalogError {
    match error {
        super::catalog::CatalogError::Limit => FleetCatalogError::Limit,
        _ => FleetCatalogError::Corrupt("fleet record encoding"),
    }
}

fn hash(bytes: &[u8]) -> [u8; 32] {
    super::catalog_wire::digest(bytes)
}
