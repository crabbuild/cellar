// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Persistent ownership of group archive generations. Dropping local handles
//! never retires a remote owner. Allocating intents must be persisted before CAS.
mod state;
mod wire;
use wire::WireCreation;

use super::{objects::Object, *};
use cellar_logic::cell_group::GroupRoute;
use object_store::{PutMode, PutOptions, UpdateVersion};
use rand::RngCore;
use state::State;

const IDENTITY_DOMAIN: &str = "cellar-cell-group-archive-identity-1";
const STATE_DOMAIN: &str = "cellar-cell-group-archive-ownership-1";
const INTENT_DOMAIN: &str = "cellar-cell-group-archive-intent-1";
const MAX_CONTROL_BYTES: usize = 64 * 1024;
const MAX_INTENT_BYTES: usize = 8192;

/// Persist before creation. Retry reconciles an already complete creation.
/// An incomplete identity requires authority recovery; counters never reset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "WireCreation", into = "WireCreation")]
pub struct CellGroupArchiveCreation {
    domain: String,
    id: [u8; 32],
    route: GroupRoute,
}

/// Fresh, one-use initialization authority. Serialized creation metadata can
/// reopen established state but cannot recreate counters after metadata loss.
pub struct PreparedCellGroupArchiveCreation {
    archive: CellGroupArchive,
    identity: CellGroupArchiveCreation,
}

impl PreparedCellGroupArchiveCreation {
    pub fn intent(&self) -> &CellGroupArchiveCreation {
        &self.identity
    }

    pub async fn commit(
        self,
        domain: &impl CellGroupArchiveExecution,
    ) -> Result<CellGroupArchiveRegistry> {
        self.archive.create_registry(domain, &self.identity).await
    }
}

impl CellGroupArchiveCreation {
    pub fn route(&self) -> &GroupRoute {
        &self.route
    }

    fn check(&self, history: GroupHistory) -> Result<()> {
        if self.domain != IDENTITY_DOMAIN || self.id == [0; 32] || self.route.history != history {
            return Err(CellGroupArchiveError::Invalid("archive registry identity"));
        }
        super::super::ManifestTree::new(self.route.clone(), Default::default())
            .map_err(CellGroupCheckpointError::from)?;
        Ok(())
    }
}

/// A never-reused upload generation. It is protected until explicitly aborted
/// or consumed by publication, including after its local handle is dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellGroupArchiveUploadTicket {
    registry: [u8; 32],
    generation: u64,
    allocation: [u8; 32],
}

impl CellGroupArchiveUploadTicket {
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Descriptor {
    pub object: Object,
    pub marker: GroupManifest,
}

impl Descriptor {
    fn check(&self, identity: &CellGroupArchiveCreation, revision: u64) -> Result<()> {
        self.marker
            .check()
            .map_err(CellGroupCheckpointError::from)?;
        if self.marker.route() != &identity.route
            || self.object.registry != identity.id
            || self.object.generation < 2
            || self.object.generation >= revision
            || self.object.length == 0
            || self.object.length > 8192
        {
            return Err(CellGroupArchiveError::Invalid(
                "archive retained descriptor",
            ));
        }
        Ok(())
    }
}

/// Explicit remote hold, distinct from integrity, quorum and serving proofs.
/// Persist it with its owner; release only after that owner retires/fences all
/// users and joins entered work. Never release merely because a Rust value drops.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellGroupArchiveHold {
    registry: [u8; 32],
    id: u64,
    allocation: [u8; 32],
    root: Descriptor,
}

impl CellGroupArchiveHold {
    pub fn marker(&self) -> &GroupManifest {
        &self.root.marker
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum Action {
    Reserve {
        parent: Option<Descriptor>,
    },
    Acquire {
        root: Descriptor,
    },
    Publish {
        ticket: CellGroupArchiveUploadTicket,
        root: Descriptor,
    },
}

/// One exact allocation at one registry revision. Persist the entire intent
/// before sending its mutation. A lost response never justifies a new intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellGroupArchiveIntent {
    domain: String,
    registry: [u8; 32],
    revision: u64,
    nonce: [u8; 32],
    action: Action,
}

impl CellGroupArchiveIntent {
    fn digest(&self) -> Result<[u8; 32]> {
        if self.domain != INTENT_DOMAIN
            || self.registry == [0; 32]
            || self.nonce == [0; 32]
            || self.revision < 2
            || self.revision == u64::MAX
        {
            return Err(CellGroupArchiveError::Invalid("archive allocation intent"));
        }
        Ok(Sha256::digest(encode(self, MAX_INTENT_BYTES)?).into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pending {
    pub ticket: CellGroupArchiveUploadTicket,
    pub parent: Option<Descriptor>,
}

pub(super) enum Allocation {
    Upload(CellGroupArchiveUploadTicket),
    Hold(Box<CellGroupArchiveHold>),
}

pub(super) struct Protection {
    pub revision: u64,
    pub roots: Vec<Descriptor>,
    pub pending_generations: Vec<u64>,
}

/// Conditional storage adapter for one exact group route and registry identity.
/// Owns no Raft engine and cannot grant membership or application durability.
#[derive(Clone)]
pub struct CellGroupArchiveRegistry {
    pub(super) archive: CellGroupArchive,
    identity: CellGroupArchiveCreation,
}

fn encode(value: &impl Serialize, limit: usize) -> Result<Vec<u8>> {
    let mut output = super::checkpoint::Bounded {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut output, value).map_err(|_| CellGroupArchiveError::Limit)?;
    Ok(output.bytes)
}

impl CellGroupArchive {
    /// Preparation performs no storage mutation. The execution domain supplies
    /// randomness; the caller durably records the complete result before create.
    pub async fn prepare_registry_creation(
        &self,
        domain: &impl CellGroupArchiveExecution,
        route: GroupRoute,
    ) -> Result<PreparedCellGroupArchiveCreation> {
        domain
            .run(async {
                let mut id = [0; 32];
                crate::asyncrt::rng("cell-group-archive-registry").fill_bytes(&mut id);
                let creation = CellGroupArchiveCreation {
                    domain: IDENTITY_DOMAIN.into(),
                    id,
                    route,
                };
                creation.check(self.history)?;
                Ok(PreparedCellGroupArchiveCreation {
                    archive: self.clone(),
                    identity: creation,
                })
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    async fn create_registry(
        &self,
        domain: &impl CellGroupArchiveExecution,
        creation: &CellGroupArchiveCreation,
    ) -> Result<CellGroupArchiveRegistry> {
        domain
            .run(async {
                let _io = self.admit()?;
                creation.check(self.history)?;
                let registry = CellGroupArchiveRegistry {
                    archive: self.clone(),
                    identity: creation.clone(),
                };
                if self
                    .read_versioned(&registry.path("registry"), MAX_CONTROL_BYTES)
                    .await?
                    .is_some()
                {
                    registry.load().await?;
                    return Ok(registry);
                }
                let bytes = encode(creation, MAX_CONTROL_BYTES)?;
                // Only a confirmed exclusive identity creation can initialize the
                // counter. An existing identity may be reconciled only by opening
                // valid established state, never by recreating a missing registry.
                match registry.write("identity", bytes, PutMode::Create).await {
                    Ok(()) => (),
                    Err(_) => {
                        registry
                            .load()
                            .await
                            .map_err(|_| CellGroupArchiveError::Unconfirmed)?;
                        return Ok(registry);
                    }
                }
                let desired = encode(&State::empty(creation.clone()), MAX_CONTROL_BYTES)?;
                let outcome = registry.write("registry", desired, PutMode::Create).await;
                match registry.load().await {
                    Ok(_) => Ok(registry),
                    Err(_) => outcome.and(Err(CellGroupArchiveError::Unconfirmed)),
                }
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    /// Open established state. Missing identity/state and malformed conditional
    /// versions refuse; opening never allocates or repairs a missing record.
    pub async fn open_registry(
        &self,
        domain: &impl CellGroupArchiveExecution,
        expected: &CellGroupArchiveCreation,
    ) -> Result<CellGroupArchiveRegistry> {
        domain
            .run(async {
                let _io = self.admit()?;
                expected.check(self.history)?;
                let registry = CellGroupArchiveRegistry {
                    archive: self.clone(),
                    identity: expected.clone(),
                };
                registry.load().await?;
                Ok(registry)
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }
}

impl CellGroupArchiveRegistry {
    pub fn archive(&self) -> &CellGroupArchive {
        &self.archive
    }

    pub fn route(&self) -> &GroupRoute {
        &self.identity.route
    }

    pub fn creation(&self) -> &CellGroupArchiveCreation {
        &self.identity
    }

    pub(super) fn id(&self) -> [u8; 32] {
        self.identity.id
    }

    fn path(&self, name: &str) -> Path {
        self.archive.prefix.child("ownership").child(name)
    }

    async fn write(&self, name: &str, bytes: Vec<u8>, mode: PutMode) -> Result<()> {
        self.archive
            .io(async {
                self.archive
                    .store
                    .put_opts(&self.path(name), bytes.into(), PutOptions::from(mode))
                    .await
                    .map_err(CellGroupArchiveError::Storage)?;
                Ok(())
            })
            .await
    }

    async fn load(&self) -> Result<(State, UpdateVersion)> {
        let identity = self
            .archive
            .read_versioned(&self.path("identity"), MAX_CONTROL_BYTES)
            .await?
            .ok_or(CellGroupArchiveError::MissingRegistry)?
            .0;
        if identity != encode(&self.identity, MAX_CONTROL_BYTES)? {
            return Err(CellGroupArchiveError::Invalid(
                "archive registry identity changed",
            ));
        }
        let (bytes, version) = self
            .archive
            .read_versioned(&self.path("registry"), MAX_CONTROL_BYTES)
            .await?
            .ok_or(CellGroupArchiveError::Invalid(
                "missing established archive registry",
            ))?;
        if version.e_tag.as_ref().is_some_and(String::is_empty)
            || version.version.as_ref().is_some_and(String::is_empty)
            || version.e_tag.as_ref().map_or(0, String::len)
                + version.version.as_ref().map_or(0, String::len)
                > 8192
            || (version.e_tag.is_none() && version.version.is_none())
        {
            return Err(CellGroupArchiveError::Invalid(
                "archive registry conditional version",
            ));
        }
        let state: State = serde_json::from_slice(&bytes)
            .map_err(|_| CellGroupArchiveError::Invalid("archive registry encoding"))?;
        state.check(&self.identity)?;
        Ok((state, version))
    }

    pub async fn prepare_upload(
        &self,
        domain: &impl CellGroupArchiveExecution,
    ) -> Result<CellGroupArchiveIntent> {
        domain
            .run(async {
                let _io = self.archive.admit()?;
                let state = self.load().await?.0;
                self.intent(
                    &state,
                    Action::Reserve {
                        parent: state.selected.clone(),
                    },
                )
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    pub async fn reserve(
        &self,
        domain: &impl CellGroupArchiveExecution,
        intent: &CellGroupArchiveIntent,
    ) -> Result<CellGroupArchiveUploadTicket> {
        if !matches!(intent.action, Action::Reserve { .. }) {
            return Err(CellGroupArchiveError::Invalid("upload intent purpose"));
        }
        domain
            .run(async {
                let _io = self.archive.admit()?;
                match self.commit(intent).await? {
                    Allocation::Upload(ticket) => Ok(ticket),
                    _ => Err(CellGroupArchiveError::Invalid("upload allocation purpose")),
                }
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    pub async fn prepare_acquire(
        &self,
        domain: &impl CellGroupArchiveExecution,
    ) -> Result<CellGroupArchiveIntent> {
        domain
            .run(async {
                let _io = self.archive.admit()?;
                let state = self.load().await?.0;
                let root = state
                    .selected
                    .clone()
                    .ok_or(CellGroupArchiveError::Invalid("no selected archive"))?;
                self.intent(&state, Action::Acquire { root })
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    pub async fn acquire(
        &self,
        domain: &impl CellGroupArchiveExecution,
        intent: &CellGroupArchiveIntent,
    ) -> Result<CellGroupArchiveHold> {
        if !matches!(intent.action, Action::Acquire { .. }) {
            return Err(CellGroupArchiveError::Invalid("acquire intent purpose"));
        }
        domain
            .run(async {
                let _io = self.archive.admit()?;
                match self.commit(intent).await? {
                    Allocation::Hold(hold) => Ok(*hold),
                    _ => Err(CellGroupArchiveError::Invalid("hold allocation purpose")),
                }
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    fn intent(&self, state: &State, action: Action) -> Result<CellGroupArchiveIntent> {
        let mut nonce = [0; 32];
        crate::asyncrt::rng("cell-group-archive-intent").fill_bytes(&mut nonce);
        let intent = CellGroupArchiveIntent {
            domain: INTENT_DOMAIN.into(),
            registry: self.identity.id,
            revision: state
                .revision
                .checked_add(1)
                .ok_or(CellGroupArchiveError::Limit)?,
            nonce,
            action,
        };
        let mut proposed = state.clone();
        proposed.allocate(&intent)?;
        proposed.check(&self.identity)?;
        Ok(intent)
    }

    pub(super) async fn commit(&self, intent: &CellGroupArchiveIntent) -> Result<Allocation> {
        if intent.registry != self.identity.id {
            return Err(CellGroupArchiveError::Conflict);
        }
        let (mut state, version) = self.load().await?;
        if let Some(result) = state.reconcile(intent)? {
            return Ok(result);
        }
        state.allocate(intent)?;
        state.check(&self.identity)?;
        let outcome = self
            .write(
                "registry",
                encode(&state, MAX_CONTROL_BYTES)?,
                PutMode::Update(version),
            )
            .await;
        let observed = self
            .load()
            .await
            .map_err(|_| CellGroupArchiveError::Unconfirmed)?
            .0;
        if let Some(result) = observed.reconcile(intent)? {
            return Ok(result);
        }
        if matches!(
            outcome,
            Err(CellGroupArchiveError::Storage(
                object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. }
            ))
        ) {
            return Err(CellGroupArchiveError::Conflict);
        }
        Err(CellGroupArchiveError::Unconfirmed)
    }

    pub(super) async fn prepare_publish(
        &self,
        ticket: CellGroupArchiveUploadTicket,
        root: Descriptor,
    ) -> Result<CellGroupArchiveIntent> {
        let state = self.load().await?.0;
        self.intent(&state, Action::Publish { ticket, root })
    }

    pub(super) async fn publication_hold(
        &self,
        intent: &CellGroupArchiveIntent,
    ) -> Result<CellGroupArchiveHold> {
        if !matches!(intent.action, Action::Publish { .. }) {
            return Err(CellGroupArchiveError::Invalid("publication intent purpose"));
        }
        match self.load().await?.0.reconcile(intent)? {
            Some(Allocation::Hold(hold)) => Ok(*hold),
            _ => Err(CellGroupArchiveError::Expired),
        }
    }

    pub(super) async fn pending(&self, ticket: &CellGroupArchiveUploadTicket) -> Result<Pending> {
        if ticket.registry != self.identity.id {
            return Err(CellGroupArchiveError::Conflict);
        }
        self.load()
            .await?
            .0
            .pending
            .iter()
            .find(|p| &p.ticket == ticket)
            .cloned()
            .ok_or(CellGroupArchiveError::Expired)
    }

    pub(super) async fn protection(&self) -> Result<Protection> {
        let state = self.load().await?.0;
        Ok(Protection {
            revision: state.revision,
            roots: state.roots(),
            pending_generations: state.pending.iter().map(|p| p.ticket.generation).collect(),
        })
    }

    pub(super) async fn referenced(&self, hold: &CellGroupArchiveHold) -> Result<Descriptor> {
        if hold.registry != self.identity.id {
            return Err(CellGroupArchiveError::Conflict);
        }
        self.load()
            .await?
            .0
            .holds
            .iter()
            .find(|h| *h == hold)
            .map(|h| h.root.clone())
            .ok_or(CellGroupArchiveError::Expired)
    }

    pub async fn release(
        &self,
        domain: &impl CellGroupArchiveExecution,
        hold: &CellGroupArchiveHold,
    ) -> Result<()> {
        if hold.registry != self.identity.id {
            return Err(CellGroupArchiveError::Conflict);
        }
        domain
            .run(self.retire(Some(hold), None))
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    pub async fn abort(
        &self,
        domain: &impl CellGroupArchiveExecution,
        ticket: &CellGroupArchiveUploadTicket,
    ) -> Result<()> {
        if ticket.registry != self.identity.id {
            return Err(CellGroupArchiveError::Conflict);
        }
        domain
            .run(self.retire(None, Some(ticket)))
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    async fn retire(
        &self,
        hold: Option<&CellGroupArchiveHold>,
        ticket: Option<&CellGroupArchiveUploadTicket>,
    ) -> Result<()> {
        let _io = self.archive.admit()?;
        for _ in 0..8 {
            let (mut state, version) = self.load().await?;
            if !state.retire(hold, ticket)? {
                return Ok(());
            }
            state.check(&self.identity)?;
            let outcome = self
                .write(
                    "registry",
                    encode(&state, MAX_CONTROL_BYTES)?,
                    PutMode::Update(version),
                )
                .await;
            let mut observed = self
                .load()
                .await
                .map_err(|_| CellGroupArchiveError::Unconfirmed)?
                .0;
            if !observed.retire(hold, ticket)? {
                return Ok(());
            }
            if matches!(
                outcome,
                Err(CellGroupArchiveError::Storage(
                    object_store::Error::Precondition { .. }
                        | object_store::Error::AlreadyExists { .. }
                ))
            ) {
                continue;
            }
            return Err(CellGroupArchiveError::Unconfirmed);
        }
        Err(CellGroupArchiveError::Conflict)
    }
}
