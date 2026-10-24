//! Authoritative admission inventory for namespace births. Freeze orders against
//! admission at one CAS root; pending decisions stay enumerable without listing.
use super::{BirthProposal, CatalogError, Inner, NamespaceCatalog};
use crate::raft::catalog_wire as record;
use crate::raft::coordination_index::{Index, IndexCursor, PageStore};
use bytes::Bytes;
use cellar_logic::consensus::ProposalId;
use futures_util::future::BoxFuture;
use object_store::{path::Path, PutMode, PutOptions, UpdateVersion};
use std::{sync::Arc, time::Duration};

mod import;
mod wire;
pub(crate) use import::BirthImportCut;
use wire::{Head, Mode, Pin};

const HEAD_KIND: u8 = 6;
const PIN_KIND: u8 = 7;
const MAX_ATTEMPTS: usize = 8;
const MAX_PAGE: usize = 16;
const INDEX_PAGE_LIMIT: usize = 64 * 1024;

/// Exact frozen admission set, including decisions whose writers have not yet
/// published. This is not an import-completeness or membership/removal proof.
#[derive(Clone)]
pub struct FrozenBirthInventory {
    owner: Arc<Inner>,
    root: [u8; 32],
    operation: ProposalId,
    revision: u64,
    count: u64,
    index: Option<[u8; 32]>,
}

impl FrozenBirthInventory {
    pub fn len(&self) -> u64 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn operation(&self) -> ProposalId {
        self.operation
    }

    pub fn namespace_root(&self) -> [u8; 32] {
        self.root
    }
}

#[derive(Clone, Debug)]
pub struct BirthInventoryCursor {
    root: [u8; 32],
    after: IndexCursor,
}

/// One admitted canonical birth. Its actual live/cancelled decision still needs
/// exact-key reconciliation; a missing decision is never absence proof.
pub struct BirthInventoryEntry {
    proposal: BirthProposal,
    digest: [u8; 32],
}

impl BirthInventoryEntry {
    pub fn proposal(&self) -> &BirthProposal {
        &self.proposal
    }

    pub fn expected_digest(&self) -> [u8; 32] {
        self.digest
    }
}

pub struct BirthInventoryPage {
    pub entries: Vec<BirthInventoryEntry>,
    pub next: Option<BirthInventoryCursor>,
}

struct Store<'a>(&'a NamespaceCatalog);

impl Store<'_> {
    fn key(&self, tail: &str) -> Path {
        Path::from(format!("{}births/{tail}", self.0.namespace_key()))
    }

    fn pin(&self) -> Result<Pin, CatalogError> {
        Ok(Pin {
            root: self.0.root_digest,
            genesis: record::digest(&record::encode(
                HEAD_KIND,
                &Head::genesis(self.0),
                self.0.inner.limit,
            )?),
        })
    }

    async fn head(&self) -> Result<Option<(Head, UpdateVersion)>, CatalogError> {
        self.0
            .inner
            .read_versioned(&self.key("head"))
            .await?
            .map(|(bytes, token)| {
                Ok((
                    record::decode(HEAD_KIND, &bytes, self.0.inner.limit)?,
                    token,
                ))
            })
            .transpose()
    }

    async fn current(&self) -> Result<(Head, UpdateVersion), CatalogError> {
        let pin = self
            .0
            .inner
            .read(&self.key("pin"))
            .await?
            .ok_or(CatalogError::Corrupt("missing birth inventory pin"))?;
        let pin: Pin = record::decode(PIN_KIND, &pin, self.0.inner.limit)?;
        if pin != self.pin()? {
            return Err(CatalogError::Corrupt("birth inventory pin"));
        }
        let (head, token) = self
            .head()
            .await?
            .ok_or(CatalogError::Corrupt("missing birth inventory head"))?;
        head.validate(self.0)?;
        if !token.e_tag.as_ref().is_some_and(|value| !value.is_empty())
            && !token
                .version
                .as_ref()
                .is_some_and(|value| !value.is_empty())
        {
            return Err(CatalogError::Corrupt("birth inventory missing CAS token"));
        }
        Ok((head, token))
    }

    async fn cas(&self, head: &Head, token: UpdateVersion) -> Result<bool, CatalogError> {
        // No hidden retries or overwrite fallback. A lost result is reconciled
        // through the exact root; callers retain the original intent/operation.
        let bytes = record::encode(HEAD_KIND, head, self.0.inner.limit)?;
        match self
            .0
            .inner
            .store
            .put_opts(
                &self.key("head"),
                bytes.into(),
                PutOptions::from(PutMode::Update(token)),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::Precondition { .. } | object_store::Error::NotFound { .. },
            ) => Ok(false),
            Err(error) => Err(CatalogError::Storage(error)),
        }
    }

    async fn intent(&self, id: [u8; 32]) -> Result<record::Birth, CatalogError> {
        let bytes = self
            .0
            .inner
            .read(&self.key(&format!("intents/{}", hex::encode(id))))
            .await?
            .ok_or(CatalogError::Corrupt("missing birth intent"))?;
        if record::digest(&bytes) != id {
            return Err(CatalogError::Corrupt("birth intent hash"));
        }
        let intent: record::Birth = record::decode(record::BIRTH, &bytes, self.0.inner.limit)?;
        self.proposal(&intent)?;
        Ok(intent)
    }

    fn proposal(&self, intent: &record::Birth) -> Result<BirthProposal, CatalogError> {
        let scope = intent
            .scope
            .clone()
            .decode()
            .map_err(|_| CatalogError::Corrupt("birth intent scope"))?;
        self.0.check_scope(&scope)?;
        if intent.root_digest != self.0.root_digest
            || intent.cancelled_by.is_some()
            || intent.empty_digest != record::digest(&[])
            || intent.voters.len() > 32
            || intent.learners.len() > 32
        {
            return Err(CatalogError::Corrupt("birth intent identity"));
        }
        self.0.check_birth_owner(intent.managed_owner.as_ref())?;
        let voters = intent
            .voters
            .iter()
            .map(record::Member::decode)
            .collect::<Result<Vec<_>, _>>()?;
        let learners = intent
            .learners
            .iter()
            .map(record::Member::decode)
            .collect::<Result<Vec<_>, _>>()?;
        self.0.check_members(&voters, &learners)?;
        Ok(BirthProposal {
            scope,
            voters,
            learners,
        })
    }
}

impl PageStore for Store<'_> {
    type Error = CatalogError;

    fn page_limit(&self) -> usize {
        INDEX_PAGE_LIMIT
    }

    fn read_page(&self, id: [u8; 32]) -> BoxFuture<'_, Result<Bytes, CatalogError>> {
        Box::pin(async move {
            self.0
                .inner
                .read_versioned_bounded(
                    &self.key(&format!("objects/{}", hex::encode(id))),
                    INDEX_PAGE_LIMIT,
                )
                .await?
                .map(|(bytes, _)| Bytes::from(bytes))
                .ok_or(CatalogError::Corrupt("missing birth inventory page"))
        })
    }

    fn create_page(&self, id: [u8; 32], bytes: Vec<u8>) -> BoxFuture<'_, Result<(), CatalogError>> {
        Box::pin(async move {
            if self
                .0
                .inner
                .create_and_observe_bounded(
                    &self.key(&format!("objects/{}", hex::encode(id))),
                    &bytes,
                    INDEX_PAGE_LIMIT,
                )
                .await?
                != bytes
            {
                return Err(CatalogError::Corrupt("birth inventory page collision"));
            }
            Ok(())
        })
    }
}

fn name_key(cell: &str) -> String {
    hex::encode(record::digest(cell.as_bytes()))
}

fn pointer(bytes: &[u8]) -> Result<[u8; 32], CatalogError> {
    bytes
        .try_into()
        .map_err(|_| CatalogError::Corrupt("birth intent pointer"))
}

impl NamespaceCatalog {
    /// Prepare only before namespace-root publication. A non-genesis inventory
    /// must have an existing namespace root; it cannot authorize root recreation.
    pub(super) async fn initialize_birth_inventory(&self) -> Result<bool, CatalogError> {
        let store = Store(self);
        let genesis = Head::genesis(self);
        let pin = self.inner.read(&store.key("pin")).await?;
        let head = store.head().await?;
        if pin.is_some() && head.is_none() {
            return Err(CatalogError::Corrupt("lost birth inventory head"));
        }
        let head = if let Some((head, _)) = head {
            head
        } else {
            let bytes = record::encode(HEAD_KIND, &genesis, self.inner.limit)?;
            let observed = self
                .inner
                .create_and_observe(&store.key("head"), &bytes)
                .await?;
            record::decode(HEAD_KIND, &observed, self.inner.limit)?
        };
        if head.root != self.root_digest {
            return Err(CatalogError::Conflict(
                "namespace identity already reserved",
            ));
        }
        head.validate(self)?;
        if pin.is_none() {
            if head != genesis {
                return Err(CatalogError::Corrupt("missing active birth inventory pin"));
            }
            let bytes = record::encode(PIN_KIND, &store.pin()?, self.inner.limit)?;
            let observed = self
                .inner
                .create_and_observe(&store.key("pin"), &bytes)
                .await?;
            if observed != bytes {
                return Err(CatalogError::Corrupt("birth inventory pin collision"));
            }
        }
        let (current, _) = store.current().await?;
        Ok(current == genesis)
    }

    pub(super) async fn require_birth_inventory(&self) -> Result<(), CatalogError> {
        Store(self).current().await?;
        Ok(())
    }

    pub(super) async fn admit_birth_intent(
        &self,
        proposal: BirthProposal,
        owner: Option<&crate::raft::catalog::ManagedBirthOwner>,
    ) -> Result<record::Birth, CatalogError> {
        let (proposal, _) = self.prepare_birth_with_owner(proposal, owner)?;
        let store = Store(self);
        let index = Index::new(&store);
        let key = name_key(&proposal.scope.cell);
        let intent = record::Birth {
            root_digest: self.root_digest,
            scope: crate::application::consensus::protocol::WireScope::from(&proposal.scope),
            voters: proposal
                .voters
                .iter()
                .copied()
                .map(record::Member::encode)
                .collect(),
            learners: proposal
                .learners
                .iter()
                .copied()
                .map(record::Member::encode)
                .collect(),
            empty_digest: record::digest(&[]),
            managed_owner: owner.cloned(),
            cancelled_by: None,
        };
        let bytes = record::encode(record::BIRTH, &intent, self.inner.limit)?;
        let id = record::digest(&bytes);
        for _ in 0..MAX_ATTEMPTS {
            let (mut head, token) = store.current().await?;
            if let Some(value) = index.lookup(head.index, &key).await? {
                let prior = store.intent(pointer(&value)?).await?;
                if prior.scope.cell() != proposal.scope.cell {
                    return Err(CatalogError::Corrupt("birth intent name"));
                }
                return Ok(prior);
            }
            match (&head.mode, owner) {
                (Mode::Open, None) => self.reserve_group_identity(&proposal.scope).await?,
                (Mode::Owned { fleet, catalog }, Some(owner))
                | (Mode::Adopted { fleet, catalog, .. }, Some(owner))
                    if *fleet == owner.fleet && *catalog == owner.catalog =>
                {
                    self.verify_group_identity(&proposal.scope).await?;
                }
                (Mode::Frozen { .. } | Mode::Importing { .. }, _) => {
                    return Err(CatalogError::BirthsFrozen);
                }
                _ => return Err(CatalogError::ManagedAdmissionRequired),
            }
            if self
                .inner
                .create_and_observe(&store.key(&format!("intents/{}", hex::encode(id))), &bytes)
                .await?
                != bytes
            {
                return Err(CatalogError::Corrupt("birth intent collision"));
            }
            head.index = index.update(head.index, &key, Some(id.to_vec()), 0).await?;
            head.count = head.count.checked_add(1).ok_or(CatalogError::Limit)?;
            head.revision = head.revision.checked_add(1).ok_or(CatalogError::Limit)?;
            let result = store.cas(&head, token).await;
            let (observed, _) = store.current().await?;
            if let Some(value) = index.lookup(observed.index, &key).await? {
                let prior = store.intent(pointer(&value)?).await?;
                if prior.scope.cell() != proposal.scope.cell {
                    return Err(CatalogError::Corrupt("birth intent name"));
                }
                return Ok(prior);
            }
            result?;
        }
        Err(CatalogError::Unconfirmed)
    }

    pub(super) async fn verify_birth_intent(
        &self,
        birth: &record::Birth,
    ) -> Result<(), CatalogError> {
        let store = Store(self);
        let (head, _) = store.current().await?;
        let value = Index::new(&store)
            .lookup(head.index, &name_key(birth.scope.cell()))
            .await?
            .ok_or(CatalogError::Corrupt("untracked birth decision"))?;
        let id = pointer(&value)?;
        let mut expected = birth.clone();
        expected.cancelled_by = None;
        if record::digest(&record::encode(record::BIRTH, &expected, self.inner.limit)?) != id {
            return Err(CatalogError::Corrupt(
                "birth decision differs from admitted intent",
            ));
        }
        store.intent(id).await?;
        Ok(())
    }

    /// Stop new birth admissions. Already-admitted writers remain in this exact
    /// frozen set and may finish; reconcile/cancel every pending decision before
    /// treating the set as an import cut. Retry the same operation after loss.
    pub async fn freeze_births(
        &self,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<FrozenBirthInventory, CatalogError> {
        if timeout.is_zero() || timeout > Duration::from_secs(60) {
            return Err(CatalogError::Invalid("birth freeze timeout"));
        }
        self.require_mode(cellar_logic::NamespaceMode::Raft)?;
        if self.config.managed_fleet.is_some() {
            return Err(CatalogError::ManagedAdmissionRequired);
        }
        crate::asyncrt::timeout(timeout, async {
            let store = Store(self);
            for _ in 0..MAX_ATTEMPTS {
                let (mut head, token) = store.current().await?;
                match head.mode {
                    Mode::Frozen { operation: prior } if prior == operation.bytes() => {
                        return Ok(FrozenBirthInventory {
                            owner: self.inner.clone(),
                            root: self.root_digest,
                            operation,
                            revision: head.revision,
                            count: head.count,
                            index: head.index,
                        });
                    }
                    Mode::Frozen { .. } => {
                        return Err(CatalogError::Conflict("namespace already frozen"))
                    }
                    Mode::Open => (),
                    _ => return Err(CatalogError::ManagedAdmissionRequired),
                }
                head.mode = Mode::Frozen {
                    operation: operation.bytes(),
                };
                head.revision = head.revision.checked_add(1).ok_or(CatalogError::Limit)?;
                if let Err(error) = store.cas(&head, token).await {
                    let (observed, _) = store.current().await?;
                    if observed.mode != head.mode {
                        return Err(error);
                    }
                }
            }
            Err(CatalogError::Unconfirmed)
        })
        .await
        .map_err(|_| CatalogError::Unconfirmed)?
    }

    /// Read at most sixteen canonical intents from a fixed frozen root. No
    /// bucket listing or caller-provided manifest can replace this inventory.
    pub async fn frozen_birth_page(
        &self,
        cut: &FrozenBirthInventory,
        after: Option<&BirthInventoryCursor>,
        limit: usize,
        timeout: Duration,
    ) -> Result<BirthInventoryPage, CatalogError> {
        if !Arc::ptr_eq(&cut.owner, &self.inner)
            || cut.root != self.root_digest
            || after
                .is_some_and(|cursor| cursor.root != cut.root || cursor.after.index != cut.index)
        {
            return Err(CatalogError::Conflict("foreign birth inventory cut/cursor"));
        }
        if !(1..=MAX_PAGE).contains(&limit) {
            return Err(CatalogError::Limit);
        }
        if timeout.is_zero() || timeout > Duration::from_secs(60) {
            return Err(CatalogError::Invalid("birth inventory timeout"));
        }
        crate::asyncrt::timeout(timeout, async {
            let store = Store(self);
            let (head, _) = store.current().await?;
            if head.index != cut.index
                || head.count != cut.count
                || head.revision != cut.revision
                || head.mode
                    != (Mode::Frozen {
                        operation: cut.operation.bytes(),
                    })
            {
                return Err(CatalogError::Corrupt("frozen birth inventory changed"));
            }
            let page = Index::new(&store)
                .scan(cut.index, after.map(|cursor| &cursor.after), limit)
                .await?;
            let mut entries = Vec::with_capacity(page.entries.len());
            for entry in page.entries {
                let id = pointer(&entry.value)?;
                let intent = store.intent(id).await?;
                if name_key(intent.scope.cell()) != entry.key {
                    return Err(CatalogError::Corrupt("inventory entry name"));
                }
                entries.push(BirthInventoryEntry {
                    proposal: store.proposal(&intent)?,
                    digest: id,
                });
            }
            Ok(BirthInventoryPage {
                entries,
                next: page.next.map(|after| BirthInventoryCursor {
                    root: cut.root,
                    after,
                }),
            })
        })
        .await
        .map_err(|_| CatalogError::Unconfirmed)?
    }
}
