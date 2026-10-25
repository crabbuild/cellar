//! Immutable current admission cut for inventory verification. Capturing this
//! cut does not freeze new births or grant maintenance/removal authority.
use super::*;
use crate::raft::catalog::ManagedBirthOwner;

pub(crate) struct ManagedBirthInventory {
    owner: Arc<Inner>,
    root: [u8; 32],
    pub(crate) count: u64,
    index: Option<[u8; 32]>,
}

impl NamespaceCatalog {
    pub(crate) async fn managed_birth_inventory(
        &self,
        owner: &ManagedBirthOwner,
    ) -> Result<ManagedBirthInventory, CatalogError> {
        let (head, _) = Store(self).current().await?;
        if !matches!(head.mode, Mode::Owned { fleet, catalog } | Mode::Adopted { fleet, catalog, .. }
            if fleet == owner.fleet && catalog == owner.catalog)
        {
            return Err(CatalogError::ManagedAdmissionRequired);
        }
        Ok(ManagedBirthInventory {
            owner: self.inner.clone(),
            root: self.root_digest,
            count: head.count,
            index: head.index,
        })
    }

    pub(crate) async fn managed_birth_page(
        &self,
        cut: &ManagedBirthInventory,
        after: Option<&BirthInventoryCursor>,
    ) -> Result<BirthInventoryPage, CatalogError> {
        if !Arc::ptr_eq(&cut.owner, &self.inner)
            || cut.root != self.root_digest
            || after.is_some_and(|c| c.root != cut.root || c.after.index != cut.index)
        {
            return Err(CatalogError::Conflict("foreign managed inventory cut"));
        }
        let store = Store(self);
        let page = Index::new(&store)
            .scan(cut.index, after.map(|c| &c.after), MAX_PAGE)
            .await?;
        let mut entries = Vec::with_capacity(page.entries.len());
        for entry in page.entries {
            let id = pointer(&entry.value)?;
            let intent = store.intent(id).await?;
            if name_key(intent.scope.cell()) != entry.key {
                return Err(CatalogError::Corrupt("managed inventory entry"));
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
    }
}
