// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A live producer's verified exact input/result, never a commit or serving proof.
use super::images::Preview;
use crate::raft::cell_groups::CellImagePin;
use cellar_logic::{cell_group::GroupHistory, consensus::Term};
use sha2::{Digest, Sha256};
use std::sync::Arc;

struct Binding {
    engine: Arc<()>,
    group: GroupHistory,
    term: Term,
    payload: [u8; 32],
}

impl Binding {
    fn new(engine: Arc<()>, group: GroupHistory, term: Term, bytes: &[u8]) -> Self {
        Self {
            engine,
            group,
            term,
            payload: Sha256::digest(bytes).into(),
        }
    }

    fn accepts(&self, engine: &Arc<()>, group: GroupHistory, term: Term, bytes: &[u8]) -> bool {
        Arc::ptr_eq(engine, &self.engine)
            && self.group == group
            && self.term == term
            && self.payload == <[u8; 32]>::from(Sha256::digest(bytes))
    }
}

/// Only execution can mint this after independent preview replay/verification.
/// Commands and pending proposal records hold weak references. The producer
/// owns the image until a committed apply ticket retains it. Metadata fits the
/// existing 4 KiB envelope; no payload copy or independent cache is retained.
/// The underlying image inventory keeps its original physical admission.
pub(crate) struct CellPreparedImage {
    binding: Binding,
    image: CellImagePin,
}

impl CellPreparedImage {
    pub(super) fn new(
        engine: Arc<()>,
        group: GroupHistory,
        term: Term,
        bytes: &[u8],
        preview: &Preview,
    ) -> Arc<Self> {
        let image = match preview {
            Preview::Materialized(pin) => CellImagePin::Materialized(pin.clone()),
            Preview::Rooted(pin) => CellImagePin::Extents(pin.clone()),
        };
        Arc::new(Self {
            binding: Binding::new(engine, group, term, bytes),
            image,
        })
    }

    pub(crate) fn accepts(
        &self,
        engine: &Arc<()>,
        group: GroupHistory,
        term: Term,
        bytes: &[u8],
    ) -> bool {
        self.binding.accepts(engine, group, term, bytes)
    }

    pub(crate) fn image(&self) -> &CellImagePin {
        &self.image
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};

    fn group() -> GroupHistory {
        GroupHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        }
    }

    #[test]
    fn preparation_requires_the_same_live_owner_history_and_term() {
        let owner = Arc::new(());
        let history = group();
        let term = Term::new(1);
        let binding = Binding::new(owner.clone(), history, term, b"complete encoded transition");
        assert!(binding.accepts(
            &owner.clone(),
            history,
            term,
            b"complete encoded transition"
        ));
        assert!(!binding.accepts(&Arc::new(()), history, term, b"complete encoded transition"));
        assert!(!binding.accepts(
            &owner,
            history,
            Term::new(2),
            b"complete encoded transition"
        ));
        for changed in [
            GroupHistory {
                cluster: ClusterId::new([9; 16]).unwrap(),
                ..history
            },
            GroupHistory {
                namespace: NamespaceId::new([9; 16]).unwrap(),
                ..history
            },
            GroupHistory {
                group: GroupId::new([9; 16]).unwrap(),
                ..history
            },
            GroupHistory {
                incarnation: IncarnationId::new([9; 16]).unwrap(),
                ..history
            },
        ] {
            assert!(!binding.accepts(&owner, changed, term, b"complete encoded transition"));
        }
    }

    #[test]
    fn preparation_fingerprint_covers_every_supplied_input_byte() {
        let owner = Arc::new(());
        let history = group();
        let term = Term::new(1);
        let bytes = b"complete encoded transition";
        let binding = Binding::new(owner.clone(), history, term, bytes);
        for index in 0..bytes.len() {
            let mut changed = bytes.to_vec();
            changed[index] ^= 1;
            assert!(!binding.accepts(&owner, history, term, &changed));
        }
        assert!(!binding.accepts(&owner, history, term, &bytes[..bytes.len() - 1]));
    }

    #[test]
    fn preparation_metadata_fits_the_existing_envelope_allowance() {
        // Pinned image data/metadata remain owned by their already admitted
        // inventory. This record adds fixed metadata and reference counts only.
        assert!(std::mem::size_of::<CellPreparedImage>() + 2 * std::mem::size_of::<usize>() <= 512);
    }
}
