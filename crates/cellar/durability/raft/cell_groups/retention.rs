// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical retention around engine publication. An uncertain write keeps both
//! closures and disables collection until a new owner recovers durable markers.

use super::{GroupManifest, ManifestError, ManifestPins, PreparedGroupManifest};
use crate::raft::cache::GroupLog;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard,
    },
};

type GroupKey = [[u8; 16]; 4];

struct Root {
    marker: GroupManifest,
    _pins: ManifestPins,
}

struct Group {
    engine: Arc<()>,
    closed: Arc<AtomicBool>,
    current: Option<Root>,
    pending: Option<Root>,
    checkpoint: Option<Root>,
    expected_checkpoint: Option<GroupManifest>,
    pending_checkpoint: bool,
}

#[derive(Default)]
struct State {
    groups: BTreeMap<GroupKey, Group>,
    retained: Vec<ManifestPins>,
    reconciled: bool,
    quarantined: bool,
}

pub(super) struct RootRetentions {
    state: Mutex<State>,
    max_roots: usize,
}

fn key(marker: &GroupManifest) -> GroupKey {
    let h = marker.route().history;
    [
        h.cluster.bytes(),
        h.namespace.bytes(),
        h.group.bytes(),
        h.incarnation.bytes(),
    ]
}

impl RootRetentions {
    pub(super) fn new(max_roots: usize) -> Self {
        Self {
            state: Mutex::new(State::default()),
            max_roots,
        }
    }

    fn state(&self) -> Result<MutexGuard<'_, State>, ManifestError> {
        let state = self.state.lock().map_err(|_| ManifestError::Quarantined)?;
        if state.quarantined {
            return Err(ManifestError::Quarantined);
        }
        Ok(state)
    }

    fn used_roots(state: &State) -> usize {
        state.groups.len() * 2
            + state.retained.len()
            + state
                .groups
                .values()
                .filter(|g| g.checkpoint.is_some() || g.pending_checkpoint)
                .count()
    }

    fn admit_group(&self, state: &State, key: GroupKey) -> Result<(), ManifestError> {
        // Reserve current + pending slots before a group can submit a write.
        if !state.groups.contains_key(&key) && Self::used_roots(state) + 2 > self.max_roots {
            return Err(ManifestError::Limit);
        }
        Ok(())
    }

    pub(super) fn recover<S>(
        &self,
        group: &GroupLog<S>,
        marker: &GroupManifest,
        pins: &ManifestPins,
    ) -> Result<(), ManifestError> {
        if group.closed.load(Ordering::Acquire) {
            return Err(ManifestError::Quarantined);
        }
        let expected_checkpoint = group
            .checkpoint_manifest()
            .map_err(|_| ManifestError::Invalid("closed engine"))?;
        let mut state = self.state()?;
        let key = key(marker);
        self.admit_group(&state, key)?;
        let selected = state.groups.entry(key).or_insert_with(|| Group {
            engine: group.owner.clone(),
            closed: group.closed.clone(),
            current: None,
            pending: None,
            checkpoint: None,
            expected_checkpoint,
            pending_checkpoint: false,
        });
        if !Arc::ptr_eq(&selected.engine, &group.owner)
            || selected.pending.is_some()
            || selected
                .current
                .as_ref()
                .is_some_and(|r| r.marker != *marker)
        {
            return Err(ManifestError::Conflict);
        }
        if selected.current.is_none() {
            selected.current = Some(Root {
                marker: marker.clone(),
                _pins: pins.clone(),
            });
        }
        Ok(())
    }

    pub(super) fn recover_checkpoint<S>(
        &self,
        group: &GroupLog<S>,
        marker: &GroupManifest,
        pins: &ManifestPins,
    ) -> Result<(), ManifestError> {
        let mut state = self.state()?;
        let key = key(marker);
        let selected = state
            .groups
            .get(&key)
            .ok_or(ManifestError::Invalid("recover applied root first"))?;
        if !Arc::ptr_eq(&selected.engine, &group.owner)
            || selected.pending.is_some()
            || selected.closed.load(Ordering::Acquire)
            || selected.expected_checkpoint.as_ref() != Some(marker)
            || selected
                .checkpoint
                .as_ref()
                .is_some_and(|r| r.marker != *marker)
        {
            return Err(ManifestError::Conflict);
        }
        if selected.checkpoint.is_none() {
            if Self::used_roots(&state) >= self.max_roots {
                return Err(ManifestError::Limit);
            }
            state
                .groups
                .get_mut(&key)
                .ok_or(ManifestError::Conflict)?
                .checkpoint = Some(Root {
                marker: marker.clone(),
                _pins: pins.clone(),
            });
        }
        Ok(())
    }

    pub(super) fn begin<S>(
        self: &Arc<Self>,
        group: &GroupLog<S>,
        prepared: &PreparedGroupManifest,
        previous: Option<&GroupManifest>,
        checkpoint: bool,
    ) -> Result<PublicationPin, ManifestError> {
        if group.closed.load(Ordering::Acquire) {
            return Err(ManifestError::Quarantined);
        }
        let mut state = self.state()?;
        let key = key(prepared.marker());
        self.admit_group(&state, key)?;
        if !state.groups.contains_key(&key) && previous.is_some() {
            return Err(ManifestError::Invalid(
                "selected root has not been recovered",
            ));
        }
        if checkpoint
            && state
                .groups
                .get(&key)
                .is_none_or(|g| g.checkpoint.is_none())
            && Self::used_roots(&state) + usize::from(!state.groups.contains_key(&key)) * 2
                >= self.max_roots
        {
            return Err(ManifestError::Limit);
        }
        let selected = state.groups.entry(key).or_insert_with(|| Group {
            engine: group.owner.clone(),
            closed: group.closed.clone(),
            current: None,
            pending: None,
            checkpoint: None,
            expected_checkpoint: None,
            pending_checkpoint: false,
        });
        if !Arc::ptr_eq(&selected.engine, &group.owner)
            || selected.pending.is_some()
            || selected.current.as_ref().map(|r| &r.marker) != previous
        {
            return Err(ManifestError::Conflict);
        }
        selected.pending_checkpoint = checkpoint;
        selected.pending = Some(Root {
            marker: prepared.marker().clone(),
            _pins: prepared.images.clone(),
        });
        Ok(PublicationPin {
            owner: self.clone(),
            key,
            checkpoint,
            armed: false,
            finished: false,
        })
    }

    pub(super) fn require_checkpoint<S>(&self, group: &GroupLog<S>) -> Result<(), ManifestError> {
        let expected = group
            .checkpoint_manifest()
            .map_err(|_| ManifestError::Invalid("closed engine"))?;
        let Some(expected) = expected else {
            return Ok(());
        };
        let state = self.state()?;
        let selected = state
            .groups
            .get(&key(&expected))
            .ok_or(ManifestError::Invalid("checkpoint not recovered"))?;
        if !Arc::ptr_eq(&selected.engine, &group.owner)
            || selected.checkpoint.as_ref().map(|r| &r.marker) != Some(&expected)
        {
            return Err(ManifestError::Invalid("checkpoint not recovered"));
        }
        Ok(())
    }

    pub(super) fn reconcile(&self, retained: Vec<ManifestPins>) -> Result<(), ManifestError> {
        let mut state = self.state()?;
        if state
            .groups
            .values()
            .any(|g| g.closed.load(Ordering::Acquire))
        {
            return Err(ManifestError::Quarantined);
        }
        if state.groups.values().any(|g| g.pending.is_some()) {
            return Err(ManifestError::Conflict);
        }
        if Self::used_roots(&state) - state.retained.len() + retained.len() > self.max_roots {
            return Err(ManifestError::Limit);
        }
        state.retained = retained;
        state.reconciled = true;
        Ok(())
    }

    pub(super) fn can_reclaim(&self) -> Result<(), ManifestError> {
        let state = self.state()?;
        if state
            .groups
            .values()
            .any(|g| g.closed.load(Ordering::Acquire))
        {
            return Err(ManifestError::Quarantined);
        }
        if state
            .groups
            .values()
            .any(|g| g.checkpoint.as_ref().map(|r| &r.marker) != g.expected_checkpoint.as_ref())
        {
            return Err(ManifestError::Invalid("checkpoint not recovered"));
        }
        if !state.reconciled {
            return Err(ManifestError::Invalid("root inventory not reconciled"));
        }
        if state.groups.values().any(|g| g.pending.is_some()) {
            return Err(ManifestError::Conflict);
        }
        Ok(())
    }
}

pub(crate) struct PublicationPin {
    owner: Arc<RootRetentions>,
    key: GroupKey,
    checkpoint: bool,
    armed: bool,
    finished: bool,
}

impl PublicationPin {
    pub(crate) fn arm(&mut self) {
        self.armed = true;
    }

    pub(crate) fn commit(mut self) -> Result<(), ManifestError> {
        {
            let mut state = self.owner.state()?;
            let group = state
                .groups
                .get_mut(&self.key)
                .ok_or(ManifestError::Conflict)?;
            let pending = group.pending.take().ok_or(ManifestError::Conflict)?;
            if self.checkpoint {
                group.expected_checkpoint = Some(pending.marker.clone());
                group.checkpoint = Some(Root {
                    marker: pending.marker.clone(),
                    _pins: pending._pins.clone(),
                });
            }
            group.pending_checkpoint = false;
            group.current = Some(pending);
        }
        self.finished = true;
        Ok(())
    }
}

impl Drop for PublicationPin {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut state = self.owner.state.lock().unwrap_or_else(|e| e.into_inner());
        if self.armed {
            // Do not release either old or new image/page pins after an unknown
            // engine outcome, including unwinding between sync and publication.
            state.quarantined = true;
        } else if let Some(group) = state.groups.get_mut(&self.key) {
            group.pending = None;
            group.pending_checkpoint = false;
            if group.current.is_none() {
                state.groups.remove(&self.key);
            }
        }
    }
}
