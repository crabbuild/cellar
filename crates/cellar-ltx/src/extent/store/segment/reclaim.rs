// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Conservative whole-segment GC. Mixed live/dead segments retain their full
//! physical charge. No tombstones, relocation, or reader retargeting is needed.
use super::*;

impl NodeExtentStore {
    pub(in super::super) fn reclaim_segments(&self, limit: usize) -> Result<usize, StoreError> {
        let segments = self.inner.segments.as_ref().ok_or(StoreError::Layout)?;
        let roots = {
            let state = self.inner.state()?;
            state
                .roots
                .iter()
                .map(|(root, entry)| {
                    Ok((
                        ExtentRootReference::new(*root, entry.bytes as u32)?,
                        entry.location.ok_or(StoreError::Integrity)?.segment,
                        entry.retained
                            || Arc::strong_count(&entry.token) > 1
                            || state
                                .references
                                .values()
                                .any(|v| v.active && v.reference.root == *root),
                    ))
                })
                .collect::<Result<Vec<_>, StoreError>>()?
        };
        // Build segment dependencies from every physically retained root,
        // including retired roots sharing a segment with pinned live bytes.
        let mut edges = BTreeMap::<u64, BTreeSet<u64>>::new();
        let mut live = BTreeSet::new();
        for (reference, source, retained) in roots {
            if retained {
                live.insert(source);
            }
            let data = self.read_root_file(reference)?;
            let state = self.inner.state()?;
            for object in &data.objects {
                if let Some(entry) = state.objects.get(object) {
                    edges
                        .entry(source)
                        .or_default()
                        .insert(entry.location.ok_or(StoreError::Integrity)?.segment);
                }
            }
        }
        loop {
            let before = live.len();
            for (source, targets) in &edges {
                if live.contains(source) {
                    live.extend(targets);
                }
            }
            if live.len() == before {
                break;
            }
        }
        let candidates = segments
            .state()?
            .files
            .keys()
            .filter(|id| !live.contains(id))
            .copied()
            .collect::<BTreeSet<_>>();
        let mut incoming = BTreeMap::<u64, BTreeSet<u64>>::new();
        for (source, targets) in &edges {
            for target in targets {
                if candidates.contains(target) && candidates.contains(source) {
                    incoming.entry(*target).or_default().insert(*source);
                }
            }
        }
        let mut selected = BTreeSet::new();
        // Select each candidate with every still-present referring segment.
        // Newer root-only segments can be removed before large old data sets;
        // only an actual dependency cycle larger than the budget must wait.
        for id in candidates.iter().rev() {
            let remaining = limit - selected.len();
            if remaining == 0 {
                break;
            }
            if selected.contains(id) {
                continue;
            }
            let mut cohort = BTreeSet::new();
            let mut pending = BTreeSet::from([*id]);
            while let Some(next) = pending.pop_last() {
                if selected.contains(&next) || !cohort.insert(next) {
                    continue;
                }
                if cohort.len() > remaining {
                    break;
                }
                if let Some(parents) = incoming.get(&next) {
                    pending.extend(
                        parents
                            .iter()
                            .filter(|parent| {
                                !cohort.contains(*parent) && !selected.contains(*parent)
                            })
                            .copied(),
                    );
                }
            }
            if cohort.len() <= remaining {
                selected.extend(cohort);
            }
        }
        if selected.is_empty() {
            return Ok(0);
        }
        let result = (|| {
            for id in &selected {
                self.inner
                    .filesystem
                    .remove_file(&self.inner.directory.join(name(*id)))?;
            }
            self.inner.sync_directory_cleanup()
        })();
        if let Err(error) = result {
            self.inner.quarantine();
            return Err(error);
        }
        let mut physical = segments.state()?;
        let mut inventory = self.inner.state()?;
        inventory.objects.retain(|_, entry| {
            !entry
                .location
                .is_some_and(|v| selected.contains(&v.segment))
        });
        inventory.roots.retain(|_, entry| {
            !entry
                .location
                .is_some_and(|v| selected.contains(&v.segment))
        });
        inventory.root_extents = inventory.roots.values().map(|v| v.slots).sum();
        for id in &selected {
            let segment = physical.files.remove(id).ok_or(StoreError::Missing)?;
            inventory.bytes = inventory
                .bytes
                .checked_sub(segment.length)
                .ok_or(StoreError::Integrity)?;
            if physical.current == Some(*id) {
                physical.current = None;
            }
        }
        Ok(selected.len())
    }
}
