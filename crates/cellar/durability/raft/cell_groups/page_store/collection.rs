// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical container release; logical page removal alone earns no quota credit.

use super::*;
use std::ops::Bound::{Excluded, Unbounded};

impl ManifestPageStore {
    /// Inspect at most `scan_limit` physical containers and relocate at most 96
    /// live records in total. A copy must fit alongside its
    /// source in the existing quotas. Live leases retain their logical identity.
    /// Only completed copy/unlink/directory barriers release physical credit.
    pub fn reclaim(&mut self, scan_limit: usize) -> Result<ManifestReclaimReport, ManifestError> {
        self.reclaim_with_compaction(scan_limit, MAX_BATCH_PAGES)
    }

    pub(crate) fn reclaim_with_compaction(
        &mut self,
        scan_limit: usize,
        max_compaction_pages: usize,
    ) -> Result<ManifestReclaimReport, ManifestError> {
        self.require_live()?;
        if !(1..=4096).contains(&scan_limit)
            || max_compaction_pages > MAX_BATCH_PAGES
            || self.directory_batch.is_some()
        {
            return Err(ManifestError::Limit);
        }
        self.retention.can_reclaim()?;
        let mut report = ManifestReclaimReport::default();
        let mut removed_pages = 0;
        let mut removed_bytes = 0;
        for _ in 0..scan_limit {
            let start = self.collection_cursor.map_or(Unbounded, Excluded);
            let Some((id, source)) = self
                .packs
                .range((start, Unbounded))
                .next()
                .map(|(id, pack)| (*id, pack.clone()))
            else {
                self.collection_cursor = None;
                break;
            };
            self.collection_cursor = Some(id);
            report.scanned += 1;
            // The complete small record index is already resident. Filesystem
            // reads have a shared record budget and use one page buffer.
            let live: Vec<_> = source
                .records
                .iter()
                .copied()
                .filter(|record| {
                    self.inventory.get(&record.key).is_some_and(|entry| {
                        Arc::ptr_eq(&entry.pack, &source) && Arc::strong_count(&entry.lease) > 1
                    })
                })
                .collect();
            if live.len() == source.records.len() {
                continue;
            }
            let mut copied_pages = 0;
            let mut copied_bytes = 0;
            if !live.is_empty() {
                if live.len() > max_compaction_pages.saturating_sub(report.relocated_pages) {
                    continue;
                }
                let records = relocated(&live);
                let end = records.last().unwrap().end();
                let target_id = pack::identity(&records, end);
                let target = if let Some(existing) = self.packs.get(&target_id).cloned() {
                    // An interrupted prior compaction can leave an identical
                    // completed destination. Validate its actual selected bytes
                    // and complete fresh owner barriers before releasing source.
                    if existing.records != records {
                        return Err(ManifestError::Invalid("page pack identity collision"));
                    }
                    for record in &records {
                        self.read_pack(&existing, *record)?;
                    }
                    self.quarantined = true;
                    self.filesystem.open(&existing.path)?.sync_all()?;
                    self.filesystem.sync_all(&self.directory)?;
                    existing.persisted.store(true, Ordering::Release);
                    existing
                } else {
                    let length = end + FOOTER_BYTES;
                    if self.capacity(records.len(), length).is_err() {
                        report.blocked_compactions += 1;
                        continue;
                    }
                    // Charge the copy before creating it; never borrow the old
                    // file's quota until its deletion barrier actually completes.
                    self.charge(records.len(), length, 1)?;
                    self.quarantined = true;
                    let path = self
                        .directory
                        .join(format!("{}.pending", hex::encode(records[0].key)));
                    let mut file = self.filesystem.create_new(&path)?;
                    file.write_all(pack::MAGIC)?;
                    for (old, new) in live.iter().zip(&records) {
                        let bytes = self.read_pack(&source, *old)?;
                        file.write_all(&new.header())?;
                        file.write_all(&bytes)?;
                        self.usage.written_pages += 1;
                    }
                    copied_pages = records.len();
                    copied_bytes = length;
                    let opened = self.finish_pack(Pending {
                        path,
                        file,
                        records,
                        data_end: end,
                    })?;
                    self.filesystem.sync_all(&self.directory)?;
                    opened.persisted.store(true, Ordering::Release);
                    self.packs.insert(opened.id, opened.clone());
                    opened
                };
                for record in &target.records {
                    let entry = self
                        .inventory
                        .get_mut(&record.key)
                        .ok_or(ManifestError::Invalid("missing relocated page"))?;
                    // Keep the original lease: existing roots/readers continue
                    // to protect exactly this logical page after relocation.
                    entry.pack = target.clone();
                    entry.record = *record;
                }
                report.compactions += 1;
                report.relocated_pages += live.len();
            }
            self.quarantined = true;
            self.filesystem.remove_file(&source.path)?;
            for record in &source.records {
                if self
                    .inventory
                    .get(&record.key)
                    .is_some_and(|entry| Arc::ptr_eq(&entry.pack, &source))
                {
                    self.inventory.remove(&record.key);
                }
            }
            self.packs.remove(&id);
            // Stage only metadata removal. Every old allocation stays charged
            // until the shared final directory barrier, including on unwind.
            removed_pages += source.records.len();
            removed_bytes += source.length;
            report.pages += source.records.len() - copied_pages;
            report.files += 1;
            report.bytes += source.length - copied_bytes;
        }
        if report.files != 0 {
            self.filesystem.sync_all(&self.directory)?;
            self.usage.pages -= removed_pages;
            self.usage.files -= report.files;
            self.usage.bytes -= removed_bytes;
            self.quarantined = false;
        }
        Ok(report)
    }
}

fn relocated(live: &[Record]) -> Vec<Record> {
    let mut end = HEADER_BYTES;
    live.iter()
        .map(|record| {
            let next = Record {
                offset: end,
                ..*record
            };
            end = next.end();
            next
        })
        .collect()
}
