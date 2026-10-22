//! Retained history refuses new work; temporary reservations defer callbacks.
use super::{Driver, Phase};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Decision {
    Granted,
    Deferred,
    Refused,
}

impl Driver {
    pub(super) fn archive_admission(&self) -> Decision {
        let reservation = self
            .options
            .execution
            .max_image_bytes
            .saturating_add(self.options.execution.envelope.max_payload_bytes as u64)
            .saturating_add(self.options.execution.envelope.max_header_bytes as u64)
            .saturating_add(1024);
        let mut bytes = reservation;
        let mut entries = 1u64;
        let mut reserved_bytes = 0u64;
        let mut reserved_entries = 0u64;
        for cell in self.cells.values() {
            let Ok(cache) = cell.group.read() else {
                return Decision::Refused;
            };
            let after = cell.archived.get().max(1);
            if cache.applied.position.index > cell.archived {
                if let Some(image) = &cache.applied.image {
                    bytes = bytes.saturating_add(image.length);
                }
            }
            for (_, entry) in cache.entries.range(after.saturating_add(1)..) {
                bytes =
                    bytes.saturating_add(u64::from(protobuf::Message::compute_size(entry)) + 128);
                if entry.get_entry_type() == ::raft::prelude::EntryType::EntryNormal
                    && !entry.data.is_empty()
                {
                    entries = entries.saturating_add(1);
                }
            }
            if let Some(pending) = cell.pending.as_ref().filter(|pending| pending.allow_new) {
                reserved_bytes =
                    reserved_bytes.saturating_add(pending.archive_bytes.unwrap_or(reservation));
                reserved_entries = reserved_entries.saturating_add(1);
            }
            if bytes > self.options.archive.max_unarchived_bytes
                || entries > self.options.archive.max_unarchived_entries as u64
            {
                // Still execute duplicate lookup, but never a new callback.
                return Decision::Refused;
            }
        }
        if bytes.saturating_add(reserved_bytes) <= self.options.archive.max_unarchived_bytes
            && entries.saturating_add(reserved_entries)
                <= self.options.archive.max_unarchived_entries as u64
        {
            Decision::Granted
        } else {
            Decision::Deferred
        }
    }

    pub(super) fn pump_admission(&mut self) {
        let now = crate::asyncrt::mono_ms();
        if self.admission_waiters.is_empty() || now < self.admission_at {
            return;
        }
        self.admission_at = now.saturating_add(self.options.poll_ms);
        // These waiters retain the existing bounded request/input permits.
        // They acquire no execution reservation and run no callback until
        // granted. FIFO order and a bounded turn avoid starving other work.
        for _ in 0..4 {
            let Some(scope) = self.admission_waiters.pop_front() else {
                break;
            };
            let Some(pending) = self
                .cells
                .get(&scope)
                .and_then(|cell| cell.pending.as_ref())
            else {
                continue;
            };
            if pending.phase != Phase::AwaitAdmission || pending.request.control.cancelled() {
                continue;
            }
            let decision = self.archive_admission();
            if decision == Decision::Deferred {
                self.admission_waiters.push_front(scope);
                break;
            }
            if let Some(pending) = self
                .cells
                .get_mut(&scope)
                .and_then(|cell| cell.pending.as_mut())
            {
                pending.allow_new = decision == Decision::Granted;
                pending.phase = Phase::Start;
            }
        }
    }
}
