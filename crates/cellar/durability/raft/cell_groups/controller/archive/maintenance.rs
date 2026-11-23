// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One node-owned predecessor-release lane. Publication for the same group
//! remains serialized; unrelated groups retain the existing publication lane.
use super::*;

pub(in crate::raft::cell_groups::controller) struct Completed {
    group: GroupHistory,
    result: Result<job::Maintenance>,
    observation: std::result::Result<LogIndex, RuntimeError>,
}

impl Manager {
    pub(super) fn launch_maintenance(
        &mut self,
        runtime: &CellGroupRuntime,
        shared: &Shared,
        now: u64,
    ) {
        if self.maintenance_job.is_some() {
            return;
        }
        let active = self.active_group;
        let proof = self.proof_group;
        let entries = &mut self.entries;
        let Some(group) = self.queue.next_maintenance(|group| {
            let Some(entry) = entries.get_mut(&group) else {
                return Eligibility::Remove;
            };
            if !entry.dirty || entry.fatal || entry.stopping {
                entry.queued = false;
                return Eligibility::Remove;
            }
            settlement::prune(&mut entry.waiters, now);
            let quiet = entry
                .latest_applied
                .zip(entry.published_index)
                .is_some_and(|(applied, published)| applied <= published);
            if active == Some(group)
                || proof == Some(group)
                || entry.retry_at > now
                || !entry.maintenance_pending
                || !quiet
                || entry.context.is_none()
                || !entry.waiters.is_empty()
                || shared.foregrounds.blocks(group, now)
            {
                Eligibility::Blocked
            } else {
                Eligibility::Background
            }
        }) else {
            return;
        };
        let entry = self
            .entries
            .get_mut(&group)
            .expect("archive maintenance owner");
        shared.foregrounds.background_started(group);
        entry.queued = false;
        // Consume the previous cohort's one-shot wake. A new waiter arriving
        // during this job can still bypass its completion's retry backoff.
        entry.foreground_wake = false;
        entry.dirty = false;
        let context = entry.context.clone().expect("opened archive maintenance");
        let execution = entry.execution.clone();
        let runtime = runtime.clone();
        // The fixed lane adds no snapshot, tail or SQL admission. Remote work
        // uses the original shared archive I/O bank. Retain async ownership
        // before stop/removal can inspect the group; physical children keep
        // their ordinary independent joins.
        let work = execution.retain();
        self.maintenance_group = Some(group);
        if let Some(status) = shared
            .archives
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&group)
        {
            status.attempts = status.attempts.saturating_add(1);
        }
        self.maintenance_job = Some(Box::pin(async move {
            let _work = work;
            let result =
                std::panic::AssertUnwindSafe(job::maintain(&context, &execution, &runtime))
                    .catch_unwind()
                    .await
                    .unwrap_or(Err(Error::Invalid("archive maintenance panicked")));
            let observation = runtime
                .status(group)
                .await
                .map(|status| status.applied.index);
            Completed {
                group,
                result,
                observation,
            }
        }));
    }

    pub(super) fn complete_maintenance(
        &mut self,
        completed: Completed,
        shared: &Shared,
        now: u64,
        closing: bool,
    ) {
        self.maintenance_job = None;
        self.maintenance_group = None;
        // In particular, do not clear another group's publication job or its
        // admitted verification handoff. This completion supplies no coverage.
        let entry = self
            .entries
            .get_mut(&completed.group)
            .expect("completed archive maintenance");
        let interval = entry
            .registration
            .publisher
            .retry_interval_ms(shared.limits.retry_interval_ms);
        let result = completed.result.and_then(|result| {
            let applied = completed.observation.map_err(Error::Runtime)?;
            let latest = entry
                .latest_applied
                .map_or(applied, |seen| seen.max(applied));
            entry.latest_applied = Some(latest);
            match result {
                job::Maintenance::Quiet {
                    published,
                    retained,
                } => {
                    if entry.published_index != Some(published.position().index) {
                        return Err(Error::Invalid("archive maintenance head changed"));
                    }
                    entry.maintenance_pending = retained;
                    entry.dirty = latest > published.position().index || retained;
                }
                job::Maintenance::PublicationRequired => {
                    entry.maintenance_pending = false;
                    entry.dirty = true;
                }
            }
            Ok(())
        });
        let mut statuses = shared.archives.lock().unwrap_or_else(|e| e.into_inner());
        let status = statuses
            .get_mut(&completed.group)
            .expect("archive maintenance status");
        match result {
            Ok(()) => status.error = None,
            Err(error) => {
                entry.fatal = !retryable(&error);
                entry.dirty = true;
                status.stopped = entry.fatal;
                status.error = Some(Arc::new(error));
            }
        }
        settlement::prune(&mut entry.waiters, now);
        if entry.fatal {
            let error = status.error.clone().expect("failed archive maintenance");
            for request in entry.waiters.drain(..) {
                request.fail(error.clone());
            }
        } else if !entry.waiters.is_empty() {
            entry.dirty = true;
        }
        if entry.dirty {
            entry.retry_at = now.saturating_add(interval);
            if entry.foreground_wake {
                entry.retry_at = 0;
            }
        }
        if !closing && !entry.stopping && !entry.fatal && entry.dirty && !entry.queued {
            entry.queued = true;
            self.queue.enqueue(completed.group);
        }
    }
}
