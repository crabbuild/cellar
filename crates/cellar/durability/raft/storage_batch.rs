// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Coalesce already-queued disjoint groups on the existing engine/root fence.
//! No fill timer, additional worker, or per-group execution owner is introduced.

use super::*;
use std::ops::Range;

pub(super) struct Cells {
    pub operation: OperationId,
    pub writes: Vec<GroupWrite<GroupHistory>>,
    pub reply: Reply<SyncReceipt<GroupHistory>>,
    pub permit: Permit,
}

struct Completion {
    operation: OperationId,
    range: Range<usize>,
    reply: Reply<SyncReceipt<GroupHistory>>,
    _permit: Permit,
}

pub(super) struct Outcome {
    pub deferred: Option<Request>,
    pub fatal: Option<LogError>,
}

impl Worker {
    pub(super) fn persist_cell_requests(
        &mut self,
        first: Cells,
        receiver: &mpsc::Receiver<Request>,
    ) -> Outcome {
        let mut keys: BTreeSet<_> = first.writes.iter().map(|write| write.group.key).collect();
        let mut bytes = first.permit.bytes;
        let mut requests = vec![first];
        let mut deferred = None;
        // Empty writes retain their original standalone fence semantics. The
        // queue's physical item permits also cover this worker-owned cohort.
        while !keys.is_empty() && requests.len() <= self.options.queue_capacity {
            let request = match receiver.try_recv() {
                Ok(request) => request,
                Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => break,
            };
            match request {
                Request {
                    queued,
                    work: Work::PersistCells(operation, more, reply),
                    _permit: permit,
                } if !more.is_empty()
                    && bytes
                        .checked_add(permit.bytes)
                        .is_some_and(|total| total <= self.options.max_batch_bytes)
                    && keys
                        .len()
                        .checked_add(more.len())
                        .is_some_and(|groups| groups <= self.options.max_groups)
                    && more.iter().all(|write| !keys.contains(&write.group.key)) =>
                {
                    queued.finish(true);
                    bytes += permit.bytes;
                    keys.extend(more.iter().map(|write| write.group.key));
                    requests.push(Cells {
                        operation,
                        writes: more,
                        reply,
                        permit,
                    });
                }
                other => {
                    // Never skip a same-group write, a lifecycle/cleanup
                    // operation, or a byte-limited request to find more work.
                    deferred = Some(other);
                    break;
                }
            }
        }

        let count = requests.len();
        let mut writes = Vec::new();
        let mut completions = Vec::with_capacity(count);
        for request in requests {
            let begin = writes.len();
            writes.extend(request.writes);
            completions.push(Completion {
                operation: request.operation,
                range: begin..writes.len(),
                reply: request.reply,
                _permit: request.permit,
            });
        }
        let combined = self.measured_cell_persist(completions[0].operation, &writes);
        let mut results = Vec::with_capacity(count);
        let mut fatal = None;
        match combined {
            Ok(receipt) => {
                let sequence = receipt.sequence;
                let mut groups = receipt.groups.into_iter();
                for completion in &completions {
                    results.push(Ok(SyncReceipt {
                        operation: completion.operation,
                        sequence,
                        groups: groups.by_ref().take(completion.range.len()).collect(),
                    }));
                }
            }
            Err(LogError::Invalid(_) | LogError::WrongIdentity | LogError::Overloaded)
                if count > 1 =>
            {
                // These persist errors are exclusively before fence/arming.
                // Isolate invalid or aggregate-admission failures without
                // reissuing any write after an uncertain physical outcome.
                for completion in &completions {
                    let result = if fatal.is_some() {
                        Err(LogError::Closed)
                    } else {
                        self.measured_cell_persist(
                            completion.operation,
                            &writes[completion.range.clone()],
                        )
                    };
                    if let Some(error) = result.as_ref().err().filter(|error| error.fatal()) {
                        fatal = Some(error.clone());
                        self.closed.store(true, Ordering::Release);
                    }
                    results.push(result);
                }
            }
            Err(error) => {
                if error.fatal() {
                    fatal = Some(error.clone());
                    self.closed.store(true, Ordering::Release);
                }
                results.extend((0..count).map(|_| Err(error.clone())));
            }
        }
        // Every original payload/root owner is gone before any source-byte or
        // item permit is refunded, even after fallback or waiter abandonment.
        drop(writes);
        for (completion, result) in completions.into_iter().zip(results) {
            let _ = completion.reply.send(result);
        }
        Outcome { deferred, fatal }
    }

    fn measured_cell_persist(
        &mut self,
        operation: OperationId,
        writes: &[GroupWrite<GroupHistory>],
    ) -> Result<SyncReceipt<GroupHistory>, LogError> {
        let timer = self.performance.timer(Measured::EnginePersist);
        let result = self.persist(operation, writes);
        timer.finish(result.is_ok());
        result
    }
}

#[cfg(test)]
#[path = "storage_batch_tests.rs"]
mod tests;
