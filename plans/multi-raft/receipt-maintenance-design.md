# Bounded receipt maintenance

Status: implementation in progress. The production CLI component is tracked
separately; this document does not mark MR-13 complete. A new SQLite regression
first locks the inclusive retry deadline before changing pruning behavior.

## Contract

Command receipts remain recoverable through their persisted `retry_until_ms`,
including the deadline itself. Deletion requires `now > retry_until_ms`. A client
sequence at or below its durable expired floor returns `CommandExpired` and
cannot execute again. Floors and client identities are never reclaimed to admit
another identity.

Automatic maintenance preserves each client's latest receipt. This protects
receiver acknowledgements while their source may retry after a lost response.
Once a newer sequence commits, an older receipt can leave only as part of a
contiguous expired prefix. A later expired receipt does not permit crossing an
earlier unexpired receipt. This policy is deliberately conservative for ordinary
business clients as well as framework alarm, outbox and inbox clients.

The existing verification-only `stage_prune` primitive is not an operational
scheduler. Operational pruning accompanies a new command's replicated update:
receipt deletions, sequence floors, business state and the new receipt share one
private SQLite transaction and one LTX publication. Callback failure, rejected
proposal, lost authority or an uncommitted private image cannot publish cleanup.
A submitted response loss retains the usual command ambiguity contract.

## Bounded admission path

`ExecutionLimits` captures an explicit optional maintenance policy. Its bounded
client and row scan limits apply on the owning synchronous SQL worker. The
planner activates near the configured receipt ceiling; it retains at most one
prefix description per scanned client, rather than materializing receipt bodies.

A private executor retains a cyclic scan cursor across command turns and image
rebuilds. The cursor is scheduling metadata and grants no authority. Restart or
leadership change can reset it safely. Scanning advances the cursor even if no
expired prefix is found, so stable leaders eventually inspect cold clients.
Every candidate is checked against the current client floor, last sequence and
persisted retry deadlines in the private transaction before deletion.

Duplicate lookup and command identity/sequence validation precede maintenance.
A duplicate can recover its existing receipt under capacity pressure. A new
command can enter only when bounded, safe cleanup leaves a slot for its receipt;
otherwise it receives `Overloaded` before its business callback. Capacity cannot
shorten retention or delete a client's floor. Configurations whose latest
receipts consume the entire ceiling continue to refuse new work safely.

No extra maintenance client or result receipt is required. The framework does
not add a system operation to an application's immutable schema manifest, invoke
business code on followers, create a new consensus history, or change format
markers. Followers, snapshots and S3 archives carry the ordinary committed LTX
image, including the same receipt floors.

## Required oracles

1. At the exact retry deadline pruning refuses; one millisecond later the
   existing explicit primitive can advance its floor. Duplicate recovery at the
   deadline returns the original result.
2. Non-monotonic receipt deadlines cannot produce a hole. Latest receipts remain
   available, including a signed receiver acknowledgement whose response was
   lost and whose source retries after generic receipt retention elapsed.
3. Each maintenance turn respects client and row scan limits; a cyclic cursor
   reaches eligible cold clients despite unexpired earlier clients.
4. Full live retention refuses before callbacks. Expired contiguous prefixes
   permit new commands without increasing the configured retained-row ceiling.
5. Failed callbacks and uncommitted/rejected proposals leave the published rows
   and floors unchanged. Cancellation and leadership loss preserve ambiguity and
   physical ownership until joined.
6. Replicas, retained-disk restart and archive recovery preserve committed floors,
   reject old sequence replay and continue the exact next sequence. Include
   unsigned 64-bit sequence boundaries.
7. Alarm and outbox recurrence crosses the former receipt ceiling with declared
   retry windows and bounded resources. Held/lost receiver acknowledgements do
   not repeat a business effect or discard required reconciliation state.

Physical S3, deployment fencing, density, performance and release evidence remain
separate qualification gates. This component does not prune live archive objects
or durable node-management receipts.
