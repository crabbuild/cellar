# Committed archive and output boundary

MR-09 is complete with [source-pinned evidence](evidence/MR-09.json). Its node-owned dispatcher now
connects archive publication to command output. MR-10 still owns atomic Raft
snapshot installation and authorized replacement-member recovery; the original
17-package delivery goal remains open.

## Authority and proofs

Each registered member has an archive capability bound to its immutable
namespace/birth/schema. The node runs one archive producer at a time, rotating
across eligible cells. A SQL worker captures an immutable published generation
and its matching durable engine cut; network and physical verification run
outside the private command connection. Captured log bytes have an independent
budget, so a command waiting for S3 cannot consume the capacity needed to archive
it. Query-only ingress creates neither a replay workspace nor a cell image.

The publisher checks every referenced object and physically replays the complete
snapshot/tail set into its separate verification store before conditionally
publishing the head. Exact reads reconcile lost object/head responses. Its
completion covers the captured local cut, even when another replica has already
archived a later cut of the same history. It never imports that replica's applied
position. The pure core receives coherent archived progress and, for an original
command, an Archived proof naming the same command identity, index/term and
entry digest already proven Committed, Applied and Queryable.

Quorum output requires commit and durable image publication. S3 output also
requires the archive proof. A namespace minimum strengthens request options and
is recorded in the atomic command receipt. Duplicate lookup compares the
operation/contract/input, recovers the stored result from a committed image and
waits for the stronger requested or stored minimum without executing or proposing
the business command again. A verified duplicate makes cancellation ambiguous
even though this retry submitted nothing. Stronger retries do not rewrite the
minimum originally recorded with an already committed transaction.

S3 does not elect leaders, grant votes or bypass a missing majority. A restored
cell image is not an activated Raft member. Per-cell archive heads also do not
establish a global application checkpoint. Blob/effect capabilities remain
unsupported until MR-12 enumerates their complete committed dependency closure;
the archive format and verifier already accept explicit immutable dependencies.

## Capacity and lifetime

Defaults allow one node-wide producer, 4 MiB of captured log bytes, 128 MiB per
object, 256 MiB per recovery set, 32 tail bundles, 1,024 tail entries, 128
dependencies and 256 KiB manifests. Object-store operations have a 10-second
deadline, including the complete streamed read; failed attempts retry after
100 ms. The timer stays bound to the originating execution domain. Shutdown
cancels object-store waits and joins running blocking work before releasing
the verification workspace and cell directories.

New business callbacks reserve a conservative maximum image/envelope allocation
against a 256-MiB aggregate unarchived-byte admission cap and 4,096 data-bearing
entries. Protocol entries also consume the byte cap. Queued/admitted callbacks
retain their reservation until their producer finishes. Duplicate lookup remains
available when new work is refused. These are command-admission limits, not a
claim that total process or disk usage is 256 MiB: incoming Raft traffic and
images additionally have the transport/log-engine/per-store hard limits. MR-13
still owns broader operational watermarks and reporting.

The host validates compatible execution/archive/apply limits and requires at
least three image generations and capacity for four maximum-size image copies.
A private preview, current base and replacement may coexist while preparation
reserves two copies of its peak. Older archive pins can temporarily backpressure
an already committed apply. That apply retains its original runtime admission
through the blocking effect and retry owner, retries after capacity is reclaimed,
and does not report a resource shortage as corruption or acknowledge early.

After a new engine image marker is durable, its complete image can establish a
new full archive cut. The older generation may be retired; existing query/archive
pins still retain its bytes. Startup first adopts the engine-selected marker,
then validates the complete retained reference set before retiring other
inventory. The replay workspace contains verification copies only and can reclaim
its recovered inventory under its own exclusive lock. It never contains the only
acknowledged application copy. Quorum shutdown does not authorize discarding
replica disks while archive coverage is incomplete.

`Bucket::raft_store` supplies the bucket prefix and conditional-write client
without automatic CAS retries. The catalog and archive still reconcile exact
keys themselves. The counter example uses this bridge with its durable local
development bucket; the SDK can inject the same existing cloud bucket adapter.

## Qualification scope

The owning archive target contains the seven named MR-09 requirements. Native
Node/SDK cases cover S3-only image and receipt restoration after deleting producer
storage, minimum durability, stronger retries and restart, outage backpressure,
ambiguous timeout, retention, hung I/O shutdown, bounded I/O retries and resumed
apply under archive pin pressure. Three real TLS-connected voters cover minority
refusal, leader replacement, reconciliation after healing and isolated archive
replay. A small three-generation fixture exercises repeated commands and restart.
The prefix adapter is exercised through the durable local conditional backend.

Direct durable-marker fixtures independently cover speculative exclusion,
concurrent/delayed heads, missing/corrupt dependencies, lost conditional-write
responses, wrong birth/marker refusal, full snapshots and physical tails,
configuration checkpoints and failed replay before head publication. Lower-level
image tests cover complete reference validation and retained readers.

These tests do not qualify physical S3, power loss, zones, production performance,
automatic member repair, committed effects or a full release. Those remain their
original later packages and qualification gates.

Final qualification pins source `2298fd442f0b867695b564527a66b61684aff2c2`:
26 archive cases and 225 other Rust cases pass. Axum feature runs pass 27/33
cases including the compile doctest, and the real executable passes its native/
Axum restart drill. All 23 recorded commands pass. The six ignored process
child entry points are invoked by their owning parents and excluded from the
251-case Rust count. The raw output, command arguments, executable digests and
limits are retained in the evidence; MR-10 through MR-17 remain unfinished.
