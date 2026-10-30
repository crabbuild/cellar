<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Native filesystem dense capacity, 2026-10-27

All three native SDK profiles passed: 10 MiB, 100 MiB and 1 GiB of actual dense
payloads, bounded edits, receipt replay, empty-local-state bucket recovery, a
proven new-owner append, tail reclamation and two complete joined shutdowns.
This is functional capacity evidence for the captured profile. Production
latency, hard memory bounds and the full capacity/fault matrix remain open.

The [raw observations](2026-10-27-filesystem-capacity.json) contain the complete
profiles, executable/source hashes and log hashes. The
[harness guide](../filesystem.md) documents commands, assertions and metric scope.

## Artifact and environment

- Source baseline: `d3206c924031a3f2ca3387b542c02877bb91423d`, with uncommitted filesystem
  implementation. Eight compiled source hashes in the JSON match the current
  fixture, filesystem backend and replica storage adapter.
- All three independent processes used executable SHA-256
  `d51e0f10251d811f70ad6b3892fd7905107ee3529c0d658491fd3c54d97d5f93`.
- macOS arm64, 32 GiB physical memory, Rust 1.98.1. Unoptimized test profile,
  debug information disabled. The shared development host had other build
  activity; these measurements do not establish a latency SLO.
- A private SQLite development bucket per process. Fixed 64 KiB file chunks,
  one-MiB transactions/transfers, 2 GiB file/allocation ceilings, default logical
  and transaction-work limits. Explicit 120-second recovery/handler budgets;
  the native fixture's three-second ordinary operation/adoption window remains.
- Every write commits its digest receipt and progress with the file effect.
  Acknowledgement uses the configured bucket proof. Cold recovery has a new node
  identity after the predecessor's local directory is deleted. Every block,
  inode/version, progress and receipt is compared before the new append.

## Observed measurements

Write elapsed covers sequential creation of the initial payload. Request latency
includes routing, server-side hashing and the proof gate; range reads return a
digest rather than a one-MiB HTTP body. Cold admission includes node startup.

| Dense payload | Write elapsed, s | Write p99, ms | Warm read p99, ms | First cold read, s | New-owner append, ms | Sampled process RSS peak, MiB |
| --- | --- | --- | --- | --- | --- | --- |
| 10 MiB | 1.060 | 322.67 | 6.45 | 0.379 | 256.00 | 150.8 |
| 100 MiB | 6.141 | 197.68 | 13.05 | 1.855 | 2219.69 | 789.6 |
| 1024 MiB | 58.193 | 98.11 | 8.45 | 3.142 | 44.29 | 1446.7 |

RSS includes the driver, both node lifetimes, SQLite, replication and the
in-process development store. Samples are 10 ms apart. The larger profile uses
paged recovery at the configured 256 MiB threshold, so its behavior differs from
the smaller eager-restored profiles. The observations do not prove a hard peak
or a per-cell memory bound.

| Dense payload | Sampled node file lengths peak, MiB | Sampled WAL lengths peak, MiB | Current retained bucket objects, MiB |
| --- | --- | --- | --- |
| 10 MiB | 32.9 | 4.22 | 48.8 |
| 100 MiB | 214.9 | 4.27 | 211.8 |
| 1024 MiB | 2105.9 | 4.30 | 1069.3 |

File lengths are sampled every 16 writes and at the last write. They include
local replica files and are not allocated disk blocks or exact transient peaks.
Bucket sizes describe retained objects after both shutdowns, including history;
they are not network bytes transferred. The raw JSON also separates `.ltx` sizes.

## Recovery defect exposed and fixed

The initial GiB cold open failed with SQLite I/O error. The default generic
paged-read helper runs on its own runtime; the development store requires a
Cellar execution-domain owner while polling metadata and body reads. The small
`default_paged_reader_retains_its_owner_and_refuses_after_owner_closure` regression
reproduced that failure. The host now wraps those exact supplied-client range
reads in the activation's captured owner and refuses them after its closure.
Native S3 and caller-supplied page-reader factories retain their contracts.

The regression passed after the fix, as did all six native replication cases
and strict Clippy for the library and capacity harness. The GiB rerun then passed
all content, state, receipt, append, reclamation and shutdown assertions. Its log
still records one final-durability retry during predecessor shutdown; the final
joined shutdown proof remained mandatory. Raw logs retain bucket fallback and
shutdown observations rather than interpreting them as successful fleet proof.

The SDK workflow is configured to execute and upload the 10 MiB case in its ABI
jobs. This report does not claim a completed remote CI run, an external object
store, peer-fault qualification, guest overhead, alternative chunk sizes,
small-file density, many-cell fairness or final installed-package qualification.
Those gates remain in the [implementation status](../../../docs/design/per-cell-filesystem-implementation-status.md).
