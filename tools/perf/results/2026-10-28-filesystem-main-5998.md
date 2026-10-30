<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Filesystem main integration, 2026-10-28

Fresh selected checks pass after integrating `origin/main` at
`5998b82cac1b14dbf05b8308b4161144c48252bf` in merge
`d22523af51324a387d687cac90eb422d8d73a998`. This includes the newer native/guest
RPC response ownership and sandbox backup-recovery changes. The
[structured observations](2026-10-28-filesystem-main-5998.json) retain exact
executable/log identities, capacity profiles, Python source hashes and the S3
file/state/receipt ledger.

Environment: macOS arm64, Rust 1.98.1, Node 24.13.1 and CPython 3.14.5 GIL and
free-threaded runtimes. Development builds omit debug information. The private
RustFS service runs in the local Linux VM; native host behavior remains macOS.

## Accepted checks

| Boundary | Observed result |
| --- | --- |
| Native filesystem | 21 library, three SDK and five serial node/recovery cases pass; opt-in capacity and S3 cases run separately |
| Guest/RPC integration | 36 guest units and both response-ownership handoff/cancellation cases pass |
| JavaScript | 88 library, six Node/V8 filesystem, two synchronous and one transport-authority cases pass |
| Selected JavaScript faults | Node and V8 fsync/fence/Handoff, plus Node read_exact lost-reply/fence/Handoff pass with separate reports |
| Python ABIs | Each passes 31 library tests, four filesystem modes and six selected bridge scenarios |
| Python cancellation | Twelve real-byte-call cases and twelve local-reopen checks per ABI retain exact confirmed progress and receipts |
| Native S3 faults | Fresh integrated executable passes publication refusal, owner fencing, failed-recovery admission, stale-capability refusal and two empty-local-state recoveries |
| Dense capacity | Independent 10 MiB, 100 MiB and 1 GiB processes pass all content/state/receipt, edit/replay, recovery, append, reclamation and shutdown assertions |
| Repository checks | Strict all-target Clippy for cellar/admin/guest/JavaScript/Python with combined guest features; formatting/layout for 1,387 Rust files; 128 JavaScript sources need no formatting; native dependency graph has 482 packages and no JavaScript engine |

Each Python ABI's selected bridge set covers fsync deadline, fsync fence/Handoff,
proof transport failure before fence, generic proof failure/recovery and queue
lost acknowledgement. The earlier full proof matrices retain their own artifact
scope. This checkpoint does not claim to rerun every earlier case on the newer
executable.

The first integrated JavaScript and GIL supervisors overlapped by mistake. Their
passing observations are preserved outside the accepted serial profile. One
replacement supervisor reran the complete selected JavaScript/guest and GIL
matrices serially. Those accepted reports supersede the overlapping observations.
The free-threaded matrix began after the initial JavaScript process had joined
and did not overlap another native fixture. No test assertion or deadline was
changed to obtain these results.

The native S3 case uses a new private bucket and the pinned RustFS image from the
[earlier checkpoint](2026-10-28-filesystem-release-checkpoint.md), with two CPUs
and a 2 GiB container limit. All remaining nodes join; the store and generated
credential file are removed. This uses supplied-client fault gates over actual
S3 traffic, rather than a physical network or disk partition.

## Functional capacity observations

All three processes use the same fresh native executable, fixed 64 KiB chunks,
one-MiB bounded transactions, explicit 2 GiB file/allocation limits and the
documented 120-second handler/recovery profile. The
[capacity guide](../filesystem.md) defines the workload and measurement scope.
These are development-bucket results on a shared host.

| Dense payload | Initial write elapsed, s | First cold admission, s | Sampled process RSS peak, MiB |
| --- | --- | --- | --- |
| 10 MiB | 0.992 | 0.394 | 165.8 |
| 100 MiB | 5.571 | 2.690 | 1031.0 |
| 1 GiB | 55.105 | 2.319 | 2019.2 |

RSS includes the supervisor, both node lifetimes, SQLite, replication and the
in-process development store. It is sampled, not a hard peak or per-cell memory
bound. The 1 GiB profile uses paged recovery; smaller profiles restore eagerly.
These observations cannot establish a memory regression or production SLO without
a controlled comparison. They also cannot justify a 1 GiB process-memory limit
for a 1 GiB file. Production memory, amplification and latency qualification
remains open.

## Remaining release disposition

The initial API profile is implemented. Production qualification remains open
in the [release ledger](../../../docs/design/per-cell-filesystem-implementation-status.md):
the remaining operation/error/lifetime/resource audit, physical peer/network/disk
and sustained-GC profiles, supported-target CI, final integrated installed
artifacts, and a workload/resource envelope with explicit SLO/RPO/RTO.

The completed installed session/Pi/Python and Vercel S3 source-entry checks retain
their earlier executable scope. The newer integration requires its own final
installed artifacts. Queued CI runs are not accepted as passing Linux evidence.
FUSE and additional links/locks/content tiers remain independent of the initial
primitive release.
