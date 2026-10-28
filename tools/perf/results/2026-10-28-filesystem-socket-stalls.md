<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Filesystem S3 forwarding-stall qualification, 2026-10-28

The native filesystem recovery scenario passes with TCP connections retained
open while both forwarding directions stop. Three independent proxies cover
replica publication, owner lease access and failed recovery reads. The S3 client
and production filesystem/runtime code are unchanged. The source baseline is
`0e986468da6b3ed3229246a24fc245460cd48bcb` plus the test changes pinned in the
[structured record](2026-10-28-filesystem-socket-stalls.json).

Environment: macOS arm64, Rust 1.98.1, unoptimized development artifacts with
debug information disabled. Each campaign owns a private RustFS container,
limited to two CPUs and 2 GiB, pinned to
`ghcr.io/rustfs/rustfs@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`.
Each S3 scenario has a separate empty bucket. All three campaigns remove their
containers and stores; credentials are generated privately and omitted from
reports. Raw evidence and frozen executables remain in the checkout-specific
target's `qualification/socket-stalls/`, `socket-stalls-repro/` and
`socket-stalls-final/` directories.

## Fault and lifecycle assertions

Stall acknowledgement waits until every admitted forwarding task has parked or
finished. Existing sockets remain owned by parked tasks. New connections park
without connecting upstream. Each proxy permits at most 128 connection tasks;
shutdown aborts and joins them and verifies the active counter is zero. The
separate loopback control transfers data in both directions before the stall,
then requires reads to stay pending on both endpoints and a fresh client. EOF
or a forwarded response fails that control.

The application ledger retains the existing assertions:

1. A proved first file/state/receipt transaction, covering a 196,625-byte payload
   across chunk boundaries, with exact hash, inode and version.
2. A second locally committed transaction whose publication is stalled. Its
   response fails with an unknown outcome; the owner actually fences after
   lease access stalls. The caller executor remains alive.
3. Predecessor local-data deletion. Healthy coordination with stalled replica
   reads must return HTTP 500 and admit no application constructor.
4. A healthy empty successor recovers exactly the first transaction, finds no
   second receipt and refuses old descriptor/context authority.
5. A proved third transaction, successor local-data deletion and another
   recovery matching that transaction exactly. Remaining nodes join fully.

The final unproved response is HTTP 500 with unknown outcome. Failed recovery
also returns HTTP 500, after 30,112 ms. The three application constructions are
the first owner and two healthy successors; failed recovery contributes none.

| Proxy | Forwarded before stall | Parked | Denied | Cancelled and joined | Remaining |
| --- | --- | --- | --- | --- | --- |
| Lease | 7 | 8 | 0 | 8 | 0 |
| Publication | 1 | 1 | 0 | 1 | 0 |
| Recovery | 0 | 2 | 0 | 2 | 0 |

Counts describe connections, not requests or transferred bytes. The extra lease
connection and both recovery connections park without reaching the server.

## Preserved failure and observer correction

The initial fixture reused the other profiles' 15-second HTTP deadline. It
failed twice: the client timed out before the stalled provider read finished.
The instrumented reproduction observed one application constructor, two stalled
recovery connections and an incomplete Handoff drain when shutdown interrupted
that read. The report had `application_work_drained: false`, `cells_drained:
false` and `drain_timed_out: true`, while ingress, local durability and the log
closed. These observations are retained, not classified as graceful success.

Source inspection distinguishes the budgets: `NodeStartupPolicy` bounds
predecessor node-log recovery, while cell restore intentionally retains its
operation through individually bounded provider requests. The S3 configuration
uses 15-second attempts and a 30-second retry window; that window prevents
further retries and does not cancel an already admitted attempt. The old HTTP
observer therefore could not observe the expected provider failure.

Only this new stall profile's failed-recovery observer now allows 60 seconds.
The original HTTP 500, no-constructor, exact-ledger, stale-authority and complete
shutdown assertions remain. Other HTTP observers stay at 15 seconds. No
runtime, provider, ownership or shutdown budget changed. The diagnostic logging
is removed. This passing profile waits for failed recovery to settle before
shutdown; it does not qualify early shutdown of an active stalled restore as
graceful.

## Validation and remaining scope

On the final frozen executable, seven ordinary native filesystem cases pass in
47.738 seconds; four opt-in cases are explicitly ignored. Exact selection then
passes the supplied-store profile in 15.080 seconds, connection cuts in 16.031
seconds and forwarding stalls in 45.898 seconds. These are fixture durations,
not latency or recovery objectives. Strict native-node Clippy and the repository
static checks are recorded with log hashes in the structured record.

The JavaScript SDK workflow registers the exact case serially in another private
bucket and uploads its report. The [run instructions](../filesystem.md#s3-forwarding-stalls)
describe reproduction. Registration does not establish successful remote CI.
Kernel packet loss/delay, peer transport, expired credentials, physical disk
faults, sustained load and optimized deployment artifacts retain separate gates.
