<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Filesystem S3 connection-cut qualification, 2026-10-28

The native file/state/receipt recovery scenario passes with actual TCP connections
cut beneath the unmodified S3 client. The source baseline is
`d16514866cc7ac963bd06145c5e20b30c3b563cc` plus the fixture changes pinned in the
[structured record](2026-10-28-filesystem-socket-faults.json). Production runtime
code is unchanged. The existing supplied-store scenario also passes on the same
fresh executable.

Environment: macOS arm64, Rust 1.98.1, unoptimized development builds with debug
information disabled. Each campaign uses a disposable local RustFS container,
limited to two CPUs and 2 GiB, pinned to
`ghcr.io/rustfs/rustfs@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`.
Each S3 scenario has its own empty private bucket. Test credentials are generated
in the supervisor's environment and never included in the report. Both campaigns
remove their containers and private stores. Raw logs, manifests and frozen
executables remain in the checkout-specific target's `qualification/socket-faults/`
and `qualification/socket-faults-final/` directories.

## Fault boundary and ledger

Three bounded loopback TCP proxies independently control replica publication,
owner lease access and failed recovery reads. The publication cut closes existing
pooled connections and joins their forwarding tasks before accepting the second
application mutation. New connection attempts are accepted then closed without
forwarding bytes. Lease access remains healthy until that local commit and a
failed publication are observed; the lease proxy is then cut too.

The test requires:

1. A proved first file/state/receipt transaction with matching SHA-256, inode and
   content version. The generated payload is 196,625 bytes, crossing three
   64 KiB chunk boundaries.
2. A locally committed second transaction with different content/version, a
   failed client response recorded as unknown, and actual lease-expiry fencing.
3. Predecessor local-data deletion. A node with healthy coordination but a cut
   replica path must refuse recovery without constructing the application.
4. An empty healthy successor that recovers exactly the first acknowledged
   transaction, finds no second receipt and refuses the old descriptor/context.
5. A proved third file/state/receipt transaction, successor local-data deletion,
   and another empty-node recovery matching that new receipt exactly.
6. Complete shutdown of the remaining nodes, closed predecessor services,
   preserved caller executor and joined proxy servers with no remaining tasks.

The final caller observed a transport failure; the initial campaign observed
HTTP 500. Both remain unknown outcomes in the client ledger. Failed recovery
returned HTTP 500 through the application's existing root-handler contract.
The three application constructions were the original owner and two healthy
recoveries; the failed reader admitted none. The structured record retains exact
bytes commitments and identity/version observations for all three generations.

## Transport observations

| Proxy | Forwarded connections | Cancelled and joined connections | New connections denied | Remaining connections |
| --- | --- | --- | --- | --- |
| Lease | 7 | 7 | 21 | 0 |
| Publication | 1 | 1 | 7 | 0 |
| Recovery | 0 | 0 | 6 | 0 |

The first two proxies must have forwarded and cancelled existing connections;
all three must deny subsequent connection attempts. Counts describe connections,
not object-store operations or transferred bytes. A separate control verifies
bidirectional forwarding, EOF on both sides after a cut, rejection of a fresh
connection and joined cleanup. Each proxy admits at most 128 connection tasks.

## Validation and limits

The final executable passes six ordinary native filesystem cases, including the
proxy control, in 47.400 seconds. Three opt-in cases are explicitly ignored in
that run. Exact selection then executes the supplied-store S3 case in 15.047
seconds and the TCP-cut case in 15.959 seconds. These times include fixture work;
they are not recovery SLOs. The existing node, request, proof and shutdown budgets
and assertions are unchanged.

The first runtime campaign passed, but strict Clippy rejected a raw Tokio select
macro in the proxy. The final source uses Cellar's biased selection macro with
an explicit cut-before-accept reason. Both runtime campaigns and the initial
Clippy failure are retained; no assertion or deadline was relaxed. Final strict
Clippy for the native node target, formatting, layout, native dependency and
documentation checks pass.

The JavaScript SDK workflow now runs the exact TCP-cut case serially in a fresh
private bucket and uploads its report. The [run instructions](../filesystem.md#s3-connection-cuts)
cover local reproduction. Workflow registration does not prove a remote run.
This is connection-closure evidence against a local S3-compatible server; packet
blackholes/delay, peer transport cuts, credential expiry, host disk failures,
sustained load and optimized deployment artifacts remain separate release gates.
