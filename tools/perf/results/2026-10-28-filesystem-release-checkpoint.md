<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Filesystem recovery and package checkpoint, 2026-10-28

Selected native S3 fault, agent recovery and installed-package checks pass on
macOS arm64 with Rust 1.98.1 and Node 24.13.1. These artifacts incorporate the
Python cancellation correction at `542e5e89`. They precede the subsequent
`5998b82c` main integration; fresh qualification of that integration is separate.
The [structured checkpoint](2026-10-28-filesystem-release-checkpoint.json)
retains executable/package/log/report identities and scoped results.

## S3-backed native fault recovery

The native fixture runs against a private RustFS container using the pinned image
`ghcr.io/rustfs/rustfs@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`,
with two CPUs and a 2 GiB container limit. The native Cellar host remains on
macOS; this does not qualify the Linux host target or independent failure zones.

The S3 case passes the same assertions as the refreshed development-store case:

- Generation one is proven as an atomic file/state/receipt transaction.
- Replica publication is refused before a second local commit; independent
  owner-store refusal causes actual lease-expiry fencing. The client receives no
  successful acknowledgement of generation two.
- Old local data is removed. Unreadable replica admission returns the root
  handler's expected 500 without constructing application code.
- A healthy owner restores generation one, refuses stale capabilities, proves
  generation three and joins. Another empty-local-state recovery matches the
  generation-three receipt and joins too.

The S3 run records eleven publication refusals, 45 lease-store refusals and two
recovery-read refusals. The frozen executable SHA-256 is
`749d1166dca5c3f204ebb97dc0accd85fb269179fe6ba619ef39b6e8638bec7e`.
Fault gates wrap the real S3 client. They do not establish socket-timeout,
physical network-partition, credential-expiry or disk-stall behavior.

## Agent recovery and delivery

The Vercel source-entry S3 drill passes six SIGKILL cuts, then recovers sixteen
concurrent production-entry tasks and handles 32 unchanged duplicates. The tool
drill passes external effects, filesystem mutation and saved-read interruption.
Both filesystem cuts recover nine receipts across two attempts without changing
the duplicated result. S3 object inspection verifies actual remote LTX objects;
the replacement hosts start with empty local storage.

Fresh Node captures are staged for the session, Pi and Vercel examples. SDK bytes
match the repository. Original serving-runtime prefixes are moved aside before
the installed-package tests, so installation must restore published archives.

| Installed artifact | Observed checks |
| --- | --- |
| Session agent | Four isolated sessions, SIGKILL/empty-node recovery, retained files/receipts and follow-up reads; joined host |
| Pi agent | Eight production-entry tasks, HTTP validation/conflicts, SIGKILL recovery and 32 unchanged duplicates; joined host |
| Python 3.14.5 GIL | File-commit and saved-read cuts, nine receipts/five decisions per case, cold recovery and joined hosts; actual GIL remains enabled |
| Python 3.14.5 free-threaded | Same two cuts and recovery assertions; actual GIL remains disabled |

The Pi installed case exercises its ordinary task flow. Its filesystem source
fixtures retain their separate scope. The Vercel installed capture is staged;
its S3 tool drill uses the source entry and is not an installed-package claim.

All forty native-proof Node/V8 cases and 24 Node lost-byte-reply cases also pass
again using recovered executables whose hashes match the prior recorded
artifacts. Their 64 recaptured reports have stopped bridges, zero active
invocations and zero retained bytes. Source equivalence was checked before
recapture; later main changes do not silently qualify those frozen executables.

## Failures found by the drills

The pinned RustFS image rejects the fixture's generated 32-character access key
with InvalidAccessKeyId; a 20-character key succeeds. CI now generates that
working profile. The saved-read fixture also generated a 65-character bucket
name, exceeding the accepted limit. A regression test fails on that name before
the correction. Bounded scenario text and a compact UUID preserve isolation
within the bucket-name limit. All 38 Vercel unit tests pass after correction,
then the complete S3 tool drill passes on a fresh private store.

Failed attempts remain separate from passing results. Every private container
and generated credential file used by these drills is removed. New raw evidence
is retained outside the ignored workspace directory, in the checkout target's
`qualification/` directory. No credentials are included in the committed report.

Production readiness remains open in the
[release ledger](../../../plans/per-cell-filesystem-implementation-status.md).
Supported-target CI, final integrated artifacts, the remaining contract/fault
audit and a deployment workload/resource envelope retain their explicit gates.
