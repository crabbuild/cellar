<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Native filesystem example qualification, 2026-10-28

The [Rust notebook](../../../examples/filesystem/README.md) closes the missing
native application example in FS-02/05. Each cell owns a file, business count
and deduplication receipts. Exact `tx.fs()` appends commit with the count and
receipt; `context.fs().sync()` requests the configured proof before a successful
response. The example bounds entries, UTF-8 bytes and receipt growth.

Source baseline: `ec521d6154763998809c10d9502e033d25fcd7d0`, plus the example,
smoke and CI changes identified by the [structured record](2026-10-28-filesystem-native-example.json).
No production filesystem implementation or limits changed. Raw evidence lives
in the checkout-specific target's `qualification/linux-arm64-release/` directory.

## Application assertions

The focused smoke runs through the real CLI, native package validation and
HTTP routes. It requires:

- Empty state, UTF-8 byte counts, identical duplicate receipts and conflicting-ID
  refusal without another append.
- Eight concurrent appends, concurrent replays, exact resulting text/count and
  stable inode identity.
- Exact 4,096-byte acceptance, empty/oversized/malformed input refusals and no
  change to the prior notebook after refusal.
- Independent cell files and receipt IDs. A 128-entry notebook refuses a new ID
  while retaining identical replay of an existing ID.
- A preserving restart with identical file bytes, count, metadata and receipts,
  followed by a new append under the successor activation.
- Graceful joined shutdown of both hosts. Each fixture owns temporary state;
  concurrent requests occur inside that fixture, not across independent runs.

The combined cookbook additionally exercises the notebook through its
asset-routing configuration and checks retained file/count after restart.
The catalog validation checks the focused configuration, registered factory and
manifest. This is graceful-restart application evidence; the native fault
fixtures retain their separate crash, empty-local-state and external-store scope.

## Artifact selection and CI

The focused driver accepts `--profile release` to change the staged application's
Cargo profile. Selecting only a release CLI would otherwise leave the example
on its documented development default. The new SDK CI job builds and exercises
both release executables on the release workflow's three targets: Linux x86-64,
Linux arm64 and macOS arm64. It records the source, toolchain, target and binary
hashes, requires unchanged binary identities after restart, and uploads build
and smoke logs even when a step fails.

Both the development and release focused smokes pass on macOS arm64 with Rust
1.98.1. The unchanged release profile builds successfully in 10 minutes
15 seconds; its focused run passes in 3.577 seconds with both binary hashes
unchanged through restart. The final focused development run passes in 28.767
seconds, and the combined development cookbook also passes. These times include
fixture startup, Cargo/package work and shutdown under different build-cache
conditions; they are not comparable benchmarks or latency/recovery objectives.
The runtime infrastructure in both profiles is the local development host/store.

The catalog test, strict Clippy for the native cookbook target, format/layout
checks over 1,390 Rust files, and the 482-package native dependency guard pass.
The workflow parses, its target matrix matches the release workflow, and both
inline metadata/check snippets parse. Documentation validation is included in
the structured record. These local checks do not establish a successful remote
CI run or qualify other language runtime packages.

## Linux release build failure

The Linux arm64 attempt uses the unchanged release profile: size optimization,
fat LTO, one codegen unit, panic abort and stripped symbols. Cargo has one build
job and incremental compilation disabled. The Rust 1.98.1 Bookworm image is
pinned to `rust@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e`.

The local Colima VM provides two CPUs and 2,053,578,752 bytes of RAM. Its mounts
do not expose the host Workspace volume; two attempted bind starts failed
before compilation. The actual build uses a private workspace volume owned only
by this checkout and a `git archive` of the baseline. Dependencies compile, then
the `cellar` library compiler is killed by SIGKILL. Docker records
`OOMKilled: true`, exit 101. The build starts at 11:50:38 UTC and ends at
11:54:31 UTC on 2026-10-28.

No application executable or Linux runtime qualification is produced. The VM
settings and release optimization profile were not relaxed. The retained build
log, image/source identity and container state distinguish this resource failure
from a filesystem behavior failure. Successful builds and application runs on
the supported Linux targets remain required.

## Remaining release scope

The recipe and selected application checks do not complete FS-04/05. Complete
the operation/error/authority matrix, remaining transport/credential/disk faults,
sustained reclamation and fairness, supported runtime/ABI artifacts, and measured
deployment capacity with approved SLO/RPO/RTO. See the
[release ledger](../../../plans/per-cell-filesystem-implementation-status.md#release-priorities).
FUSE and additional capability profiles remain separate work.
