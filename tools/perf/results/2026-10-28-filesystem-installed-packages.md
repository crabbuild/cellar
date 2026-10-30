<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Integrated filesystem agent packages, 2026-10-28

Fresh installed session, Pi, Vercel and both Python ABI packages pass local
recovery checks with the native host integrated against `origin/main` at
`5998b82cac1b14dbf05b8308b4161144c48252bf`. The source baseline is
`ae1383178c53fc6d13193e0f74c29c546e98a77e`; this checkpoint changes qualification
fixtures, CI and documentation, with no production runtime changes.

The [structured record](2026-10-28-filesystem-installed-packages.json) contains
package manifests, verified native artifact digests, test executable/source/log
hashes, Vercel receipt observations and actual Python ABI profiles. Raw logs,
immutable packages and frozen test executables remain in the checkout-specific
workspace target's `qualification/integrated-packages/` and
`qualification/integrated-python-packages/` directories.

Environment: macOS arm64, Rust 1.98.1, Node 24.13.1 and CPython 3.14.5 GIL and
free-threaded. Cargo uses the unoptimized development profile without debug
information. Independent fixtures run serially. Each package's original runtime
prefix is moved aside before installation, which must restore the verified
archive. Node starts with a cleared environment and no source-entry override;
Python starts without a host site-packages override.

## Observed results

| Installed artifact | Verified behavior | Test elapsed, seconds |
| --- | --- | --- |
| Session filesystem agent | Four isolated sessions, SIGKILL, deleted predecessor data, exact recovered runs, duplicate/conflict handling and follow-up reads of each cell's original notes | 53.978 |
| Pi agent | Eight concurrent production-entry tasks, cold SIGKILL recovery, complete transcripts and 32 unchanged duplicate submissions | 181.078 |
| Vercel agent | Two interrupted file-tool runs, exact edited inode/version/bytes and seven original receipts per cell; a new task, 27 final receipts, 15 saved decisions, verified deletion and twelve unchanged duplicates | 76.354 |
| Python agent, GIL | File-commit and saved-read SIGKILL cuts; empty-node recovery driven by alarms, nine receipts and five model decisions per cut, exact saved reads, stale-edit refusal, deletion, duplicate/conflict handling and joined shutdown | 219.516 |
| Python agent, free-threaded | Same two cuts; actual `free_threaded=true` and `gil_enabled=false` before and after each recovery | 193.701 |

Elapsed times include installation and fixture work; they are not production
latency measurements. Pi's installed test exercises its ordinary increment task;
its filesystem-tool cases retain their separate source-entry evidence. The
session and Python fixtures retain their existing assertions and budgets.

## Installed Vercel boundary

The new [package fixture](../../../crates/cellar-javascript/tests/support/fleet_package_vercel.rs)
runs the captured production entry and installed OpenAI provider adapter. Only
the external model is replaced, by a private loopback HTTP service. No test
application entry, injected filesystem implementation, paid service or real
provider credential participates.

Each of two independent cells writes, reads, lists, searches, edits and renames
its own file, retaining a stale-version refusal. The model's fourth request is
held after the third model decision and seven tool receipts have been proved.
The host is killed; read-only inspection records the edited file and receipts,
then the predecessor's entire local data directory is deleted. The replacement
starts empty and must recover the same inode/version/bytes and all seven exact
receipts, including the read of the original contents before the edit.

The replacement completes both runs and accepts a third task. All three finish
with nine receipts and five saved model decisions. The two interrupted tasks
make model-request counts `[1,1,1,2,1]`; the new task makes `[1,1,1,1,1]`. Only
the interrupted, unsaved decision is requested again. Twelve duplicate submissions
preserve the complete results and these counts. Direct SQLite inspection checks
that final journal receipts match the public results and no linked regular files
remain. The host and provider service join afterward.

An earlier attempt inspected `db.sqlite` only after graceful shutdown and failed
to find all completed task databases. Replication's `finish_remove_local` may
rename or remove live database paths while closing a cell. The final fixture
samples completed resident state before shutdown and verifies shutdown separately.
The failed executable identity and log hash remain in the structured record;
no production behavior, assertion about file deletion or deadline was weakened.
Earlier successful package/receipt checks retain their own executable hashes.

[CI](../../../.github/workflows/javascript-sdk.yml) now builds, restores and runs
this exact ignored case and uploads its JSON report. The example
[delivery guide](../../../examples/vercel-agent/README.md#direct-http-and-delivery)
documents the same command. Strict Node-feature Clippy for the affected test
passes; formatting and layout cover 1,388 Rust files. Native dependency checks
still find 482 packages without a JavaScript engine, and documentation checks
pass. CI registration does not establish a completed remote run.

## Release scope

This closes the earlier missing local installed Vercel profile and refreshes
session/Pi/Python package evidence for the pinned main integration. It does not
qualify optimized deployment artifacts, Linux host behavior, external-store
failure modes, a real model service, or performance/resource SLOs. The remaining
operation/error/authority/resource audit and physical fault/load gates remain in
the [release ledger](../../../docs/design/per-cell-filesystem-implementation-status.md#earlier-release-priorities).
