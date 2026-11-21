<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cellar 0.1.2 release record

Purpose: Record release 0.1.2 sources, archive digests, verification, and qualification limits.

Recorded 2026-11-20 for the requested scoped crates.io maintenance release.
The source is published 0.1.1 plus the reload checkpoint fix from
[PR #176](https://github.com/crabbuild/cellar/pull/176), release metadata,
and the existing repository-only example package selector correction.
Newer main features are outside this release.

## Source and scope

| Item | Identity |
| --- | --- |
| Published baseline | `95f860bcddb1f49dc72453f100154c4eeeb7bfbd` (`v0.1.1`) |
| Checkpoint fix | `590640c7ce49ef3fd391bdae9540ba23081cd74b` |
| Released source | `db71523cbd22ad5c71d09dce97029b07d90abcef` ([v0.1.2](https://github.com/crabbuild/cellar/tree/v0.1.2)) |
| Main integration | `76a4401399aac1390db3f01000583684d88f6f31` ([PR #178](https://github.com/crabbuild/cellar/pull/178)) |
| Local verification platform | Rust/Cargo 1.98.1, `aarch64-apple-darwin` |
| Dependency resolution | Released source's `Cargo.lock`; Cargo builds locked |

A per-cell gate excludes age checkpoints from final reload capture, upload,
and close. The blocking maintenance worker owns its gate until checkpointing
ends, including when its waiting future is cancelled. No public API,
configuration, or storage format changes. Existing invalid remote chains remain
refused; this release prevents new gaps without reconstructing missing data.

The 11 Cellar packages are versioned together at 0.1.2, with exact internal
registry dependency versions. Unchanged `cellar-raft`, `cellar-raft-proto`,
and `cellar-raft-engine` remain pinned at 0.1.1. Registry README links point
to `v0.1.2`; example project selectors retain the published `cellar-main` name.

## Completed verification

- The public-API reload regression passed through Cargo and in five further
  fresh processes on both current main and the maintenance release source.
  The external Cargo harness used the integration-test file directly, avoiding
  unrelated repository example binaries.
- Strict harness Clippy passed with the repository's `clippy.toml` and warnings
  denied. Dependency deprecation warnings remain outside the harness's lint scope.
- Repository formatting, Rust module layout, native dependency, and documentation
  checks passed on the maintenance source. The native core dependency graph
  contains no JavaScript engine.
- Cargo built all 11 default packages from their distributable archives.
  These are default package build checks on the local platform, without
  optional V8 or embedded Python qualification. The packaged CLI reported
  `cellar 0.1.2`.
- The publication dry run reproduced the exact build-verified archive bytes.
  Publication used `--no-verify` after these completed archive builds.
- Every published archive was downloaded and checked against both its registry
  checksum and the verified local bytes. Each records the released source SHA,
  includes LICENSE, NOTICE, and its registry README, and uses registry-only
  runtime/build dependencies. The published host contains the checkpoint gate.

The desktop update interrupted the first package verification session.
Verification resumed to a successful complete 11-package run; archive digests
were unchanged across the interruption.

## Qualification limit and decision

Decision: publish the requested maintenance packages. Broader fleet and
cross-platform qualification is not recorded as passing. PR #176 retains the
matched baseline/fix fleet failures, including the unclassified fixed-variant
`replace before restart handoff: deadline elapsed` result. Passing the targeted
checkpoint regression does not establish that these fleet failures are resolved.

This scoped package publication is not a production deployment go decision.
Apply the [production readiness gates](../21-production-readiness.md) to the
actual workload and topology. This record was committed after publication;
the release tag remains at the exact package source revision listed above.

## Published archive digests

All rows are version 0.1.2. SHA-256 identifies the downloaded `.crate` bytes.

| Package | SHA-256 |
| --- | --- |
| [cellar-admin](https://crates.io/crates/cellar-admin/0.1.2) | `dd1d13ae91e6348443284dabbad6e21f656e415be12600b0fa6fceb23661b0c0` |
| [cellar-axum](https://crates.io/crates/cellar-axum/0.1.2) | `6f7e32fc50a897b7a272c12d04cadefc8bf637dfa009a893c4f54872d6b185b1` |
| [cellar-container](https://crates.io/crates/cellar-container/0.1.2) | `4d73d93f49c4977c5202b81de25e28f75b940bcb0a7bb96bfeee02372a574725` |
| [cellar-egress](https://crates.io/crates/cellar-egress/0.1.2) | `583d892a35b26069b25336b38b75433f9dffae43e04f15a3ce19bba8af8ec903` |
| [cellar-guest](https://crates.io/crates/cellar-guest/0.1.2) | `3aa49e95727f2f6abb09b653cc67b3770bd340e828c4b76068b6cb00c3bcfcac` |
| [cellar-javascript](https://crates.io/crates/cellar-javascript/0.1.2) | `ffbf6a25c9a259d35c2be2deb84cc53c34b51c7f0cf7d34db2764521cba7d70b` |
| [cellar-logic](https://crates.io/crates/cellar-logic/0.1.2) | `ac1a323d7fb80a9b0aefd0a6ddcce4a20a7716d58634484c7b3cb10326a90983` |
| [cellar-ltx](https://crates.io/crates/cellar-ltx/0.1.2) | `aeac8b25eeae391e536d9a1ed3db53b1e289ca6a1ccba0f4db127c3489f9d84d` |
| [cellar-main](https://crates.io/crates/cellar-main/0.1.2) | `013a9409a02d0a0965081f1ea681495292682ea4a0e7331f724910736659e531` |
| [cellar-native](https://crates.io/crates/cellar-native/0.1.2) | `6decc3c9a85c8cf416394e93c31dd3dfd81ec3edbeb12c49cc432410d54dbb9a` |
| [cellar-python](https://crates.io/crates/cellar-python/0.1.2) | `0df493ae8ba2c3daabef6f51ac907a4a47121f4133b84818485b2eed7ad789c1` |
