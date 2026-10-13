# Native packaging, deployment and rollback

A native application executable contains its factories. Build creates a
verified package; deploy publishes it; install downloads/verifies a new
executable; a process supervisor activates it. Publishing alone does not change
which executable is running across a fleet.

![Interactive deployment flow](diagrams/deployment-flow.svg)

## Project and artifact contract

The project JSON selects `cargo_manifest`, `package`, `binary`, `profile`,
optional `target`/`features`, a `primary` application and optional `cohosted`
applications. Paths resolve relative to the project file. Application records
include script/version, cells, bindings and optional compatibility settings,
cron, Queue consumers, assets and containers. See the
[counter project](../examples/native-counter-project.json) for a runnable shape.

Build uses locked Cargo dependencies with explicitly selected features and
rejects JavaScript engine dependencies in the application's normal/build graph.
Native artifacts carry target, SHA-256 and byte length; package protocol also
validates executable format/host protocol. Installation verifies downloaded
bytes and refuses to overwrite an existing executable. Preserve immutable
artifacts and the corresponding package manifest for rollback.

```sh
cellar build examples/native-counter-project.json --output counter-package
cellar deploy counter-package --bucket YOUR_BUCKET --region YOUR_REGION
cellar install counter-package --bucket YOUR_BUCKET \
  --region YOUR_REGION --output ./counter-v1
```

Use an installed CLI or the `cargo run -p cellar --bin cellar --locked --`
prefix. Custom stores can use `--endpoint URL`. These are publication
instructions for an operator-approved environment, not actions executed during
documentation work. Supply credentials through supported providers.

## Proposed promotion procedure

1. Record the source SHA, locked dependency set, target and release profile.
   Run CI, verify the package and publish a candidate to an isolated environment.
2. Exercise state compatibility, workflows, peer protocol, recovery and complete
   stop reports for that executable. Record the actual image/digest tested.
3. Approve the [release checklist](templates/release-checklist.md). Keep the
   previous package/executable and rollback-compatible state available.
4. Start one candidate node with distinct local storage and correct logical
   identity semantics. Wait for the real readiness gate and business probes.
5. Advance gradually while inspecting error/latency, durability, drain reports
   and pending work. Handoff each retiring node; do not remove surviving recovery
   evidence to make a rollout appear complete.
6. Verify new writes and restoration after takeover. Record the outcome and
   promote the same verified artifacts through environments.

This is a release practice, not a supplied Kubernetes/Helm controller. Platform
teams own the orchestrator, image construction, secrets, mounts and grace periods.
The generic Docker image contains the CLI; application images must copy their
compiled executable into a compatible runtime and use it as their entrypoint.

## Compatibility and rollback

Compiled versions, manifests, persisted protocol, cell identities, LTX, schema
and running workflow steps are coupled contracts. Prefer additive changes and
explicit expand/contract migrations. A failed `Node::adopt` factory keeps the
current generation serving; successful adoption does not erase schema risk.

Stop rollout on rising ambiguous commands, proof failures, fencing, incomplete
drains or failed business probes. Revert to the retained executable only when
its data/protocol compatibility is demonstrated. Otherwise contain traffic and
apply a forward correction or restore plan. Use [operations](17-operations.md)
and [recovery](18-recovery-backup.md).

Sources: [builder](../crates/cellar/cli/native_deploy/build.rs),
[packaging](../crates/cellar/cli/native_deploy/mod.rs),
[protocol](../crates/cellar/deployment/protocol.rs).
