# Native packaging, deployment and rollback

Package your compiled application, publish it to object storage, then install
and start its executable through your process supervisor. Each step has a
separate role: publishing a package does not replace running fleet processes.
This guide covers artifacts, rollout and rollback compatibility.

![Interactive deployment flow](diagrams/deployment-flow.svg)

## Project and artifact contract

The project JSON selects `cargo_manifest`, `package`, `binary`, `profile`,
optional `target`/`features`, a `primary` application and optional `cohosted`
applications. Paths resolve relative to the project file. Application records
include script/version, cells, bindings and optional compatibility settings,
cron, Queue consumers, assets and containers. See the
[counter project](../examples/native-counter/project.json) for a runnable shape.

Build uses locked Cargo dependencies with explicitly selected features and
rejects JavaScript engine dependencies in the application's normal/build graph.
Native artifacts carry target, SHA-256 and byte length; package protocol also
validates executable format/host protocol. Installation verifies downloaded
bytes and refuses to overwrite an existing executable. Preserve immutable
artifacts and the corresponding package manifest for rollback.

```sh
cellar build examples/native-counter/project.json --output counter-package
cellar deploy counter-package --bucket your_bucket --region your_region
cellar install counter-package --bucket your_bucket \
  --region your_region --output ./counter-v1
```

Use an installed CLI or the `cargo run -p cellar --bin cellar --locked --`
prefix. Replace `your_bucket` and `your_region` with your deployment values.
Custom stores can use `--endpoint URL`. Supply credentials through supported
providers.

The optional [Python adapter](../crates/cellar-python/README.md) embeds Python
source and its SDK facade in the executable, so those bytes participate in native
artifact verification. A `python_environment` declaration also transports the
verified libpython, standard library, packages and bundled native libraries.
Build pairs the runtime inventory with the executable; install restores its
immutable prefix before publishing the host. System libraries outside that
closure remain OS prerequisites. Changing Python source, registrations or
inventory requires rebuilding and replacing the process. See the
[environment workflow](../tools/python/README.md).

## Running workers and upgrading code

Cellar supplies the worker runtime and fleet coordination inside each application
executable: cells, SQLite state, routing, ownership, replication, alarms and
shutdown. The CLI builds and publishes that executable. Production servers need
the executable for their OS and architecture and its runtime dependencies; Rust,
Cargo and native build tools belong on the development machine or build runner.

Fleet hosts watch the shared deployment pointer and construct a replacement
generation before adopting it. Adoption requires every application version to
be registered in the running executable and its artifact identity to match the
manifest. A failed build or validation keeps the serving generation. New requests
use an adopted generation; existing requests retain their generation, and cells
transition under the configured generation-age policy. Embedded `sdk::Node`
hosts select updates explicitly through `Node::adopt`.

A configuration update or selection of code already compiled into the same
executable can use generation adoption. Changing Rust code requires building and
publishing a new executable, installing it at a new path, and having the process
supervisor roll nodes to that executable with graceful drain and readiness
checks. Cellar does not download and replace its running executable automatically.
Keep cell and resource identities stable to retain durable state; application
schema and workflow compatibility still need explicit migration and rollout
decisions.

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

Embedded CPython projects can declare a target-specific `python_environment` inventory/archive. Native build/publish/install verifies and transports the closure using the `embedded-python-v1` feature gate; the installer requires an explicit immutable `--python-root`. Generation admission also checks the process-bound inventory digest. See the [environment workflow](../tools/python/README.md) and [qualification gates](../plans/007-embedded-python-pyo3.md#implementation-milestones-and-acceptance-gates).
