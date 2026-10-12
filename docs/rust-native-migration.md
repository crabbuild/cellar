# Rust-native Cellar migration

Cellar's default CLI, SDK, compiled application host, Docker build, release
build, and workspace with all features have no V8 dependency. The source engine,
bootstrap and adapter have been removed. `cellar` is the Rust artifact and
operator CLI; `cellar-native` remains an alias. Applications run as compiled
Rust executables with registered cell and service factories.

The [SDK guide](sdk.md) maps the current primitives to their Rust APIs and tests.
Completing the broader framework still requires capability completion and
production rollout/recovery validation. Removing an engine is not proof of
production readiness.

## Cellar hard cutover

The packages are `cellar`, `cellar-logic`, `cellar-ltx`, and `cellar-native`.
APIs, configuration, environment variables, development paths and protocols
use Cellar, SQL, Blob and `CELLAR_*` exclusively. Applications start with new
state. Native packages require host protocol 2. Celld configuration aliases,
environment aliases, credential paths,
container labels, image references and RPC aliases are not supported.

Cell ownership, authenticated routing, actor gates, LTX replication, restore,
queue/workflow/cron system cells, durable wakes, operator tools and shutdown remain
Rust host components. Source configurations require a compiled application.

All Cellar-owned SQLite tables, indexes and triggers use lowercase `__sys_`
names. Application SQL cannot access that namespace. SQL migration ledgers use
`__sys_sql_migrations`, or a custom name starting with `__sys_sql_migrations_`;
the SDK and CLI access them through the migration history API. SQLite-managed
`sqlite_*` tables and application tables keep their own names.

SQL uses `__SQLDatabase` and the namespace salt `cells:v1:sql:__SQLDatabase`.
Blob records use `blob/` object keys and the `cellar-blob` metadata envelope.
Peers use `cellar-tunnel` and `cellar-log-stream`. Development state lives in
`.cellar`. Original Celld state is not imported or migrated. Third-party endpoint
names still follow their provider's actual URL format.

Structured clone runs in Rust. `RpcData::Structured`, `StoredValue::Structured`
and `QueueContentType::Structured` name the native format; queue content type
labels use `structured`.

The previous V8 differential tests are removed with the adapter. The Rust SDK,
process/fleet integration tests and primitive contract tests remain. A pre-removal
source snapshot was preserved outside the checkout during this change. Future
compatibility checks should use engine-free golden records and protocol fixtures.

## Remaining capability work

- Complete transferred/reconstructed service, cell, HTTP, stream and RPC
  capabilities, property/pipelined RPC and typed interface version evolution.
- Fill general stream/controller/strategy, messaging and EventSource gaps, HTTP
  facade semantics, and callback/abort transport behavior.
- Complete WebSocket close/cancellation parity, discarded response cleanup,
  local pair/stream surfaces, hibernation and cross-node lifecycle proofs.
- Extend guarded root-cell container controls to facets where required, and
  validate native image/fence staging, ownership and inactivity policies against
  real engines under fleet failures. Root controls and archive staging exist.
- Make the host an independently owned library runtime: typed configuration,
  async startup, explicit drain/shutdown and instance-owned services and channels.
- Provide native Wasm/Python module hosts if these remain product requirements,
  with resource staging and lifecycle limits.
- Extend high-level cryptographic key policies, async context/diagnostics and
  ergonomic equivalents of the previous Node interfaces where application code
  needs them. Rust standard library/crates replace Node module execution.
- Add enforceable process/resource isolation for untrusted native applications.
  Cooperative polling cannot preempt a nonreturning poll; per-application heap
  usage and hard limits remain unavailable.

## Production and recovery work

- Exercise complete queue ACK/retry/dead-letter and workflow occurrence/step
  replay across crashes, restarts, failed wakes and ownership changes.
- Expand Blob/assets/cache backend failure, cancellation, crash, resource and
  fleet-fence conformance against supported object stores.
- Verify multi-node Rust deployments, cross-node bindings, executable
  rollout/rollback, reexec, schema/policy evolution and control-plane delivery.
- Prove crash/restart, transfer/fencing, LTX and paged restore, alarms, wakes,
  hibernation, cancellation, reload and shutdown under faults and cloud failures.
- Validate rolling upgrades and state recovery between Cellar releases. The
  original Celld cutover requires fresh state and has no interoperability path.
- Run the broader infrastructure conformance harness. Ordinary logic/LTX
  manifests disable their standalone test targets; the internal harness needs
  separate environment inputs and is not proved by the SDK test command.

## Completion gates

1. Default and all-feature workspace builds exclude every JavaScript engine.
2. Every retained primitive has a Rust API exercised through production routing
   and its lifecycle, with explicit unsupported behavior documented.
3. Persistent state recovers through restart, transfer, fences, restore, wakes,
   alarms, hibernation, cancellation, reload and shutdown.
4. Multi-node Rust applications and verified executable delivery support
   operational rollout, rollback and team interface evolution.
5. SDK and infrastructure conformance demonstrate these contracts under faults.

Gate 1 is implemented. The other gates still need additional implementation and
validation; do not treat engine removal as completing the whole migration.
