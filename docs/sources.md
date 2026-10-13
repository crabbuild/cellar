# Sources and evidence provenance

Reviewed on 2026-10-12 against main commit
`c24feb25057793e6d6b18502c96eccbfcac2bdf4`. The docs branch starts from that
commit. This source map cites the local implementation instead of relying on
temporally different external framework documentation.

![Framework source boundaries](diagrams/architecture.svg)

## Primary source map

| Claims | Authoritative source |
| --- | --- |
| Framework scope and architectural rules | [README](../README.md), [AGENTS](../AGENTS.md) |
| Public SDK and embedded contract | [sdk.rs](../crates/cellar/sdk.rs), [embedded Node](../crates/cellar/node/embedded.rs) |
| Application handlers, contexts and background work | [application](../crates/cellar/application/mod.rs) |
| Pure decision core and route reclamation | [logic](../crates/cellar-logic/lib.rs), [policy types](../crates/cellar-logic/types.rs) |
| Gates and stop modes | [actor](../crates/cellar/actor/mod.rs), [lifecycle reports](../crates/cellar/node/lifecycle.rs) |
| Runtime/loader defaults and CLI differences | [runtime](../crates/cellar/runtime/policy.rs), [loader](../crates/cellar/runtime/workers/loader_policy.rs), [environment](../crates/cellar/cli/environment.rs) |
| Storage, transactions and migration APIs | [storage](../crates/cellar/application/storage.rs), [transactions](../crates/cellar/application/storage/transaction.rs), [SQL](../crates/cellar/application/sql.rs) |
| Ownership, peer identity and wakes | [ownership](../crates/cellar/fleet/ownership.rs), [peer](../crates/cellar/fleet/peer/auth.rs), [wakes](../crates/cellar/fleet/wake/mod.rs) |
| Replication/logs/LTX | [replication options](../crates/cellar/durability/replication/options.rs), [logs](../crates/cellar/durability/log/mod.rs), [LTX format](../crates/cellar-ltx/reference/ltx-format.md) |
| Artifact/project/CLI contracts | [builder](../crates/cellar/cli/native_deploy/build.rs), [packaging](../crates/cellar/cli/native_deploy/mod.rs), [CLI](../crates/cellar/cli/native_deploy/cli.rs), [protocol](../crates/cellar/deployment/protocol.rs) |
| Health and telemetry behavior | [node ingress](../crates/cellar/node/mod.rs), [telemetry](../crates/cellar/observability/telemetry.rs), [host metrics](../crates/cellar/execution/services.rs) |
| Toolchain and CI commands | [crate manifest](../crates/cellar/Cargo.toml), [workspace dependencies](../Cargo.toml), [SDK CI](../.github/workflows/native-sdk.yml) |
| Runnable examples and embedding | [examples](../examples/README.md), [counter](../examples/native-counter/main.rs), [measuring host](../tools/perf/embedded.rs) |

## Retained validation reports

| Report | Provenance and interpretation |
| --- | --- |
| [1,000-cell embedded](../tools/perf/results/2026-10-12-embedded-1000.md) | Bounded local embedding run; retains preceding admission/shutdown failure context |
| [10,000-cell reference](../tools/perf/results/2026-10-12-embedded-10000-reference.md) / [JSON](../tools/perf/results/2026-10-12-embedded-10000-reference.json) | Image source `20fb251d7fb239de57bc712775d6349b5dc0a0b5`; recorded checkout `b7f7ce39dcfb5f34b62cc7930289a3c72f68b49b` has matching measured sources. Earlier than cache/typed-fence changes. 34,230 bucket and 23,314 fleet acknowledged writes; 14 full stop proofs |
| [Zero-cache 1,000-cell](../tools/perf/results/2026-10-12-embedded-cache-zero-1000.md) / [JSON](../tools/perf/results/2026-10-12-embedded-cache-zero-1000.json) | Image `cellar-embedded-perf:4db0ba6`; docs-only checkout `52519f1` matches production/measuring sources. Single-node bucket posture, 64 residents, zero cached routes; 9,284 recovered acknowledgements and both Preserve proofs |
| [Harness description](../tools/perf/README.md) | Captured-policy/cache checks, isolated resources, shutdown proof guards and explicit limitations |

These reports use local Docker/Colima resources and a single RustFS server.
The reference's raw directory is mentioned in its report but is not shipped
as part of the documentation library. JSON summaries are retained here through
repository-relative links. Local raw evidence availability must be confirmed
before independent re-analysis.

In-flight-write recovery work visible on other branches was not part of the
reviewed main baseline and is not counted as passed evidence. New results must
identify the actual merged code/image and preserve both acknowledged and
ambiguous command outcomes.

Proposed naming conventions, envelopes, outboxes, sagas, topology, dashboards,
SLOs, backup procedures and team governance are application/platform practices.
They are not hidden built-in services or achieved release gates. The
[readiness ledger](21-production-readiness.md) makes the distinction explicit.
