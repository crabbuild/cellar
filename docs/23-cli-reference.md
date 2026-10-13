# CLI reference and operator boundaries

`cellar` is the single CLI executable. A compiled application executable
starts a node with its registered factories. The generic CLI builds, develops,
publishes, installs and invokes operator tools.

![Build and activation](diagrams/deployment-flow.svg)

## Verified packaging syntax

```text
cellar --help
cellar --version
cellar build PROJECT.json --output DIRECTORY
cellar dev PROJECT.json [--host IP] [--port PORT] [--logs] [--no-watch]
cellar deploy DIRECTORY --bucket NAME [--endpoint URL] [--region REGION]
cellar install DIRECTORY --bucket NAME --output EXECUTABLE [--target TRIPLE]
```

`dev --clean` intentionally resets the selected project's local state. The
project stores state under `.cellar/<project-hash>` beside its configuration;
the hash identifies the canonical project path. Earlier development-state
layouts are left untouched and are not automatically migrated. Project paths
resolve relative to the project file. Use the checked-in counter project for
the full native configuration shape. Build's default project profile is release;
the counter project explicitly chooses dev.

`deploy` publishes a verified package to a bucket, including executable/assets
and configuration. `install` reads `package.json` from the supplied directory,
fetches and verifies the target executable, and writes a new output path. It
does not overwrite an existing executable or activate it through your supervisor.
Store operations accept the endpoint/region options handled by the packaging
CLI; use the actual binary's help for the full command.

When running from source, prefix commands with:

```sh
cargo run -p cellar --bin cellar --locked --
```

Use a private `CARGO_TARGET_DIR` per repository guidance. Native package builds
select locked dependencies and reject JavaScript-engine dependency graphs.

## Operator command families

| Family | Scope | Implementation |
| --- | --- | --- |
| `cell` | Cell-oriented authenticated operator work | [cell CLI](../crates/cellar/cli/cell_cli.rs) |
| `sql` | Owner-routed SQL database operations | [SQL CLI](../crates/cellar/cli/sql_cli.rs) |
| `kv` | Owner-routed KV operations | [KV CLI](../crates/cellar/cli/kv_cli.rs) |
| `queue` | Owner-routed queue operations | [Queue CLI](../crates/cellar/cli/queue_cli.rs) |
| `outbox` | Owner-routed source inspection and audited retry (under implementation) | [Outbox CLI](../crates/cellar/cli/outbox_cli.rs) |
| `blob` | Shared Blob backend operations | [Blob CLI](../crates/cellar/cli/blob_cli.rs) |
| `diagnose` | Diagnostics through the process adapter | [CLI dispatch](../crates/cellar/cli/native_deploy/cli.rs) |
| `connect`, `credentials`, `token`, `disconnect` | Managed connection and credential commands | [managed host](../crates/cellar/node/managed.rs) |

These families have their own options and authenticated connection requirements.
Consult `cellar FAMILY --help` on the exact release before issuing mutations.
This table intentionally does not invent unsupported fleet-restart, backup or
deployment-controller commands. Operation-specific destructive procedures need
the actual authenticated target and the team's approved change record.

CLI stdout carries command data; diagnostics/progress belong on stderr. Preserve
JSON output, bounded listings and continuation information when integrating
automation. Treat truncated pages as incomplete, and do not parse progress logs
as the command result. Do not place secrets in committed command examples.
See [operations](17-operations.md), [security](14-security-tenancy.md), and
[packaging implementation](../crates/cellar/cli/native_deploy/cli.rs).

## Outbox source controls

The event primitive remains under implementation; the
[implementation status](../plans/outbox-implementation-status.md) records its
remaining release gates. Current source commands use the existing fleet
credentials and reach the authoritative owner:

```text
cellar outbox inspect SCOPE --limit 100 --json [FLEET OPTIONS]
cellar outbox retry SCOPE --expected REVISION.json --operation UUID --reason TEXT [FLEET OPTIONS]
cellar outbox status SCOPE --operation UUID --json [FLEET OPTIONS]
cellar outbox audit SCOPE --limit 100 --after POSITION --json [FLEET OPTIONS]
```

`SCOPE` is the exact cell scope, including its class and ID. Inspection returns
metadata and an optional continuation for `--cursor`; it excludes event bodies.
Save the selected lane's `revision` object as `REVISION.json` before retrying.
The guard includes source incarnation, dispatch generation, next sequence and
settled watermark. Retry advances dispatch authority and commits wake work
without removing events or changing their identity. A changed producer revision
is refused; inspect again before starting a new operation.

Operation UUIDs are 32 lowercase hexadecimal characters. Repeat the same UUID,
reason and revision after an ambiguous reply, or query its status. The retained
receipt reports whether work was re-armed and the original time the operation was applied;
it does not establish destination delivery. Different arguments with the same
UUID conflict. A settled lane records `rearmed: false` and retains its seal.
Receipts survive business reset and have no automatic expiry. New operations
stop at the smaller of the captured operation-receipt limit and 10,000 retained
receipts per source; existing receipts remain replayable. An unknown operation
returns `OperationUnknown`.

The authenticated HTTP equivalent is `POST /outbox/SCOPE` on a live node's
internal listener. JSON operations are `inspect`, `retry` (with a `change`
containing `operation`, `reason` and `expected`), `status`, and `audit`. The
ingress verifies the signed method, path and body before minting source RPC
authority. Ordinary application RPC cannot invoke that control method. Requests
are limited to 16 KiB, revision files to 4 KiB and pages to 100 records. The CLI
bounds source replies to 1 MiB and prints command data on stdout.
