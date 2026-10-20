# Raft counter

This development executable runs three embedded nodes with two independent
three-voter groups, `Counter:one` and `Counter:two`. Each node serves native HTTP
on ports 9901–9903 and an application-owned Axum router on ports 9911–9913.
Both use the same registered synchronous commands, queries and node-owned
dispatcher. Followers apply LTX and serve committed-image reads without
executing the increment callback.

From the repository root, use the toolchain and checkout-specific target
directory from [build guidance](../../AGENTS.md#build-and-validation):

```sh
mkdir -p examples/.cellar/raft-counter
openssl rand -hex 32 > examples/.cellar/raft-counter/http-token
cargo run -p cellar --bin cellar-raft-counter --locked -- \
  examples/.cellar/raft-counter \
  crates/cellar/transport/tcp/fixtures/ca.der \
  crates/cellar/transport/tcp/fixtures/server.der \
  crates/cellar/transport/tcp/fixtures/key.der \
  examples/.cellar/raft-counter/http-token
```

The supplied TLS files are public test fixtures for this loopback-only example.
All replicas share one machine; the example does not provide zone resilience.
The development object-store adapter persists catalog roots and bootstrap claims
in `catalog.sqlite`, including public version-key retention metadata. The
development version-signing key is fixed for this local drill; production
embeddings inject managed signing/verification keys through `RaftCellsServices`.
Cell data and Raft logs persist in each node's directory.
Keep all of these files for a normal restart. Losing a replica's disk requires
membership repair; this example cannot reinitialize its old voting identity.

Use a bearer header containing the token file's contents. `POST
/counters/one?delta=7` requires these headers:

| Header | Value |
| --- | --- |
| `Authorization` | `Bearer TOKEN` |
| `x-command-id` | Nonzero 16-byte client identity in hex, followed by `:1` |
| `x-retry-until-ms` | Future Unix time in milliseconds, within the configured receipt retention window |

The next new command from that client uses sequence 2. Persist a pending command's
ID, delta and retry deadline before sending it. After a lost response, resend
the same values. A conflicting delta returns 409. The optional
`x-contract-digest` header must match the registered operation when supplied;
a mismatch returns 422 before business execution or duplicate receipt lookup.
The command's normalized input is the signed 64-bit delta in big-endian bytes.
HTTP request bodies do not contribute to this endpoint's command input.

Before the Raft bindings are installed, authenticated requests return 503.
Send a command to the current leader's port. A follower returns 503 with a
`NotLeader` hint. A response containing `Ambiguous` retains the original command
ID; the gateway does not retry or generate a new one. Read using an authenticated
`GET /counters/one` on any replica after its first committed image is published.
Every successful command and query response includes `version_token`, an opaque
authenticated observation bound to its exact cell history. Queries also report
`served_by`, the actual serving Raft member ID; commands return `null` there.
The value and group/incarnation/index/term describe the committed image or receipt.
Keep the opaque token for minimum-version reads; numeric fields alone are not a
portable proof. A token does not grant access without the bearer credential.

`GET` accepts these optional headers through either HTTP host:

| Header | Value |
| --- | --- |
| `x-read-consistency` | `stale` (default), `at-least`, or `linearizable` |
| `x-min-version` | A returned `version_token`; required for `at-least`, rejected for other modes |
| `x-read-policy` | Stale reads only: `recent-source` (default) or explicitly `disconnected-historical`, which has no age bound |
| `x-query-timeout-ms` | Total query budget, 1–5000 ms; default 5000 |
| `x-query-members` | Up to eight distinct nonzero member IDs in priority order, comma separated without spaces; omission means local execution |
| `x-query-attempt-ms` | Requires members; 1–total budget, default the smaller of 250 ms and the total budget |
| `x-leader-fallback` | Requires members; `true` to try one untried leader hint after candidates, or `false` (default) |

For example, return the command's token in `x-min-version`, select `at-least`,
and send the GET to a follower. It waits for that version before reading or
returns an error; it never silently serves an older image. `linearizable`
requires a fresh ReadIndex proof and a live majority. Member routing preserves
the requested consistency, token and input across attempts under one deadline.
These development groups use member IDs 1–3. An invalid option combination or
duplicate recognized header returns 400; a valid token from another cell/history
returns 409. Signature/expiry failures return 503 without weakening consistency.
POST rejects read-specific headers rather than silently ignoring them.

The example uses Quorum command durability. S3 command durability and archive
restore remain pending; unsupported SDK requests are refused. Ctrl-C closes
the HTTP servers and joins the services and nodes. Retain replica disks when
shutdown reports unfinished work.

The `native_axum_command_parity` integration case exercises real native and Axum
listeners, direct SDK commands, duplicate receipts, conflicting inputs,
authentication, contract mismatch, all three read modes and invalid read options.
`public_multi_group_replication_and_leader_failover`
exercises two groups across three nodes, follower image publication and leader
replacement. These fixtures establish behavior within their test scope, not a
production capacity or complete disaster-recovery qualification.

Run the process restart drill against an already built binary with a new work
directory (the script retains replica disks and logs and never prints its token):

```sh
python3 examples/raft-counter/smoke.py \
  "$CARGO_TARGET_DIR/debug/cellar-raft-counter" /tmp/cellar-counter-drill
```

The drill first occupies the final Axum listener to force a late startup failure,
then resumes the same disks. It checks both ingress paths on all three nodes,
remote AtLeast/linearizable reads with the requested serving member, cross-cell
token refusal, joined shutdown, retained tokens/receipts and new writes after restart.
A recovered receipt can name a later verified covering image; its stored result
and history must still match the original command.
