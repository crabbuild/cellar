<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Raft counter

This development executable runs three embedded nodes with one shared
three-voter group. `Counter:one` and `Counter:two` retain independent SQLite state,
logical histories and command receipts inside that group. Each node serves native HTTP
on ports 9901–9903 and an application-owned Axum router on ports 9911–9913.
Both use the same registered synchronous commands, queries and node-owned
dispatcher. Followers apply LTX and serve committed-image reads without
executing the increment callback.

The retained `inventory.json` records both resource counts and public
`RaftGroupHandle::observe` samples from all three actual owners. Each sample
includes physical history/member identity, two-cell occupancy, local checkpoint,
persisted/committed/applied positions and retained entry count. These are
administrative observations; they grant no quorum or archive coverage.

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
Fresh setup uses the public `RaftNamespaceProvisioning` API to publish one
all-range group and persist/sync its one-use archive intent before creating
archive counters. Node startup opens that exact namespace and group; replicated
enrollment initializes each cell. Retained setup only opens existing catalog and
archive metadata. Missing catalog with voter disks refuses before opening a new
catalog, and predecessor per-cell directories are not migrated.
Cell data and Raft logs persist in each node's directory. `inventory.json`
records the observed controller group count and manifest cell count per node;
startup requires one group and two cells on all three replicas.
Pass `--single-cell` after the token path to enroll and serve just `Counter:one`
through the same shared group. Use a fresh directory for a different occupancy
and retain the original option for restarts.
Keep all of these files for a normal restart. Losing a replica's disk requires
membership repair; this example cannot reinitialize its old voting identity.

Use a bearer header containing the token file's contents. `POST
/counters/one?delta=7` requires these headers:

| Header | Value |
| --- | --- |
| `Authorization` | `Bearer TOKEN` |
| `x-command-id` | Nonzero 16-byte client identity in hex, followed by `:1` |
| `x-retry-until-ms` | Future Unix time in milliseconds, within the configured receipt retention window |
| `x-write-durability` | `quorum` (default) or `s3` |

The next new command from that client uses sequence 2. Persist a pending command's
ID, delta and retry deadline before sending it. After a lost response, resend
the same values. A conflicting delta returns 409. The optional
`x-contract-digest` header must match the registered operation when supplied;
a mismatch returns 422 before business execution or duplicate receipt lookup.
The command's normalized input is the signed 64-bit delta in big-endian bytes.
HTTP request bodies do not contribute to this endpoint's command input.

The bearer authorizes only the fixed `one` and `two` cells; other names refuse
before creating a handle. Before the Raft bindings are installed, authenticated
requests return 503.
Send the command to any ready native or Axum listener. The SDK can forward it
to the current leader. If the response is `Ambiguous`, retry with the same
command ID, delta and retry deadline. Read using an authenticated
`GET /counters/one` after its first committed image is published.
Every successful command and query response includes `version_token`, an opaque
authenticated observation bound to its exact cell history. Queries also report
`served_by`, the actual serving Raft member ID; commands return `null` there.
`incarnation`, `cell` and `sequence` describe the logical cell version.
`group`, `group_incarnation`, `index` and `term` come from that version's actual
commit witness. A physical group index is not a logical cell mutation counter;
another cell can advance the shared log. A receipt can retain its original
business result while a later verified image covers it.
Keep the opaque token for minimum-version reads; numeric fields alone are not a
portable proof. A token does not grant access without the bearer credential.

`GET` accepts these optional headers through either HTTP host:

| Header | Value |
| --- | --- |
| `x-read-consistency` | `stale` (default), `at-least`, or `linearizable` |
| `x-min-version` | A returned `version_token`; required for `at-least`, rejected for other modes |
| `x-read-policy` | Stale reads only: `recent-source` (default) or explicitly `disconnected-historical`, which has no age bound |
| `x-query-timeout-ms` | Total query budget, 1–5000 ms; default 5000 |
| `x-query-members` | Up to eight distinct nonzero member IDs in priority order, comma separated without spaces. Omission uses SDK routing, which reads locally when a replica is present |
| `x-query-attempt-ms` | Requires members; 1–total budget, default the smaller of 250 ms and the total budget |
| `x-leader-fallback` | Requires members; `true` to try one untried leader hint after candidates, or `false` (default) |

The following writes through native HTTP and reads through Axum. Keep the same
`command_id` and `retry_until_ms` if the POST needs to be retried.

```sh
token=$(cat examples/.cellar/raft-counter/http-token)
command_id="$(openssl rand -hex 16):1"
retry_until_ms=$(python3 -c 'import time; print(int(time.time() * 1000) + 60000)')
receipt=$(curl -fsS -X POST 'http://127.0.0.1:9901/counters/one?delta=7' \
  -H "Authorization: Bearer $token" -H "x-command-id: $command_id" \
  -H "x-retry-until-ms: $retry_until_ms" -H 'x-write-durability: quorum')
version_token=$(printf '%s\n' "$receipt" | jq -er '.version_token')
curl -fsS 'http://127.0.0.1:9912/counters/one' -H "Authorization: Bearer $token" \
  -H 'x-read-consistency: at-least' -H "x-min-version: $version_token"
```

The Axum query waits for the receipt's version or returns an error. A
`linearizable` read requires a fresh ReadIndex proof and a live majority.
Member routing preserves
the requested consistency, token and input across attempts under one deadline.
This development group uses member IDs 1–3. An invalid option combination or
duplicate recognized header returns 400; a valid token from another cell/history
returns 409. Signature/expiry failures return 503 without weakening consistency.
POST rejects read-specific headers rather than silently ignoring them.

Every native and Axum address accepts commands through the same SDK invocation.
The example explicitly grants its configured peer processes write permission for
this namespace. A follower uses its observed leader as a destination hint;
the leader independently checks the route and current write authority. A lost
forwarded response stays ambiguous: retain the original command ID and input.
The process drill retries the original receipt through all six public addresses
before and after retained restart; it cannot pass by finding only the leader.

Commands default to quorum durability and the bucket's scoped conditional
client handles background archives. Native HTTP, Axum and SDK callers can
request S3 durability to wait for a verified complete archive recovery set.
Command responses report the achieved `durability`. An archive image alone cannot activate a member or fence old hosts; this
development drill does not qualify learner repair or disaster recovery. Ctrl-C
closes HTTP servers, lets Node seal admission/drain producers, then checks the
joined Raft report for no unresolved local producers and no failed groups. Retain replica disks while archive coverage or submitted work is unfinished.

The original `native_axum_command_parity` and
`public_multi_group_replication_and_leader_failover` fixture ports remain CG-07
work. They must retain direct SDK/HTTP parity, duplicate/conflicting receipts,
authentication, contract mismatch, all read modes/options, multiple physical groups
and leader replacement. The process drill below exercises the public counter
path; it does not replace those gates or qualify production capacity, independent
hosts or complete disaster recovery.

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
It requests S3 coverage for the same command ID through the other HTTP surface.
Pass `--single-cell` to the drill to exercise the same paths at occupancy one.
A recovered receipt can name a later verified covering image; its stored result
and history must still match the original command.
