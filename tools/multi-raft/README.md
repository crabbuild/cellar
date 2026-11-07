<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Local multi-Raft qualification

The requested execution environment is a separate Colima profile, RustFS and
up to five Cellar containers. Each Cellar container must capture a hard limit
of one CPU and 3 GiB memory, with no additional swap. These local runs cannot
establish independent host/zone resilience or large-fleet capacity.

`local.py` currently prepares and probes the object-store environment. It does
not launch Cellar nodes or qualify the complete roadmap. Its public
`environment.json` records `completed: false`; credentials remain in a private
`rustfs.env` file outside the repository. Do not publish that credential file or
an unrestricted Docker inspection containing container environment variables.

Create the dedicated profile without changing the active Docker context:

```sh
colima start cellar-multi-raft --activate=false --cpus 8 --memory 16 --disk 60
python3 tools/multi-raft/local.py preflight --nodes 3
python3 tools/multi-raft/local.py prepare --nodes 3 --output /tmp/cellar-raft-local-001
python3 tools/multi-raft/local.py probe --output /tmp/cellar-raft-local-001
```

The helper uses `docker --context colima-cellar-multi-raft` explicitly. It never
stops another Colima profile or changes the user's global Docker context.
Preparation requires a fresh run directory and digest-pinned RustFS image. A
randomly named private network, object volume, credentials and loopback-only
S3 port belong to that run. The helper chooses a free host port explicitly:
Docker's guest-selected ephemeral port can collide with another Colima profile's
host forward and reach the wrong service. The pinned image uses the same
20-character access-key profile as SDK CI. RustFS has one CPU and 2 GiB memory. The readiness
probe checks its actual captured cgroup settings. The S3 probe checks seven real
HTTP operations: bucket creation, conditional creation, matching read, refused
overwrite, ETag compare-and-swap, refused stale ETag and read after replacement.
These are backend protocol checks; they do not prove Cellar archive recovery.
Container setup follows the [official RustFS Docker guide](https://docs.rustfs.com/en/installation/container/docker).

Three nodes require six guest CPUs and at least 11.5 GiB available guest memory,
including the store and observation headroom. A five-node run requires seven
CPUs and at least 17.5 GiB available guest memory; configure eight CPUs and
18 GiB or more before starting that run. The helper refuses insufficient
resources before creating a run directory or Docker resources. The declared
container limits are ceilings, not reserved or independent physical machines.
Record concurrent host activity and VM resources with performance results.

To stop just the recorded backend while keeping its object data for inspection:

```sh
python3 tools/multi-raft/local.py stop --output /tmp/cellar-raft-local-001
```

This checks the container's ownership label first. No command here deletes
volumes, replica directories, credentials or another run's resources. Failed
preparations retain their environment record for diagnosis. A separate cleanup
step can remove the exact recorded resources after their data is no longer
needed; never use a global Docker prune or delete a Colima profile as cleanup.

## Shared cell-group managed ingress checks

Use the same owned loopback RustFS backend and a fresh output directory.

```sh
python3 tools/multi-raft/cell_groups.py --suite managed-ingress \
  --backend /tmp/cellar-raft-local-001 \
  --output /tmp/cellar-managed-ingress-rustfs-001 \
  --target-directory "$HOME/Workspace/crabbuild-target/cellar-my-checkout"
```

The suite runs the managed native and Axum request drill in both image modes.
It records the operator CLI and test executable hashes. The fixture provisions
the group and archive through the admin API and CLI, enrolls two cells, and
checks routed requests, exact retries, retained restart and quorum-loss refusals.
The voters and ingress run on the development host, with fixture boot ordering
and capacity attestation.

For the public repair and maintenance gate, use the same command with
`--suite managed-lifecycle` and a fresh output directory. The lifecycle case
uses both image modes and keeps the obsolete member's physical debt through
repair. It verifies a guarded handoff, joined stop, paused retained return
and fresh resume before the returning member serves both cells.

## Shared-group effect handoff through a split

Use `--suite managed-effect-transfer` with the same owned RustFS backend and a
fresh output directory. The exact case sends a signed S3-durable effect to a
second cell, loses its committed receiver reply, then moves the two cells into
different child groups. It retains the pending source delivery through child
activation and retained restart. Guarded handoff selects an unrestarted child
voter so S3 settlement must attach its archive in the existing process.
Retrying the original envelope must settle the source once and preserve the receiver's original deduplication proof. Old
version tokens and command retries remain valid. Independent child archive
replay checks the completed outbox, receipt and receiver rows in both image
formats.

This host-native RF3 TLS fixture explicitly admits 64 KiB inputs to satisfy the
signed inbox protocol. The ordinary SQL fixture keeps its 1 KiB profile. These
checks do not establish independent voter processes, permanent host fencing,
or all-local-copy erasure immediately after publication.

## Constrained Linux managed daemon

The daemon fixture runs from the dedicated Colima guest. It launches each voter
in its own container with one CPU, 3 GiB memory, no additional swap and a separate
retained disk mount. The guest loopback network carries real Raft TLS and native
HTTP. All voters share the guest's physical host identity. The controller runs
outside the voter containers and keeps the Docker socket out of their mounts.

Start and verify an owned RustFS backend as above, then build and run from a
committed source revision.

```sh
revision=$(git rev-parse HEAD)
git archive "$revision" | docker --context colima-cellar-multi-raft build \
  --build-arg "SOURCE_REVISION=$revision" \
  -f tools/multi-raft/daemon.Dockerfile -t cellar-linux-daemon:qualification -
image=$(docker --context colima-cellar-multi-raft image inspect \
  --format '{{.Id}}' cellar-linux-daemon:qualification)
python3 tools/multi-raft/daemon.py --image "$image" \
  --backend /tmp/cellar-raft-local-001 \
  --output /tmp/cellar-managed-daemon-linux-001
```

Both image forms use the real operator CLI for capacity and first-group
provisioning. Each run stores daemon node leases and peer authentication in
its own RustFS runtime prefix. The fixture independently reads the three
live leases before exercising SQL. Raft catalog and archive objects use the
separate group paths in the same isolated bucket. The fixture also parks
all three voters through the admin CLI,
verifies that status and replay leave them parked, and checks fresh quorum and
native and Axum reads after wake. Both ingress paths exercise quorum writes,
receipt-backed follower reads, an owned leader SIGKILL, exact retry reconciliation, retained restart and
loss of quorum. Local SDK observations record the leader on all three voters
before SIGKILL and the new term on both surviving voters before restart. A
per-voter fault adapter refuses archive writes while catalog traffic and object
reads continue to use RustFS. The same command returns a quorum
receipt during the fault. A linearizable read covers that receipt. Both HTTP
adapters refuse explicit S3 acknowledgement until the original result has S3
coverage after writes resume. The public maintenance workflow stops a voter
and returns it with the same disk and a new process generation. A survivor
accepts quorum SQL while the voter is offline. The returning voter keeps
ingress closed until an explicit resume. After a guarded leadership transfer,
both HTTP adapters replay the original receipt at quorum and S3 durability.

Add `--standby` to qualify and bind a fourth constrained daemon to its
immutable Docker container. Its peer transport starts before it has any
replica. The controller verifies authenticated inspection of the standby,
then cancels and joins startup.

Use `--repair` to keep the standby running and inject a voter failure after
retained maintenance. An active voter runs the recovery service against the
bound Docker inventory. The campaign verifies rooted removal, byte-for-byte
retention of the obsolete Raft engine, restoration to three voters, and SQL
through both adapters on the replacement leader. Docker CPU and memory
limits are observed directly. The fixture supplies disk capacity inputs.

The record includes source and binary digests, S3 probes, running resource
settings, disk mounts and exit states. Logs and disks remain available after
the run. The lab attests fixture capacity on one VM. Cross-host failure, complete
RustFS loss and sustained load have separate acceptance gates.

## Sealed source archive holds

Use `cell_groups.py --suite sealed-sources` with an owned RustFS backend and a
fresh output directory. Both image forms run the actual three-voter source seal,
verify the complete archived source independently, and record the exact hold
set in the directory CAS. The fixture rejects pre-seal and released holds,
reopens the winning decision after a lost reply, and refuses an alternate hold
for the same source cut. The unmanaged namespace remains selected at its parent
ranges throughout the drill.

## Managed transition controller loss

Use `cell_groups.py --suite managed-phase-restart` with an owned RustFS backend
and a fresh output directory. The exact case runs both image modes through all
nine forward journal phases, including Archived. At each phase it interrupts an
actual provider read, drops the foreground controller, and reopens independent
metadata clients using the rooted operation ID. Replaying the original plan
must preserve the exact journal, Fleet revision and range publication.

The case samples the physical source and destination leaders for both seeded cells'
ranges. Only the source can serve before sealing, neither history can serve
between sealing and activation, and only the child can serve after activation.
Fresh replicated zero-delta SQL commands and exact retries verify the serving
history without altering the fixture's separately checked business values.
The fixture also includes lost directory CAS replies and retained voter restarts.
Controller loss is modeled by cancellation at an observed provider read;
independent process-kill and host-loss schedules remain CG-15 gates.

## Managed retirement archive checks

Use `cell_groups.py --suite managed-retirement` with an owned RustFS backend
and a fresh output directory. The exact case runs both image modes. It reopens
selected child archive holds after promotion to Live, refuses parent release
while either child archive is unreadable, and independently replays the retained
parent holds. After all source owners are physically parked and cleanup settles,
it runs archive collection and restores both child baselines into fresh physical
inventories while the complete retired source archive prefix is inaccessible.
Restored SQL and the original S3 receipt must survive. Parked source replicas
continue to occupy their physical capacity slots.

These are host-native voters with fixture capacity and joined shutdown. The
case does not qualify independent process crashes, permanent platform fencing,
or complete local disk loss immediately after range publication.

## Shared cell-group archive checks

Commit the test and harness sources, then run the native three-voter TLS fixture
against the prepared RustFS backend. Use an absolute Cargo target directory unique
to this checkout and a fresh output directory:

```sh
python3 tools/multi-raft/cell_groups.py \
  --backend /tmp/cellar-raft-local-001 \
  --output /tmp/cellar-cell-groups-rustfs-001 \
  --target-directory "$HOME/Workspace/crabbuild-target/cellar-my-checkout"
```

The runner verifies the backend's ownership label, image, running state, endpoint
and actual resource limits, then makes seven signed S3 probes in a new bucket.
It runs twenty exact ignored RustFS cases, each in materialized and extent modes:
archive-gated local compaction with independent sibling checkpoint progress,
stable publication with continuing quorum, uncertain-publication reconciliation
with joined close, refusal of unproved S3 responses, positive local command S3
settlement, deadline/cancellation isolation, refusal when a sibling dependency
is missing, namespace minimum enforcement, original enrollment creation proofs
and enrollment deadline/cancellation isolation. Receipt cases cover later archived
observations, namespace/stored minima, typed failures and payload admission, and
read cancellation/deadlines. Remote cases cover all three authenticated output
kinds, namespace/stored minima, original creation provenance, archive-wait
deadlines/cancellation and real TLS backpressure through drain, disconnect,
revocation and timeout. Catalog setup, archive
objects, conditional registry updates and independent recovery use the actual
S3 client. Fault injection surrounds that client; lost publication responses are
injected after the real conditional write commits. No memory fallback exists.

The refusal case verifies that an unbound dispatcher cannot claim S3 durability,
including a namespace minimum. The positive command cases use an explicit
controller binding, exact receipts from later archived images, retained proof
admission and fresh routing/leader checks. Enrollment cases preserve the original
creation after later mutations, honor namespace minimums, retain output proofs and
retry without rerunning initialization. Receipt lookup preserves ambiguous absence,
returns the actual later archived observation, reserves result bytes before fetch,
and tests stored-minimum S3 delivery with a provider-configured TLS gateway.
A bounded sequence of exact retries fills reduced real socket buffers without
reading replies. The fixed physical-write observation identifies a held 28 KiB
reply inside the original 64 KiB S3 envelope while a Quorum successor progresses. The runner does not qualify local log file purge,
fenced recovery or performance.
The voters run as native host tasks, so this is not the container resource gate.

`record.json` retains source and executable hashes, exact selections, output
hashes, S3 observations and captured backend identity without credentials.
Failed runs retain their record and logs. Every run owns a new remote prefix;
remote objects and the backend remain available for inspection until explicitly
stopped using the command above. No cleanup deletes another run's data.

For CG-08, select the fixed effects case set:

```sh
python3 tools/multi-raft/cell_groups.py --suite effects \
  --backend /tmp/cellar-raft-local-001 \
  --output /tmp/cellar-cell-groups-effects-rustfs-001 \
  --target-directory "$HOME/Workspace/crabbuild-target/cellar-my-checkout"
```

This runs the exact `rustfs_claim_receiver_and_completion_use_verified_coverage`
case in `native_cell_groups_effects`, in both image forms. Source claims require
published S3 coverage before send, the real receiver preserves its S3 receipt
policy, and a lost reply retries identical delivery bytes with one receiver
callback. Conditional source completion also crosses S3 settlement. A fresh
verifier independently restores the final complete image/receipt closure and
compares every cell leaf with the selected local state. The same ownership,
resource, signed-probe, executable/source and exact-selection checks apply.
The default archive suite keeps its complete existing case set. This effects
suite does not qualify a backend outage or external-effects disaster recovery.

## Native node runs

The repository's `tools/perf/raft.py` harness now provides three native SDK node
containers with the required one-CPU/3-GiB limits, its own RustFS instance, exact
replica/version checks and final archive coverage. See
[the harness guide](../perf/raft-README.md). Use this separate profile and
`--port-base 19900` to avoid another profile's host ports. Stop the preparation
backend first; the node harness allocates two CPUs/4 GiB for its own store and
two CPUs/1 GiB for its load container, requiring an eight-CPU/16-GiB VM. Its
current topology has three nodes. Five-node placement remains additional work.

Build in that context with a target cache unique to this checkout, then use a
fresh private run directory. Replace `my-checkout` before running:

```sh
python3 tools/multi-raft/local.py stop --output /tmp/cellar-raft-local-001
docker --context colima-cellar-multi-raft build -f tools/perf/raft.Dockerfile \
  --build-arg TARGET_CACHE_ID=cellar-my-checkout-raft-target \
  --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" \
  -t cellar-multi-raft:mr13-local .
python3 tools/perf/raft.py --context colima-cellar-multi-raft --port-base 19900 \
  --image cellar-multi-raft:mr13-local --cells-per-primary 2 \
  --seconds 2 --repeats 1 --concurrency 1 --rates 2 \
  --output /tmp/cellar-raft-nodes-001
python3 tools/perf/analyze_raft.py /tmp/cellar-raft-nodes-001
```

This harness uses embedded SDK startup. Production `NativeDaemon::raft_cells`
has a separate process qualification; physical CLI/RustFS recovery drills remain
part of the operating qualification.

## Remaining qualification

An additional vertical slice must launch a compiled native application through
`NativeDaemon::raft_cells`, using real RustFS clients, authenticated TLS links,
unique persistent node disks and fresh process identities. Each group needs
three voters; on five nodes, rotate voter placement between groups and use the
remaining nodes as learners so all five can serve follower reads. Ordinary
retained-disk restart must resume the existing engine; disk loss requires
membership repair, never reuse of a lost voter identity.

Record executable/source/image hashes and inspect each actual node's CPU,
memory and swap settings before traffic. Require exact command IDs, receipts,
values and version tokens through writes, follower reads, process kill,
retained-disk restart, learner repair, quorum loss, object-store outage and
joined shutdown. Keep ambiguous writes in the history for reconciliation;
neither generate fresh IDs nor count only successful responses as the oracle.

Fenced new-history disaster restore needs its own verified deployment fence.
Stopping containers alone does not prevent an old quorum from returning.
Release/durability qualification, performance measurement and federation remain
separate gates in the [roadmap](../../docs/design/multi-raft/implementation-plan.md).

## Shared-group all-voter-disk-loss drill

`group_disaster.py` is the CG-06 Docker qualification runner. It launches the
`cellar-perf-cell-group` executable with three actual shared-group voters and
three independently versioned SQLite cells in one group. It uses real RustFS,
TLS replication, S3-gated commands, physical image and receipt recovery, new
incarnations, committed activation, fresh writes and retained-disk restart.
The runner checks every recovered voter's final values and cell versions.

Commit the sources before building. Use the dedicated profile described above,
a checkout-specific build cache and the immutable image ID produced by Docker:

```sh
docker --context colima-cellar-multi-raft build -f tools/multi-raft/Dockerfile \
  --build-arg TARGET_CACHE_ID=cellar-my-checkout-group-dr-target \
  --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" \
  --iidfile /tmp/cellar-my-checkout-group-image-id .
python3 tools/multi-raft/group_disaster.py \
  --context colima-cellar-multi-raft \
  --image "$(cat /tmp/cellar-my-checkout-group-image-id)" \
  --mode materialized --output "$HOME/cellar-qualification/group-disaster-materialized-001"
```

Repeat with `--mode extents` and a fresh output directory. If Docker's automatic
address pools are exhausted, pass `--subnet` with a distinct unused private
IPv4 /24 for each retained run. The report records the inspected network and
subnet. Configuration reaches
each stopped container through the Docker API with exact byte readback before
startup. Mutable host bind mounts are avoided because virtiofs can expose mixed
file size/content across atomic replacements. The image embeds the
complete build-input manifest; the runner refuses a source mismatch. This uses
the unoptimized, debug-info-free Cargo development profile for correctness
qualification. It establishes no throughput or recovery-time claim.

The controller owns the complete disposable deployment and is its sole launcher.
It persists permanent generation revocation under a launch lock, verifies and
removes the exact owned old containers and voter volumes, then signs the exact
recovery request. Its private signing key and launch ledger never enter a voter
or operator container. Reloading the controller cannot authorize the revoked
generation; an unknown launch outcome prevents certification. Production needs
an equivalent trusted infrastructure fencing authority that revokes every old
writer's launch capability. Stopping processes alone does not supply that proof.

Only this run's three old voter disks are deleted. The RustFS object volume,
new voter volumes and evidence remain. The runner stops all remaining owned
containers on exit, including failure, and verifies normal joined shutdown.
The fresh private output directory contains credentials, opaque tokens, command
ledgers, signing material and sanitized process logs; keep it out of Git and
publish only deliberately sanitized reports. A successful drill is an exact
local recovery result, separate from the complete CG-06 task and release gates.

## Cell-group transition contracts

CG-09 uses `native_cell_groups_transitions`. Its current required cases cover
seal authorization/cancellation, delayed work at a sealed source, and child
installation/publication. The child case runs both image forms and verifies
original source version floors and receipts after transfer and retained restart.
Phase-by-phase crash, full effect continuity, remote closure after all local
copies are erased, and complete retirement acceptance remain explicit gates.

```sh
cargo +1.98.1 test -p cellar --test native_cell_groups_transitions --locked -- --list
cargo +1.98.1 test -p cellar --test native_cell_groups_transitions --locked \
  -- child_requires_install_and_publication --exact --test-threads=1
python3 tools/multi-raft/cell_groups.py --suite managed-transition \
  --backend /tmp/cellar-raft-local-001 \
  --output /tmp/cellar-transitions-rustfs-001 \
  --target-directory "$HOME/Workspace/crabbuild-target/cellar-my-checkout"
```

The RustFS transition and split/merge suites select this target and retain their
exact case names. They refuse zero-test selections and source/binary drift.
