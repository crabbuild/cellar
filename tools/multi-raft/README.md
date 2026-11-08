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

## Published transfer closure after complete local erasure

Use `--suite managed-publication-loss` with an owned RustFS backend and a fresh
output directory. Missing closure metadata or its retained source registry must
refuse publication. After the exact publication, the fixture joins every voter
before child activation, deletes all six voter disks and both verifier inventories,
and drops the former clients. Fresh clients and stores reconstruct both children
from the pinned remote closure. The case checks business state, three original S3
receipts, the exact pending outbox envelope and the receiver's deduplication proof.
A payload outage must refuse fresh replay, and replay leaves topology unchanged.

Both image modes run with host-native RF3 voters and fixture capacity. This data
reconstruction gate does not install a live replacement quorum. Permanent platform
fencing, new-incarnation disaster activation and independent process crashes remain
in the managed recovery and release qualification gates.

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


## Split/merge completion probes

The fixed `managed-partial-merge`, `managed-precopy-resources` and
`managed-movement-ledger` suites select `native_cell_groups_split_merge`.
Use the owned RustFS backend and a separate output directory for each suite.
Each case runs materialized and extent images and records source and executable
hashes. They cover all child setup steps during partial parent quorum loss,
physical pre-copy cancellation debt and live request progress, and the complete
command/effect receipt ledger through a split followed by a merge. Cached SDK
handles and former-parent ingress must discover the selected successor groups.

These suites use host-native RF3 TLS voters and fixture capacity. Independent
process crashes, further movement cycles, hard RSS and sustained load remain
release qualification work.


## Adaptive partition scheduling

Use `--suite managed-partition-scheduler` with an owned RustFS backend and a
fresh output directory. Both cases run in materialized and extent image modes.
Live RF3 observations select a whole-cell hot boundary, then canonical operator
admission reserves children. The suite loses the first reservation confirmation,
resumes the original request and checks terminal cancellation. A separate cluster
joins and stops both followers before proving that the old sizing observation
cannot reserve children or acknowledge a minority write. Trackers are explicitly
bounded and advisory eviction cannot discard the pending request.

These are scheduling/admission checks with host-native voters and fixture
capacity. Physical split/merge execution has its separate movement suite.
Autonomous inventory selection, whole-group balancing, independent voter process
faults and sustained placement load remain separate gates.


## Consented node-owned transition execution

Use `--suite managed-transition-service` with a fresh output directory and an
owned RustFS backend. The fixed case starts a worker on each actual host-native
RF3 voter. A manual plan cannot advance without execution consent. The test
checks principal/payload refusal, duplicate-worker admission, controller
replacement during a registry write refusal, and automatic preparation, sealing,
installation, publication, activation and archive coverage. It reconciles original
SQL results and floors through the selected children, then performs new writes.

Physical parent quiescence remains a separate proof. The test joins and restarts
only the owned parent voters, verifies that partial return retains the disruption
slot, and lets the controller finish once all physical observations verify. It
runs materialized and extent images with independent bounded catalog clients per
simulated node. This does not qualify live group retirement without host restart,
automatic plan selection, independent processes or sustained placement load.


## Live source retirement

Use `--suite managed-live-retirement` with an owned RustFS backend and fresh output
directory. The case executes a consented split in both image modes, then waits for
the service to drain and detach every source group without replacing any process.
It checks the exact original node identities, no active parent registrations,
retained physical log occupancy, parked-source observations, original receipts,
child archives and subsequent ingress reads and S3-durable writes. It includes
read-first and write-first ingress. A second case places one child on the three source nodes and the other on
spare voters, preserving the unchanged two-history bound. It verifies that child
owners stay live while only the parent retires. A premature quiescence request refuses before source sealing.

The SQL admission unit case holds a source result/producer reservation across
retirement: late reservations refuse, held debt remains charged, and another
group can still reserve while the source drains. Real backend checks use
host-native RF3 voters and fixture capacity. Disk deletion/reclamation, automatic
plan selection, destination failures, process fault schedules and sustained load
remain separate gates.


## Pre-seal destination capacity

Use `--suite managed-transition-capacity` with an owned RustFS backend and fresh
output directory. All three exact cases execute materialized and extent images. One
places two children beside a retained parent on two-slot nodes. The other starts
with empty destination engines and an unrelated, unmaterialized birth reservation.
The third restores the original 512-MiB image profile and proves that two slots
can fit while their combined reservations and existing usage exceed the platform
storage budget. The positive fixtures use a stricter 32-MiB per-image quota and 256-MiB image pool with the
unchanged two slots and 1-GiB deployment budget.

All must refuse before sealing, keep the original parent writable at quorum,
preserve receipt/version reads, replay the refusal after controller replacement,
and permit pre-seal cancellation without deleting unrelated reservation debt.

The gate owns fresh complete inventories at the exact Fleet root. It accounts
for current settled voters, retained physical occupancy, missing reserved/live
members and durable resource floors. Placement reservation quotas and deployment
capacity are checked separately. The node's existing control budget pays for the
bounded inventory workspace. Tests retain the original capacity and deadline
limits. Physical source reclamation, temporary disk/RSS peaks, automatic placement,
independent processes and load remain separate qualification work.

## Automatic local sizing selection and admission recovery

Use `--suite managed-automatic-selection` with an owned RustFS backend and a
fresh output directory. Its exact case runs both image modes. The sampler
finds the active local group without a caller-supplied history, waits for real
owner samples and selects a whole-cell split. After a real child reservation
loses its confirmation, a new sampler and catalog client resume the exact
persisted plan. Missing and changed plans refuse, and pending sampling makes
no catalog reads or replacement operation. The consented node-owned service
then qualifies coordinator replacement, live source retirement and original receipts/versions plus subsequent child S3 writes.
Discovery then rotates across both live child groups on the same voters, refuses
a one-tracker profile, and excludes the physically retained retired parent.

The complete managed local inventory must fit the configured 1–128 tracker
capacity. Overflow refuses; it never silently truncates to a preferred prefix.
The existing control budget owns sample metadata and the frozen request. These
host-native RF3 voters use fixture capacity; placement identities and execution
consent remain explicit. Whole-group balancing/reserve/fanout, physical source
reclamation, independent voter processes and load remain separate gates.


## Retired source reclamation

Use `--suite managed-source-reclamation` with an owned RustFS backend and a
fresh output directory. `RaftFleetControl::reclaim_transition_source` accepts
the original authorized source request only after its selected retirement and
parent-hold release. The source must already be quiesced. Each turn scans at most
256 private manifest pages and 256 shared image files; shared readers and child
roots retain their pins. False requests another bounded turn. True confirms
private page cleanup and engine/controller slot release. Fleet capacity floors
remain conservative; this API does not refund durable-byte or memory budgets.

The engine atomically replaces the mapping and complete log with a permanent
refusal keyed by group ID. The same group cannot reappear under another namespace
or incarnation. An incomplete local receipt survives loss of the caller and
supports retained-startup cleanup; a completed receipt makes retries idempotent.
Permanent metadata has an explicit engine limit (4,096 refusal records by default,
at most 4 KiB each, with 16 KiB each charged to the existing cache budget). Exhaustion refuses fresh cleanup and retains the source
slot; retries of existing receipts still resolve. This applies to permanently
retired transition parents. Removing an obsolete
member of a live history requires separate membership authorization.

The exact case keeps one child beside its source within the original two-slot,
1-GiB declared deployment profile, collects all three source replicas, then checks
minimum-version reads and S3 writes through the original ingress. A retained
restart must preserve the refusal and exact replay. Both image modes run.
Fleet floor refunds and repeated movement cycles, process fault injection,
independent voter containers, measured disk/RSS peaks and load remain open gates.


## Removed live-member collection and return

Use `--suite managed-member-reclamation` with an owned RustFS backend and fresh
output directory. `RaftCells::reclaim_removed_member` requires the latest completed
Fleet replacement and the exact obsolete local member. It joins the group owners,
fences the removed member prefix, collects private pages/images in bounded turns,
and settles the known engine slot and group disk reservation once. Unknown disk
debt, the deployment baseline and installed memory remain charged. Reuse requires
an authenticated managed seed with the same history/birth and a higher member ID.

The case moves a replica out and back to the original node, then back to the
previously joined node. It checks repeated fence replacement, local linearizable
SQL on returned members, original receipts, token reads and subsequent S3 writes.
An actual successful settlement CAS loses confirmation; retry must not refund
again. Joint/surviving-member cleanup, generic receipt/index mutation and old
genesis must refuse. Both image modes retain the original resource/deadline bounds.

This is explicit original-boot cleanup while its completion remains selected.
Automatic cleanup ownership/dispatch, historical and successor-boot cleanup,
crash/restart schedules, domain loss/reserve, large-inventory fanout, independent
voter processes, load and measured resource peaks remain separate gates.


## Durable removed-member cleanup worker

Use `FleetCatalog::start_member_cleanup_service` on an enrolled original source
boot to discover its pending cleanup without caller-supplied reservation IDs.
Alternatively set `FleetMovementServiceOptions::reclaim_removed_members` to true
on an existing movement/topology worker; admitted effects run before idle cleanup.
The node owns cancellation, shutdown and the single control-worker lease.

A completed live-source movement atomically appends to a bounded node queue.
Admission reserves queue room, including active movements. A same-group return
to that node refuses until physical completion and the receipt, resource floor,
member index and dequeue commit together. Unrelated nodes' reservations do not
block this node's settlement. Historical completed moves remain collectible
using the exact old local member and immutable completion, even after later moves.
Permanently isolated sources retain their separate offline retirement authority;
they are not queued for execution by a fenced old process.

`--suite managed-member-cleanup` tests both image modes against owned RustFS.
It executes two moves, pauses the first source's real settlement CAS after local
GC, stops and joins the worker, then replaces it. The replacement discovers the
saved node queue and settles once. A fresh worker on an empty queue makes no root
mutation. Generic queue deletion refuses and original SQL, receipts, versions
and subsequent S3 writes remain intact. Original resource/deadline bounds apply.
Successor boots, daemon startup wiring, failure-domain/reserve/fanout and load
qualification remain open.
