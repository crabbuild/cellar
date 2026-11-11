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

## Primitive compatibility evidence

CG17-01's [coverage contract](../../docs/design/multi-raft/cell-groups/qualification.md#machine-readable-inventory-and-acceptance-evidence)
has a partial SDK catalog and an independent admission verifier. Check them with:

```sh
python3 tools/multi-raft/primitive_coverage.py check-catalog
python3 -m unittest discover -s tools/multi-raft -p test_primitive_coverage.py
```

These commands validate the contract and synthetic refusal fixtures. They do
not run public primitives, start voters, or qualify an SDK configuration. An
acceptance report requires a reviewed catalog; the current partial catalog
refuses it. Complete the compiled export audit before changing that status.

The [format-1 schema](../../docs/design/multi-raft/cell-groups/primitive-coverage.schema.json)
defines the report envelope. `plan.py` reads the three contracts from the tested
source revision and verifies the report at CG-17/15/16 acceptance. The runner
must preserve repository-relative artifacts, SHA-256 hashes, source input digest
and application, voter, CLI and exact test executables for each feature mode.

For each required row, capture the exact selected test and its normal libtest
summary. The test emits a `cellar_primitive_evidence` JSON line with `cell`,
`source_revision`, `configuration`, `observations_sha256` and `ledger_sha256`.
One test can emit several rows it actually exercised. Reusing a passing log
for an unobserved configuration refuses. Keep the log intact; the verifier
handles structured lines interleaved with the libtest status line.

- Physical observations retain initial group histories, voter process, disk and
  cgroup identities, voter limits and ordered fault events. Disaster rows
  require fencing and erasure for every voter in the observed source groups.
- State ledgers retain each public symbol's original identity, expected and
  observed outcome/result/state, required proof and duplicate-commit count.
  Documented refusals retain their typed error codes. Archive/disaster payload,
  filesystem and container rows also hash actual restored bytes.
- Native, native without default features, Python, JavaScript and mixed-mode
  regressions retain distinct backend and configuration identities. A Fleet or
  bucket result cannot fill a shared Raft row.

Real runners still need to capture these artifacts from public applications,
owned process/RustFS fault runs and supported providers. A synthetic report,
configuration label or passing test name alone establishes no runtime coverage.

## Ordinary cell RPC and Storage checkpoints

Run the scoped ordinary JSON RPC adapter against an owned RustFS backend:

```sh
python3 tools/multi-raft/cell_groups.py --suite ordinary-primitives \
  --backend /tmp/cellar-raft-local-001 \
  --output /tmp/cellar-ordinary-primitives-001 \
  --target-directory /absolute/checkout-private/target
```

Pass `--keep-going` to collect all standalone primitive case results after a
case failure. Each case uses its own native process and store prefix. Any failed,
ignored, empty or missing case keeps the campaign failed; executable/source
changes and failed prerequisites still stop it. Managed process/fault suites do
not accept this option. The report retains separate passed and failed case lists.

This selects six exact cases that run both image modes on host-native RF3.
The checkpoint case uses two cells sharing a group and one SQL worker per node. It checks ordinary
Storage SQL/KV, independently committed async transactions, ordered explicit
sync, nested rollback/refusal, bounded work, partial failures, exact replay and
live leadership transfer. Explicit S3 checkpoints require real archive proof.
The activation case fills the node-wide instance cache across two SQL workers,
checks reuse and eviction, expires idle instances on the real worker clock,
transfers leadership and restarts retained RF3 voters. Original receipts replay
without constructing another instance; constructor Storage remains usable only
within its live activation, while invocation handles expire after each call.
The interleaving case runs Quorum and explicit S3. Two same-cell invocations
share one native instance between proven roots; the first resumes against the
second's committed image. Private DML/DDL and open transactions remain exclusive.
Leader replacement fences parked work. Joined stop and retained RF3 restart
preserve its first root and unfinished receipt without rerunning the handler.
The dependent SDK case runs Quorum and explicit S3 source durability. Commands,
queries, receipt inspection and enrollment wait for their source checkpoint;
target commands retain their separate durability. The case covers authenticated
forwarding, recursive same-cell calls, dropped child responses, original
deadlines, leader fencing and synchronous/atomic callback refusal. Its observer
uses the unused voter with linearizable reads while the parent and forwarded
child retain both leader controller slots. A writer-lease refusal before enqueue
can wait under the original deadline; an admitted callback error cannot replay.
The name-discovery case uses a namespace read grant to resolve an enrolled name's
current incarnation, constructs its handle inside an ordinary RPC, then issues a
separate command after source proof. It checks absence, unpolled deadline expiry,
synchronous/query/atomic refusal, leader transfer and quorum-loss refusal.
Discovery cannot create a cell or reconcile an unknown command outcome.
The committed-Storage case exercises synchronous commands and immutable queries,
including keyset cursors with forward/reverse range bounds, structured values,
mutation between steps, second-iterator invalidation, rollback and expired views.
Queries also verify exact receipt versions and cells, including an image with no
KV schema. The ordinary RPC case retains a keyset cursor across an independently
committed async root; it reads that root's inserted key without a retained snapshot.
The runner retains source, executable and log hashes. It does not accept the
complete CG-17 primitive matrix or qualify declared bindings, other automatic
output/dispatch gates, independent voter processes or disk loss.

## Membership and split routing repair

Run the public range-repair cases against the same owned backend:

```sh
python3 tools/multi-raft/cell_groups.py --suite routing-range-repairs \
  --backend /tmp/cellar-raft-local-001 \
  --output /tmp/cellar-routing-range-repairs-001 \
  --target-directory /absolute/checkout-private/target
```

Three exact cases cover fourteen fresh seven-node fixtures across both image
modes. Membership replacement and delayed split lookup each exercise SDK,
native HTTP and Axum. The overlap case checks all three callers within each
image mode. Each scenario has its own object prefix and retains the original
two-history, byte and request-time limits. Membership repair keeps the range
cached, rejects removed replica service and preserves independently authorized
ingress forwarding. Split repair preserves unrelated routes and original S3
receipts. The delayed lookup crosses an actual publication while fresh Quorum
work selects the child, then refuses stale completion without another RPC.

The runner retains source, test executable and operator CLI hashes. The fixtures
invoke the recorded Cargo CLI. It
captures passing-case output and verifies complete caller/image-mode coverage
in structured ledgers. Source capture includes compiled example modules for
every suite. These entry points do not by themselves establish a passing run,
peer/boot wire authority,
recovery publication, constrained voter processes or measured performance.

RustFS suites use the [native CI test profile](../../.github/workflows/native-sdk.yml):
optimization level one with debug assertions and overflow checks, no debug
symbols, and one Cargo build job. The record captures those explicit settings.

## Cached advice and command-origin revocation

```sh
python3 tools/multi-raft/cell_groups.py --suite routing-authority \
  --backend /tmp/cellar-raft-local-001 \
  --output /tmp/cellar-routing-authority-001 \
  --target-directory /absolute/checkout-private/target
```

Two fresh RF3/ingress fixtures cover SDK, native HTTP and Axum in both image
modes. Each interaction warms direct advice, revokes write permission on every
voter, and verifies one cached attempt with no SQL staging or callback entry.
Transport closure preserves the original command's ambiguity and typed cause;
the fixture's observations cannot supply a non-commit proof to the client.
Independent read permission remains usable. Stale grants and an unverified
process cannot restore permission. A newer valid grant reuses the refused
command ID with S3 proof and exact receipt replay, preserving the original
limits. Six structured records retain identities, routing/outcome counters,
owner observations and receipts. The runner requires exact test execution and
complete evidence, and invokes its retained Cargo operator CLI.

These entry points need their own runtime evidence. They do not close forged
or conflicting-term advice, restarted endpoint wire cuts, full RO-03, constrained
voter-process or performance qualification.

## Endpoint repair and held replies

```sh
python3 tools/multi-raft/cell_groups.py --suite routing-endpoint \
  --backend /tmp/cellar-raft-local-001 \
  --output /tmp/cellar-routing-endpoint-001 \
  --target-directory /absolute/checkout-private/target
```

Four cases cover SDK, native HTTP and Axum in both image modes. A selected
voter restart repairs only its authenticated endpoint. An unrelated follower
with unavailable endpoint metadata leaves the healthy leader usable. Read-first
Strong and AtLeast queries repair their selected endpoint while preserving
barrier, callback and original-budget requirements. The held-reply case blocks
a real group-archive PUT while another follower's new NodeView is accepted;
the unchanged leader's S3 reply and exact receipt replay must remain valid.

The runner requires all thirty observations, including both consistency modes
for each read caller/image pair. It rejects duplicates, missing coverage,
placement repair, extra work, changed directory cuts, invalid boot evidence,
callback duplication and durability downgrades. The native fixtures verify
receipt/version proofs and physical owner drainage. These entry points require
their own current-source runtime evidence. Voters are host-native processes;
constrained Linux, forged advice and complete RO-03 remain separate gates.

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

The same suite also retires all three birth voters through managed replacements
and stops their processes. A retained ingress with zero local groups must route
both a first read and, after another restart, a first quorum write to the current
voters. Its next periodic directory sweep is outside the original request budget;
targeted discovery, exact receipt replay and joined shutdown must still succeed.

For the public repair and maintenance gate, use the same command with
`--suite managed-lifecycle` and a fresh output directory. The lifecycle case
uses both image modes and keeps the obsolete member's physical debt through
repair. It verifies a guarded handoff, joined stop, paused retained return
and fresh resume before the returning member serves both cells.

## Staged partition cancellation

Use `--suite managed-partition-cancellation` with an owned RustFS backend and a
fresh output directory. Both image modes race a held physical child-reservation
CAS against a rooted cancellation, before and after partial reservation. The
cases lose the actual cancellation, namespace birth-decision and settlement
confirmations, then recover on another controller. Original reserved debt and
permanent identities remain retained. Old capabilities cannot materialize a
cancelled child; historical completion cannot erase newer selected work or fence
another operation's group. Parent receipt retries, fresh quorum writes and reads
continue. The existing automatic-selection case also proves that staged
cancellation refuses an already admitted transition without preventing its
normal execution. Host-native voters do not qualify independent failure domains,
sustained load or cleanup of a child whose live birth already won.

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

## Managed transition disaster closure

Use `--suite managed-transition-disaster-closure` with an owned RustFS backend
and a fresh output directory. Both image modes recover an unpublished merge's
complete parent domain and a published merge child's newer S3-acknowledged tail.
The fixture joins every source voter before erasing its owned disk, reconstructs
the exact selected archives in a fresh verifier, and installs fresh group, cell
and voter identities. Serving requires committed activation and a fresh quorum.
Checks include original and newer S3 receipts, old-token refusal, retained restart,
duplicate results and subsequent Quorum/S3 writes.

This is a host-native TLS workflow with a fencing contract fixture. Permanent
Docker launch revocation, independent voter failures and uncertain external
effects require their separate CG-13/15 qualification gates.

Use `--suite managed-disaster-outbox` to qualify the external-effect boundary.
An in-process adapter accepts a delivery identity and holds its response after
the source claim reaches S3. The fixture joins the callback owners, deletes all
source disks, restores the published group, checks explicit reconciliation status,
and dispatches a fresh lane without reissuing the old request. Both image modes
use real RustFS and unchanged limits. This does not qualify an external exactly-once
service or permanent Docker launch revocation.

Use `--suite managed-disaster-retry` to exercise recovery-seeded command retries
through the normal SDK after confirming their archived receipts. It replays the
original command IDs and inputs, requires fresh-history Quorum/S3 proofs, refuses
old tokens and checks unchanged cell versions and subsequent SQL values. Both
image modes use real RustFS and independent cold operator verification. The
complete CG-13 profile and permanent Docker launch revocation remain separate gates.

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
committed source revision. The daemon runner selects the exact profile and
Docker context recorded by the owned backend. For a workspace-volume Colima home,
create that directory before the initial `colima start`, then pass
`--colima-home /absolute/workspace/colima-home`. Guest commands bind the matching
Colima/Lima homes and must match the backend's Engine ID before launching a node.

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
It persists permanent component generation revocation under a launch/restart
lock **before** checking inventory completeness. Failed fencing leaves admission
closed. The inventory includes every retained launch, even when it exceeds the
minimum voter roster, and can span several generations. Partial container/disk
deletion is resumable without recreating old writers. Certification requires
positive Docker reads confirming every inventoried container and volume absent.
Its private signing key and launch ledger never enter a voter or operator
container. Reloading the controller cannot authorize the revoked generation;
an unknown launch outcome prevents certification. Production needs
an equivalent trusted infrastructure fencing authority that revokes every old
writer's launch capability. Stopping processes alone does not supply that proof.

`cellar::sdk::consensus::CellGroupPermanentFenceAuthority` verifies the
controller's receipt for both shared-group and managed transition recovery.
Pin its Ed25519 key independently of the archive. The 129-byte receipt contains
format `1`, the exact request digest, a SHA-256 inventory commitment, and the
signature over `cellar-cell-group-permanent-fence-1\0` plus those first 65 bytes.
An instance with only the pinned key can verify a persisted winning receipt;
`fence` refuses until a valid controller receipt is explicitly supplied with
`with_certificate`. This adapter performs no platform fencing itself. The old
qualification-only 96-byte receipt is no longer accepted. The managed native
drill exercises this verifier after joined disk erasure; the Docker drill above
still covers one unmanaged shared group, not complete CG-13 managed recovery.

Only this run's three old voter disks are deleted. The RustFS object volume,
new voter volumes and evidence remain. The runner stops all remaining owned
containers on exit, including failure, and verifies normal joined shutdown.
The fresh private output directory contains credentials, opaque tokens, command
ledgers, signing material and sanitized process logs; keep it out of Git and
publish only deliberately sanitized reports. A successful drill is an exact
local recovery result, separate from the complete CG-06 task and release gates.

### Managed recovery request binding

`FleetTransitionRecoveryFenceRequest::encode_challenge` exports the complete
canonical format-1 intent record and directory-barrier binding. The controller
checks its framing/checksum and recomputes the exact SDK request digest. Minimum
node/disk/physical-host lineages come from those bound bytes, never a separately
supplied roster. Authenticate the expected request digest independently of the
challenge's contents; a checksum is not caller authorization.

`managed_group_fence.ManagedLauncher` maintains its own private format-1 launch
ledger. Declare the catalog/namespace and group/incarnation histories before
launch, and register each launch's physical identity before its Docker call.
This inventory must come from actual deployment ownership, not be reconstructed
from a recovery challenge. `bind(expected_digest, challenge_bytes)` selects
**every historical deployment** serving those histories, including deployments
whose entire voter set was replaced before the selected cut. It permanently
closes their generations and histories before checking minimum identity coverage.
Unknown launches, missing declarations or wrong disk/host identities refuse.

For a group assigned after launch, use
`assign_histories(generation, catalog, namespace, histories, invoke)` before its
first SDK seed preparation or attachment. It durably appends the histories to
the existing deployment and holds the launch/revocation lock through the bounded
callback. Assignments conservatively cover every historical writer in that
cohort. A failed or lost callback response keeps the assignment; retry with the
same SDK operation identity. Assignments cannot remove histories, change the
deployment's Fleet catalog, or admit any permanently revoked generation/history.
This controller API is a foundation for the managed Docker driver; it does not
establish the complete recovery gate.

`fence(expected_digest, docker)` resumes exact immutable-ID deletion; only
`certificate_payload(expected_digest, docker)` after confirmed complete absence
can supply bytes to the private signer. No manual count/roster entry point is
accepted for managed receipts. Recreating the old history under a new deployment
generation is refused; new histories use separate declarations. The export is
bounded by twice the existing 4-MiB intent bound plus 1 KiB; this does not increase
Cellar host, RPC or signed-receipt limits. The controller's wire-only CLI is used
by the managed native/RustFS fixture to verify actual SDK challenges and compare
their independently decoded minimum against the owned voters. It is not itself
a physical fence. Full managed Docker voter recovery remains a separate gate.

### Managed operator process handoff

`RaftFleetControl::transition_recovery_challenge` selects the exact retained
intent's directory barrier, independently replays the selected archives and
returns a `RaftFleetTransitionRecoverySelection` plus the controller challenge.
Retain both before stopping the operator. The selection contains only catalog,
namespace, operation and intent/directory/fence digests. It grants no authority.
The controller authenticates its expected digest separately and maps the bound
histories to its historical launch inventory as described above.

`fence_selected_transition_recovery` checks every selector before invoking the
configured fencing authority. `reopen_fenced_transition_recovery` instead
requires the already persisted winning receipt, replays the S3 closure and
verifies the receipt with the configured authority. It never calls `fence`,
selects a barrier or creates a missing receipt. A receiving voter/operator needs
its own private replay stores; no in-memory proof or serving pin is transported.
Reopening after serving release uses the later attach/retirement APIs instead.

The disposable `cellar-perf-cell-group` executable exposes these process steps:

```sh
CELLAR_GROUP_ACTION=managed-challenge \
CELLAR_GROUP_CONFIG=/private/run/operator.json \
CELLAR_GROUP_DATA=/private/run/replay-a \
cellar-perf-cell-group
```

Use `managed-fence` to confirm the external controller's 129-byte certificate,
or `managed-reopen` with only the independently pinned public key to reopen the
persisted receipt. Each invocation opens existing Fleet/namespace roots from an
explicit `RaftFleetConnection`, explicit loopback/RustFS credentials and isolated
store/bucket prefixes. It does not initialize a fleet or admit a replacement.
The [operator implementation](node/managed_operator.rs) defines the private
configuration; the [RustFS interoperability fixture](../../crates/cellar/tests/support/cell_group_managed_transition_fence.rs)
constructs it without exposing credentials in output. Do not mount the controller
signing key or launch ledger in this process. Use a fresh private replay directory
for each cold verification and keep credential configuration out of evidence.

### Managed recovery controller steps

`CELLAR_GROUP_ACTION=managed-recovery` exposes the remaining SDK recovery
operations in independent processes. The [configuration](node/managed_recovery.rs)
contains the explicit existing S3 connection, one managed namespace, authenticated
operator principal, pinned fence public key and one `command`. Keep credentials
private and use a fresh replay directory for each invocation:

```sh
CELLAR_GROUP_ACTION=managed-recovery \
CELLAR_GROUP_CONFIG=/private/run/recovery-step.json \
CELLAR_GROUP_DATA=/private/run/replay-step \
cellar-perf-cell-group
```

The controller persists each exact command before invocation. The sequence is:

`describe` reports the opened catalog/namespace identity and image mode without
initializing metadata or granting admission. It can verify the existing-root
connection before proposing recovery work.

1. `stage`: pass the retained `selection` and `replacements`, in selected-cut
   order. Each replacement declares `history` (`group`/`incarnation`), `creation`,
   `epoch`, `voters` and `learners` (`node`/`member`). Range boundaries come from
   the verified cuts, never arbitrary caller keys. Retain the returned plan digest.
2. `plan`, `reserve`/`reservation`, and `select_seeds`/`seeds` use
   `basis: {selection, plan_digest}`. Range commands also take `history`.
   `reservation` and `seeds` are read-only: absent records return null without
   allocating capacity or choosing a receipt set. Seeding itself runs in each
   managed voter after capacity admission and reservation.
3. `publish` takes that `basis` and the already selected `seed_digest`. It only
   reopens the selected quorum; it cannot create a missing seed selection.
   Reconcile a lost publication response with `publication` and the same operation,
   plan and seed IDs, including after the active barrier has been released.
4. `publication`, `activation`, `settle`, `complete`, `release` and `retire` use
   `request: {operation, plan_digest, seed_digest}`. `activation` adds a `history`;
   `release` and `retire` add the exact `completion` digest. These commands
   rederive the phase-appropriate durable SDK proofs. They do not require an
   obsolete active-barrier proof after serving release. Retirement independently
   verifies the permanent fence with the configured public key.

Each invocation executes one bounded SDK step. Settlement and retirement report
rooted cursors; the controller repeats the same request to advance the next
bounded batch. All metadata/replay substeps share one 20-second command deadline.
No mutation is automatically retried. JSON errors retain the SDK failure code and
outcome; transport loss or timeout remains unknown. Keep the original command
and reconcile it. Successful JSON output describes a durable decision; it grants
no voter membership or permission to bypass SDK serving admission.

### Managed voter process

`CELLAR_GROUP_ACTION=managed-node` starts a real SDK `Node` and `RaftCells` with
the existing managed namespace declaration and an owned engine. Set
`CELLAR_GROUP_CONFIG` to a private configuration and `CELLAR_GROUP_DATA` to its
absolute data directory. The required configuration field `retained` must be
`true` only for an existing engine and `false` for fresh storage. Copy an explicit
new configuration while the container is stopped for retained restart; its
environment does not select this managed mode. Boot identities come from the configuration; leave the older numeric
node/generation flags at zero. The [configuration](node/managed_voter/configuration.rs)
requires explicit S3 roots/credentials, the compiled Counter contract, node/disk/
process IDs, advertised peer endpoints, private TLS paths and an independently
pinned fence public key. Never mount the controller signing key or launch ledger.
The two-group and image/checkpoint capacities match the existing managed fixture.
Recovery uses its four-cell, 1-MiB image and 4-MiB restore limits, with one shared
import pool per process. This is a functional fixture envelope, not the release
load envelope.

The `control_ready` event opens only process control. The authenticated `/control`
protocol binds every request to `x-cellar-node`, `x-cellar-disk` and
`x-cellar-process`; a stale boot is refused before SDK work.

The `qualify_node` and `activate_node` actions take the public SDK
`RaftFleetNodeCapacity` request for this exact node. The deployment controller must
provide its explicit platform attestation; the SDK independently checks the
running observer, boot, profile and rooted inventory. An `observe` reply is not a
capacity attestation. Stale boot evidence cannot admit placement capacity.
Self-inspection catches up to the requested Fleet revision within the original
deadline, including an activation immediately after qualification. It still
rechecks the exact current boot, enrollment and installed trust.

The recovery phases are:

1. `reopen_recovery`: independently replay the existing S3 selection and verify
   its persisted fence, then reopen the controller's exact replacement plan.
2. `prepare_seed`: use read-only `reopen_transition_recovery_range` to find an
   existing reservation, then prepare this engine's image under SDK admission.
   A missing reservation is refused without allocating capacity. One preparation
   is retained in the process; opaque proofs never cross the wire.
3. `seed`: sync or reconcile that preparation's durable genesis and receipt.
4. `attach` and `activate`: use descriptive operation/plan/seed IDs to rederive
   the published reservation and prove the exact applied activation.

Every phase keeps its original SDK bounds; metadata/physical substeps share the
request budget. Recovery reopening uses the existing 20-second operator budget;
other requests use five seconds. The SDK retains entered producers after a lost
response. Reconcile the same identities; never invent a new command or plan.
Clearing RPC caches releases only idle copies; SDK producers keep their own
physical pins until the owning node/domain joins entered work.
Joined stop requires the complete close inventory and no unresolved local
producers. A retained manifest's `active` field records committed activation;
it survives stop and does not indicate a running worker or serving permission.

`managed_group_voter.ManagedVoter` binds a controller-owned historical launch to
this protocol. Its start/prepare/seed/attach/activate methods durably assign the
history and hold the launch/fencing lock through the bounded request. Raw calls
cannot bypass that assignment. The client refuses redirects, ignores ambient
proxies and never automatically retries a mutation. Ordinary Counter SDK reads
default to linearizable consistency; writes require Quorum or explicit S3 proof.
Missing groups or quorum refuse admission.

### Managed Docker source cohort

`managed_group_drill.py` supplies the source side of the managed recovery drill.
Build `Dockerfile` from a clean committed source with a checkout-specific
`TARGET_CACHE_ID` as above, then pass its immutable image ID:

```sh
python3 tools/multi-raft/managed_group_drill.py \
  --context colima-cellar-multi-raft-my-run --image "$image" \
  --mode materialized --port-base 19800 \
  --output /tmp/cellar-managed-source-001
```

Repeat with `--mode extents` and a fresh output directory. The dedicated VM needs
at least six CPUs and 14.5 GiB memory. Three voters each have one CPU, 3 GiB and
no additional swap; RustFS has one CPU and 2 GiB. The colocated `Nodes` policy is
explicit; all voters report the same Docker host identity. Capacity admission
binds actual cgroup settings and measured usage to the running SDK boot/profile.
The one-GiB disk budget is declared deployment capacity, not a filesystem quota.

The `managed-bootstrap` process explicitly initializes new SDK Fleet/namespace
roots and returns the actual compiled schema, capability and namespace digests.
Ordinary managed opens cannot initialize missing roots. Separate
`managed-recovery` processes register nodes and reserve, publish and archive the
first shared group. Each voter independently verifies rooted startup and capacity.
The controller grants namespace reads/writes explicitly; membership supplies no
application permission. The three startup waiters enter concurrently under one
durable assignment/fencing lock. The driver then checks three S3-durable SQL
cells and subsequent Quorum commands in one physical group, minimum-token and
linearizable reads at every ingress, retained
restart with new process IDs, original receipt replay and joined SDK shutdown.
Peer listeners bind the exact advertised container IP; wildcard listeners do not
satisfy the physical group's local endpoint identity check.
Retry results, histories and durability stay fixed; their SDK observation may
advance to a later complete archived image. Reads before and after replay require
the same current SQL value and version, so an extra application cannot pass.
Only config and peer TLS snapshots enter stopped containers; the fencing key and
launch ledger stay with the controller. Request identities are persisted before
entry; lost mutations are retained for reconciliation without automatic retry.

This is a source-cohort gate. Its report keeps `full_dr_completed: false`;
transition preparation, permanent revocation, complete source-disk deletion and
the positive managed seed-to-serving flow still require their combined gate.
It establishes no throughput, independent-host resilience or full primitive
compatibility claim.

### Managed transition disaster driver

`managed_group_transition.py` extends that exact source workflow with an
unpublished split, source sealing, durable recovery-intent selection, permanent
Docker fencing and new-history recovery. Use the same CLI arguments and a fresh
output directory. Its dedicated VM needs eight CPUs and at least 21 GiB actual
memory (a 24-GiB profile); five voters remain limited to one CPU/3 GiB each.
Split rosters use the existing two-slot profiles without increasing their bounds.
The old five-node cohort is permanently revoked and every disk deleted before
the three fresh replacement nodes start on a different VM. Pass
`--replacement-context` and `--replacement-image` for an idle, separately owned
16-GiB VM with the exact image already loaded. Setup verifies that image before
any disk erasure, stops the replacement VM and starts the 24-GiB source phase.
After fencing, it gracefully restarts the retained RustFS service on an 8-GiB
source VM before starting the 16-GiB replacement. This keeps guest reservations
within 24 GiB on the 32-GiB development machine. No SDK request is entered during
the declared service restart. The driver controls those existing VMs through
profile-scoped Lima commands, checks exact CPU/memory, and preserves matching
Colima resource metadata. This avoids Colima's macOS filesystem-watcher startup
without touching default/remote contexts or creating another VM. VM stop refuses
running containers. A one-CPU/1-GiB, 32-connection TCP bridge preserves
S3 bytes/headers and joins its execution-domain tasks on stop. The old-host
exclusion remains unchanged; inventing a replacement host ID is forbidden. This qualifies one Counter/namespace
scenario; published child tails, external effects and all six CG-13 cases still
require their complete gate.

Publication attachment starts non-serving recovery owners. After committed
activation, Fleet settlement, directory completion and serving release, the
driver joins those owners and restarts their retained disks with new process
identities. Startup independently validates the released receipt and current
Fleet authority before establishing a fresh serving quorum. It does not reuse
pending-recovery attachment for serving. An unknown release stops the workflow
before any serving restart or application request.

Pass `--published` to install and activate both split children, initialize their
archives and acknowledge newer S3 writes in each child before selecting recovery.
The replacement cohort owns both physical groups in its existing two-group
profile. Each process prepares and seeds one range at a time under the unchanged
one-operation recovery pool. After receiving the synchronized seed result, the
controller explicitly releases the exact history/plan/commitment's idle
preparation; unknown seeds retain their original input for reconciliation.
Recovery binds every replacement to its exact fenced source, restores each
cell's own new incarnation, and replays both parent and newer child receipts.
This scenario retains the same resource/fence limits and does not qualify
external-effect ambiguity or the complete CG-13/15 matrix.

The standalone controller adds `fleet_status`, transition basis/admission/status,
archive preparation/capture, recovery basis, hold preparation/acquisition,
intent staging/reopening, challenge and fence commands. A prepared hold intent
must be persisted before acquisition; an uncertain allocation keeps that exact
intent. `RaftFleetControl` checks the principal, complete current recovery basis,
selected source and namespace-owned registry. Serialized holds still require
independent complete-closure verification before intent staging. None of these
values grant startup, quorum or serving authority.

`authorize_seal` and `seal_source` run on voter-backed control endpoints and use
the SDK's physical observer and group owner. `setup_child` uses the existing
install/attach/quorum/archive SDK steps; `ManagedVoter.setup_child` records its
history under the launch/fencing lock before entry. Raw client calls cannot
bypass that assignment. All standalone substeps share their original 20-second
budget, while voter actions retain the five-second protocol budget. Unknown
mutations stop for reconciliation; no transport failure silently retries them.
An explicitly returned Accepted activation is reconciled with the same immutable
request within one 20-second workflow budget; an unknown response stops it.

The process regression is
`process::rustfs_managed_voter_process_boot_restart_and_stop` in
`native_cell_groups_disaster`. It covers both image modes, stale/unauthenticated
requests, empty-standby read/write refusals, retained engine ownership and joined
stop, plus stale capacity refusal and public qualification/activation admission.
Full managed Docker seed-to-serving orchestration and the six-case disk-loss gate
remain required for CG-13 acceptance.

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


## Node-owned automatic partition service

`RaftFleetControl::start_partition_service` owns local observation, deployment
placement callbacks, durable admission and an embedded transition worker. Start
it on each participating managed node. It binds catalog I/O to that node's
installed backend and execution domain. Its exclusive registry slot, 2 MiB
workspace and bounded sampler use the existing shared control budget. Duplicate
services and insufficient shared capacity refuse admission. Stop cancels the
current callback; shutdown joins both workers without cancelling durable work.

The planner supplies pressure, fresh operation identities and RF3 placement for
an observed recommendation and exact current catalog/directory basis. It cannot
grant capacity, membership or serving authority. Cold startup restores the
Fleet's selected request before invoking either callback. Each turn stages a
fresh selection before any child admission, or resumes the original plan.
Errors after selection preserve the rooted work and original birth identities.

Execution defaults to `AdmissionOnly`. Explicit `Complete` policy consents to
irreversible execution only for the configured principal and namespace scopes,
including restored work. Another principal refuses. The existing transition
worker still revalidates quorum, archive, seal, activation and physical release
at every owning boundary. Existing standalone transition workers and the
embedded worker share one exclusive node slot; configure one owner.

Use `--suite managed-partition-service` with an owned RustFS backend and fresh
output directory for the scoped automatic-owner case. Both image modes cover
blocked-callback cancellation/join, duplicate refusal, a lost real selection CAS
reply before child births, cold recovery on another node without new callbacks,
admission-only and foreign-principal refusal, then explicit automatic execution
through child archives and SQL/receipt/version/S3 checks. Competing selections,
safe cancellation of refused staged work, adaptive whole-group placement,
independent voter processes, reserve/fanout and sustained load remain separate
CG-11 gates.


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


## Retained cleanup and configured startup

Set `RaftHostOptions::member_cleanup` to `Some(FleetMovementServiceOptions::default())`
to start a cleanup worker with the managed host. The node registry owns its stop
and join; no external service handle is required. This consumes the movement-worker
slot. Applications starting a combined movement/topology worker should use that
worker's `reclaim_removed_members` option instead.

Before replacing a rooted boot, startup preserves the original source witness for
both active movements and queued completed cleanup. A retained successor must keep
the exact catalog/fleet/node/disk/engine, advance the boot epoch, change process and
join identity, and qualify fresh rooted capacity. Cleanup and settlement revalidate
this authority. Already committed cleanup receipts replay without another refund.
Unqualified capacity, missing original witnesses and stale processes refuse.

Use `--suite managed-retained-cleanup` against an owned RustFS backend. Its case
runs both image modes and restarts before GC, after GC/before settlement, and after
committed settlement. It checks automatic startup ownership, duplicate-worker
refusal, joined stop, stale-capacity and missing-witness refusal, and receipt replay
on a later qualified boot. Original SQL, receipts, versions and S3 writes persist.
Independent-domain, load and measured-resource qualification remain open gates.

## Configured automatic partitions

The fixed `managed-automatic-partitions` suite runs actual SQL load through the
standard configured planner, selected-work controller replacement, RF3 split,
quiet merge with different child leaders, receipt replay and continued writes.
A separate case refuses a split that would consume the recovery reserve while
keeping the parent writable. Both materialized and extent images use real RustFS.
Voters run as host-native TLS nodes with fixture capacity; this does not qualify
independent process crashes, failure domains or sustained-load profiles.
