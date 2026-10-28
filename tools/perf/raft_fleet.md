<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Managed multi-Raft deployment harness

`raft_fleet.py` provisions three to five fresh native containers and RustFS on
the dedicated `colima-cellar-raft-perf` context. Each node has 1 CPU, 3 GiB memory,
swap disabled and automatic restart disabled. All nodes report the **same physical
host**, and the fleet explicitly uses node separation. This is a container
failure lab, not independent-host availability qualification.

The existing static `raft.py` and `raft_run.py` modes are unchanged. Setting
`RAFT_FLEET_CONFIG=/config/fleet.json` selects managed enrollment in the same
binary. Managed startup refuses retained volumes; intact return must follow the
separate maintenance protocol. Do not restart a single-use container.
An empty filesystem mounted before startup is accepted; its existing directory
does not select resume mode. The native log still rejects any retained contents.

## Provisioning

Build the current checkout with `tools/perf/raft.Dockerfile`, then pass its
immutable image ID. The runner checks the complete source manifest and executable
digest. Provisioning needs at least `nodes + 2` VM CPUs and `nodes * 3 + 4` GiB
available to the Linux Engine. It refuses an existing managed deployment and
never deletes retained containers or volumes.

```sh
python3 tools/perf/raft_fleet.py provision \
  --image sha256:YOUR_BUILT_IMAGE_ID \
  --policy /absolute/path/fleet.json \
  --output /absolute/path/external-qualification/run-1 --cells 15
```

The bounded JSON policy is the `Config` type in
[config.rs](raft_fleet/config.rs). It contains unique nonzero 16-byte arrays
`incarnation` and `physical_host`, an alphanumeric/hyphen `owner`, `nodes`,
`group_capacity`, `image_bytes`, `extent_bytes`, `replay_extent_bytes`,
`durable_bytes`, `storage`, and the explicit
`FleetDockerResourcePolicy` under `resources`. Image reservations charge eight
copies per slot. Capacity must cover those reservations and durable overhead.
The node and replay extent inventories enforce their separate explicit byte
ceilings. Overhead covers the entire metadata, shared-log and replay filesystems,
the node extent inventory, extent cache, archive replay image work and bounded container logs.
Private image reservations are charged separately. The generic inventory and
snapshot scratch defaults are not used.
The resource policy is a trusted deployment bound, not a measurement. It must
account for shared logs, replay/extent inventories, temporary files, workers and
container data before capacity is qualified. Merely passing configuration
validation does not establish that the deployment can enforce those bounds.

`storage` supplies MiB-aligned byte ceilings for `metadata_bytes`, `raft_bytes`,
`images_bytes`, `replay_bytes`, `object_store_bytes`, `snapshot_scratch_bytes`,
and `extent_cache_bytes`. The node inventory must also admit the native private
workspace's WAL and capture reservations; image size alone is insufficient.
The runner provisions separate ext4 filesystems backed by exclusively created,
random-prewritten files on the dedicated VM data disk. It checks host free space
before allocation and keeps a 4-GiB reserve. It retains loop/backing-file/UUID
identities, verifies usable capacity, and never retries formatting or deletes
files after an unknown outcome. Direct-I/O prewrites avoid filling the guest
page cache. Ext4 uses one inode per 16 KiB and a journal of 1/64 of the backing
size (minimum 4 MiB); each filesystem must still expose at least 90% usable space.
Native roots are read-only, `/tmp` is a bounded
128-MiB tmpfs, and container logs are capped. RustFS data has a separate hard
filesystem bound, with bounded volatile logs and temporary files.

These loop-backed volumes are for a single VM boot. After a VM restart, their
recorded device mapping must be re-established and verified before any retained
volume maintenance; the harness does not perform that recovery automatically.
A filesystem ceiling bounds space but does not guarantee workload headroom or
availability after exhaustion. Capacity qualification and traffic testing remain
separate operations.

If the VM data disk's host volume lacks room, `--host-backing /absolute/directory`
places only the new filesystem backing files on another host volume. First mount
that owned directory writable at the same path in the dedicated Colima profile
using virtiofs. The runner verifies host-created identity bytes inside the guest
before allocating, checks free space on that volume, and also preserves the VM
metadata volume's 4-GiB reserve. It does not move or reformat existing VM data.
This adds a host shared-filesystem layer to disk I/O; retain that topology with
benchmark evidence and use the same backing mode for matched comparisons.

For a five-node, 15-cell counter qualification, a conservative starting policy
uses 14 group slots, 4-MiB images, a 1-GiB node extent inventory and a 128-MiB
replay inventory. The following hard ceilings consume 19 GiB of backing across
the fleet; provisioning additionally requires 4 GiB of host free-space reserve.

| Pool | Per-node bound |
| --- | ---: |
| Metadata filesystem | 128 MiB |
| Shared Raft log and snapshot filesystem | 512 MiB |
| Cell images, inventory and cache filesystem | 2,048 MiB |
| Recovery replay filesystem | 384 MiB |
| Snapshot scratch within the Raft filesystem | 128 MiB |
| Extent cache within the images filesystem | 64 MiB |

RustFS gets a separate 4-GiB filesystem. The placement profile declares 2,752 MiB
of usable durable capacity and charges 2,176 MiB of non-group overhead. Each
resident group charges at least 32 MiB; a 64-MiB movement peak and 128-MiB reserve
leave room for 12 resident groups during single-node repair. Memory must still
be measured against the native buffer reservation and the actual cgroup usage.
These are configuration bounds for a small integration workload, not measured
500-cell capacity or a performance recommendation.

The 1-GiB extent inventory admits one private counter writer at a time under
the current 580-MiB main/WAL/capture reservation. The SQL owner releases an idle
writer when retaining it would prevent another equally bounded writer from
fitting. Active proposals retain their full reservation, and concurrent admission
can still return overload. Use measured workload concurrency when sizing a
throughput profile; group placement capacity is not a concurrent-writer count.

The initializer declares the extent-backed namespace and registers every node
with stable request IDs and revisions. Exact initializer retries reconcile the
original journal, even after later catalog writes; conflicting identity changes
fail. Nodes start sequentially, claim their real boots and discover authenticated
peers. Provisioning finishes only after every `/fleet` probe observes Registered
with no histories. The report records **capacity_qualified=false** and
**deployed_recovery_qualified=false**.

Evidence and process identities remain in the external run directory, whose
`config/` contains credentials. Do not commit or print its contents. Each
`container-N.json` records the exact immutable container and retained volume.
Configuration is copied into an owned Docker volume through a never-started
helper container, so external host workspace mounts are unnecessary. Native
containers mount that volume read-only. Docker failure details are retained in
the private run directory; command arguments containing credentials are not
printed.

## Operator requests

`GET /fleet` is authenticated and reports the current rooted state, native boot,
profile digest, group occupancy, installed trust revision and local recovery
diagnostics. It does not grant a health or removal proof.
Recovery diagnostics include each observed boot's health classification, direct
failure count, last inspection error and sample age. Use them to distinguish a
failed connection from stale metadata, local overload or an expired call.

`POST /fleet` accepts these explicit actions:

| Action | Inputs and boundary |
| --- | --- |
| `Capacity` | A full `FleetCapacityRequest` containing the original identity, revision and retained deployment attestation. `Qualify` admits Ready. `Activate` first binds the exact container, checks fresh Engine memory/limits and native occupancy, then invokes normal capacity admission. |
| `Reserve` | `request`, `history`, `voters` (three initial node numbers). Reserves the managed birth and resolves the physical namespace birth. |
| `Confirm` | `request`, `history`. Recovers the original reservation, reconciles physical birth and confirms it in the fleet catalog. |
| `ActivateInitial` | `history`. Registers and activates an initial member through the guarded SDK path. Send to each initial voter. This grants no replacement-membership bypass. |
| `StartRecovery` | Starts the Node-owned recovery worker with Docker isolation/resource providers and physical extent archive replay. Requires Active local capacity and complete namespace inventory. Explicitly authorizes permanent failed-boot isolation. |

`history` contains an integer `cell`, plus nonzero 16-byte `group` and
`incarnation` arrays. The namespace and class come from the compiled deployment.
`request` is `FleetRequestIdentity`: `id`, `operation`, `principal` and
`expected_revision`. Keep these exact values after a timeout or lost response.

```sh
python3 tools/perf/raft_fleet.py apply --run /absolute/path/external-qualification/run-1 \
  --node 1 --command /absolute/path/operator-request.json
```

The client syncs an immutable request record before dispatch and retains each
response. Repeating the command reconciles the same identity and payload; it
never changes the expected revision or invents a new operation after failure.
Failed mutations remain unconfirmed. The endpoint has a 16-KiB request limit,
one mutation at a time per node and a 30-second outer deadline; each native
admission retains its existing five-second budget.
An activation retry at an older revision first uses native receipt reconciliation.
An exact committed request can succeed after its process or Engine has stopped;
an uncommitted or changed request cannot use this path to activate capacity.

Node shutdown stops and joins the recovery worker; durable operations, reservations
and excluded boots remain owned. This lab endpoint does not implement intact
return or an independent controller stop/restart command.

Qualify and activate all intended nodes before reserving births. Recovery should
start after bindings and initial RF3 groups are verified. Its status is local
diagnostics; independently verify committed membership, archive coverage and
foreground writes before claiming successful healing or terminal removal.
The managed endpoint refuses the static fixture's leadership-transfer shortcut.

The scenario runner collects actual kernel limits, native startup bounds,
mounted-filesystem capacity and retained loop/UUID bindings before admission:

```sh
python3 tools/perf/raft_fleet_operations.py admit --run /absolute/path/run-1
python3 tools/perf/raft_fleet_operations.py seed --run /absolute/path/run-1
```

`admit` qualifies and activates each empty Registered node. `seed` reserves and
confirms the configured cell histories, then activates their three initial
members. These actions retain exact request payloads before dispatch; rerunning
the same mode reconciles prior receipts. Unknown mutations stop the invocation
without refreshing their revisions or generating replacement requests.
Successful activation alone does not establish elected leaders, successful
foreground writes, archive catch-up or recovery.

## Remaining qualification

This harness exposes the deployment path; it is not a full scenario runner yet.
Measured capacity admission, RF3 writes during extent-backed recovery and
terminal removal, controller interruption, intact return, capacity changes,
multi-failure handling, 500-cell density and matched TPS/p99/soak runs remain
separate required checks. Do not infer any of them from a Registered fleet or
unit-test counts.
