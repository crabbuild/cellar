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
`durable_bytes`, and the explicit
`FleetDockerResourcePolicy` under `resources`. Image reservations charge eight
copies per slot. Capacity must cover those reservations and durable overhead.
The node and replay extent inventories enforce their separate explicit byte
ceilings. Overhead must cover replay storage and any node inventory above the
per-group reservation total; the generic 64-GiB inventory defaults are not used.
The resource policy is a trusted deployment bound, not a measurement. It must
account for shared logs, replay/extent inventories, temporary files, workers and
container data before capacity is qualified. Merely passing configuration
validation does not establish that the deployment can enforce those bounds.

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

## Remaining qualification

This harness exposes the deployment path; it is not a full scenario runner yet.
Measured capacity admission, RF3 writes during extent-backed recovery and
terminal removal, controller interruption, intact return, capacity changes,
multi-failure handling, 500-cell density and matched TPS/p99/soak runs remain
separate required checks. Do not infer any of them from a Registered fleet or
unit-test counts.
