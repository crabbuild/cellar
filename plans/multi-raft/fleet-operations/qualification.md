# Fleet operations qualification

## Required topology and workload

Start with five Cellar containers, one vCPU and 3 GiB each, memory swap disabled,
500 logical RF3 cells, approximately 100 leaders and 300 hosted copies per node.
Add a sixth node for scale-out and evacuate to four for scale-in. Use a dedicated
Colima profile and RustFS prefix, pinned binaries/images and fresh namespaces.
Record actual limits rather than configuration intentions. Keep other workloads
visible in samples; shared-host results cannot qualify independent-node capacity.

Provision the Colima VM for the simultaneous node resource envelopes plus RustFS,
the load generator and recovery overhead. Six 3-GiB nodes alone have an 18-GiB
combined limit; the earlier 16-GiB VM cannot cover those simultaneous envelopes
plus backend work. Record host/VM capacity and memory pressure before admission.
If the development host cannot support a tier, report it unqualified instead of
silently changing node limits or counting an overcommitted run as capacity proof.

Qualify vertical resize separately: grow one node from 1-vCPU/3-GiB to
2-vCPU/6-GiB and return it to the original profile only when retained state fits.
Also deliberately attempt an impossible reduction and require a typed refusal.
Performance at larger resource limits is a different tier, never evidence of a
same-resource optimization.

Use the existing native counter workload and ledger, then a skewed workload with
10% of cells receiving 80% of writes. Exercise at least two image sizes: the
existing counter image and a seeded 1 MiB state. Record actual physical bytes and
archive lag. Hold offered load constant across fault/movement comparisons. Include
open-loop arrivals, queueing, rejection and timeout counts; do not improve p99 by
silently dropping slow requests or excluding failed intervals.

## Safety and functional matrix

| Scenario | Required oracle |
| --- | --- |
| Concurrent operator requests | One root admission per identity; conflicting payload/revision refuses without partial admission |
| Lost CAS response and stale reads | Exact request converges or remains explicitly unknown; no duplicate reservation |
| Crash at each root/page/receipt boundary | Orphans grant no work; referenced operations remain discoverable and resumable |
| Delayed permit after cancellation | Group consumed sequence refuses the stale operation after its fence commits |
| Controller restart during seed/joint/receipt | Exact plan resumes; no member-ID reuse or freed reservation before terminal proof |
| Add sixth node under traffic | Healthy groups redistribute; new leaders are caught-up voters; all 500 histories remain accounted for |
| Kill one node | All groups retain quorum; failed memberships repair on eligible nodes without application traffic triggers |
| Kill two nodes sharing some cells | Affected minority groups refuse writes/seeding; other groups progress; S3 reachability changes no vote |
| Store outage while moving | No new root admission or false certificate; existing Raft authority continues subject to archive retention |
| Temporary network partition | Suspect/unknown states do not invent failure proof; majority histories remain consistent |
| Restart maintenance | Handoff verified, same volume/IDs resume, grace bounded, second maintenance refused |
| Extended maintenance and return | Full evacuation certificate; fresh returning registration; old disk cannot resurrect membership |
| New cell concurrent with final drain | Birth reservation prevents missed membership; final root CAS rescans or succeeds on a complete cut |
| Delete cell concurrent with drain | Retirement and tombstones preserve inventory coverage and retention obligations |
| Remove a node with stale receipt | Revision/configuration/permit conflict refuses infrastructure-removal readiness |
| Capacity or strict domain shortage | Typed blocked outcome, no partial unsafe scale-in or relaxed replica policy |
| Cancel during joint state | Finish existing joint action, retain partial effects, reject late cancelled work |
| Sustained overload | Metadata, memory, seed traffic and task counts remain bounded; consensus traffic retains service |
| Empty managed node | Readiness and trusted registration succeed without dummy cells or unbounded dormant registrations |
| New capacity at low load | Redistribution progresses even when no source is overloaded and multiple nodes tie for peak pressure |
| Receipt published before final CAS | Unreferenced candidate receipt cannot report completion or authorize removal |
| Changed proof during finalization | New candidate digest is reconciled under the same operation; only the terminal rooted digest is authoritative |
| Restart versus drain classification | Restart exclusion does not trigger immediate full evacuation through the existing draining flag |
| Stop races cancel/resume | Durable local epoch/process guard selects one result; old stop cannot kill the rejoined process |
| Restart and resize requested concurrently | Exactly one disruption slot; second planned offline operation refuses or waits |
| Rolling maintenance | No next-node stop before verified rejoin and restored redundancy of the previous node |
| Increased resource descriptor only | Placement capacity does not increase before matching observed runtime limits |
| Retained state exceeds smaller profile | Scale-down refuses despite low live-replica count; no deletion or OOM loop |
| Sole-node zone failure | Strict-domain headroom check refuses scale-in even when aggregate slots fit |
| Multiple departing nodes | No new replica targets a later departing node; complete keep-set plan precedes evacuation |
| Assignment search exhausts its budget | Typed search-limit result, no partial unsafe scale-in and no false infeasibility proof |
| Final fit but no legal move prefix | Refuse admission before copying; show the staging/retained-history constraint rather than assuming source reclamation |
| Resource sample races materialization/release | No double-credit; stale process observations retain conservative physical and reservation charges |
| Retired destination has free aggregate capacity | Refuse exact-history rebinding despite available slots; use a fresh eligible target |
| Lost accepted stop reply | Retain disruption ownership and reconcile the same request/process before cancellation or another stop |
| Survivor fails during evacuation finalization | Retain absence fences, refresh positive health proof and block completion until its actual prerequisites hold |
| Retire cells then reduce capacity | Serving counts change only after terminal proof; retained bytes/slots still constrain scale-in and memory reduction |

Verify every successful command against the retained operation ledger and all
current serving replicas, then verify archive coverage of acknowledged committed
positions. Ambiguous results may have committed: reconcile by command ID and
receipt instead of requiring counters to equal only successful HTTP responses.
No duplicate execution is permitted. Use linearizability/history checks where
the API promises them; follower reads obey their declared causal/strong contract.

## Proposed acceptance budgets

These are engineering delivery gates for this topology, not existing measured
capabilities. Set exact workload parameters and baseline before each run. Changes
to a gate require a documented scope/SLO decision, never an after-the-fact silent
threshold increase.

| Metric | Gate |
| --- | --- |
| Acknowledged data loss, duplicate execution, unsafe membership/removal | Zero in every run; any violation fails delivery |
| Healthy steady state | Maximize sustained successful TPS with p99 below 200 ms preferred, 500 ms hard ceiling; report errors separately |
| Controller enabled with no movement | At most 5% TPS loss and at most 10% p99 increase versus matched controller-disabled workload; inconclusive noisy results do not pass |
| Optional balancing candidate | At least 5% successful-TPS improvement at comparable p99, or at least 10% p99 reduction with TPS no worse than 5%, on the skew workload; no increased failure rate and no hard-ceiling violation |
| Required movement under load | Target p99 below 500 ms outside measured election unavailability; report election windows too, without silently filtering whole-run latency |
| Single failure service recovery | Every quorum-capable affected group resumes acknowledged writes within 30 seconds in the counter profile |
| Single failure redundancy recovery | All 300 missing copies repaired within 10 minutes in the counter profile; separately report larger-image time and bytes |
| Five-to-six redistribution | Achieve configured pressure deadband within 10 minutes in equal-counter profile, then no unnecessary moves for five minutes |
| Evacuation five-to-four | Complete proof within 10 minutes in counter profile with feasible domain/capacity policy; otherwise typed blocked result, never false success |
| Physical resources | Enforced 1-vCPU/3-GiB bounds, no OOM, no hidden swap; all documented queues and reservations within bounds |
| Endurance | 24-hour mixed write, repair and maintenance soak with retained history/archive checks and no unbounded growth |
| Planned capacity redistribution | Low-load five-to-six operation progresses within the existing counter convergence budget without relying on an overload trigger |
| Vertical resize correctness | Same intact identity resumes under verified new limits; failed/unsafe reduction leaves the prior effective profile and all data preserved |

For one-node-failure tests, pre-place all ten possible three-node combinations
evenly where policy permits. For two-node failures, enumerate each of the ten
pairs on small functional fixtures, then run at least one overlapping pair at
500-cell density. RF3 never promises survival of arbitrary two-node loss.
In the exact equal five-node fixture, each pair initially leaves 50 cells with
three copies, 300 with two and 150 with one. Verify that initial classification
before repair changes the placement. See the [failure-spread example](planning-details.md#spread-correlated-failures-as-well-as-counts).

Record per-stage initializer ownership, move-slot occupancy, copied bytes and
leader-transfer attempt rates. Compare measured service rates with the
[convergence lower bounds](maintenance-and-rebalancing.md#predict-convergence-from-the-limiting-service-stage).
The ten-minute targets remain unpassed until the complete workflow, retries,
archive checks and final observation windows fit. Raising concurrency is a new
candidate requiring foreground and resource verification, not an automatic fix.

Run at least three fresh matched baseline/candidate pairs, alternate AB/BA order,
warm up before timing and retain every failed cohort. Report per-run and pooled
successful TPS, bytes/sec, p50/p95/p99, errors, queueing, physical CPU/throttling,
memory, disk, control-store request/CAS conflict rates and archive backlog.
Use independent run-level uncertainty rather than treating correlated individual
requests as independent evidence. If variance cannot distinguish the proposed
gain from noise, the optional optimization stays out of the final performance PR.

## Object-store qualification

Against the exact RustFS deployment, test racing conditional creates, competing
updates using the same token, exact read-after-write, injected lost responses,
node restart and interrupted writes. Verify stale tokens fail, only one competing
update succeeds, accepted bytes are not torn, and newer referenced pages can be
read. Never infer these properties from AWS documentation or a successful PUT.
Keep catalog access separate from bulk archive traffic in measured budgets,
while preserving the actual shared backend contention in the final run.

## Evidence and delivery report

Retain artifacts under ignored `.cellar/raft-perf/fleet-operations/<run-id>/` or
external durable storage. Include source SHA/dirty diff, executable digest, image
digest, RustFS version/digest, Colima/container limits, complete policy, placement,
request ledger, fault schedule, root/operation transitions, group configuration
proofs, archive verification, resource samples and command logs. Exclude secrets.
Do not commit raw evidence under `plans/multi-raft/evidence/`.

The release report must separate implemented functionality, passed functional
gates, measured performance, failed/inconclusive runs and physical-deployment
limitations. In addition to normal completion-record fields, FO-10 requires
`qualification_report`, `qualification_sha256`, and `gates` covering
`history_safety`, `store_cas`, `single_failure`, `two_failure_refusal`,
`scale_out`, `scale_in`, `maintenance`, `steady_overhead`, `skew_gain`,
`resource_bounds`, `vertical_resize`, `lifecycle_fencing`, `low_load_distribution`,
`receipt_finalization`, and `soak_24h`, each marked `passed` with retained artifacts.
The plan validator checks report/log presence and hashes; technical review must
validate the actual measurements and conclusions.
