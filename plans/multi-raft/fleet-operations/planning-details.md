# Capacity planning and execution details

Status: proposed implementation contract. This document expands the algorithms,
worked examples and proof boundaries in [capacity and scaling](capacity-and-scaling.md)
and [maintenance and rebalancing](maintenance-and-rebalancing.md). It specifies
acceptance work; it does not claim these fleet workflows already run. See the
[implementation progress](progress.md) for delivered code and verification.

## Count logical cells, serving copies and retained histories separately

Use `L` for logical, non-retired cells and `N` for eligible Cellar nodes.
Each stable group has exactly three voting members and one leader. With equally
weighted cells and unconstrained placement:

```text
total serving copies = 3 * L
average serving copies per node = 3 * L / N
average leaders per node = L / N
```

These are lower bounds for planning, not memory sizing or throughput estimates.
Leader execution, follower application, idle-group overhead and archive work
have different costs. Temporary learners and retained histories are additional.

| Logical workload | Nodes | Stable copies | Average copies/node | Average leaders/node |
| --- | --- | --- | --- | --- |
| Initial five-node qualification | 5 | 1,500 | 300 | 100 |
| Same 500 cells after expansion | 6 | 1,500 | 250 | 83 or 84 |
| Same 500 cells after scale-in | 4 | 1,500 | 375 | 125 |
| Original 1,500-cell, three-node performance topology | 3 | 4,500 | 1,500 | 500 |

The last row is a different workload tier. Passing the five-node, 500-cell
qualification does not qualify 1,500 cells or the original per-node density.

During `A,B,C -> B,C,D`, learner D raises that group's materialized copy count
to four before A leaves. Afterward A may retain its durable history, so physical
occupancy can remain four copies even when there are only three serving voters.
Neither a successful move nor a lower active-cell count proves reclaimed space.

These calculations use the declared node-failure model. Five containers on one
Colima host are five process failure units, not five independent physical hosts.
The deployment's trusted failure-domain labels determine what placement can
claim about host or zone loss.

## Build an explicit resource ledger

Maintain separate counters for live voters, learners, dormant registrations,
retired bindings, durable engine slots, physical bytes and runtime reservations.
Some counters overlap: a learner can already own an engine slot and image bytes.
Do not sum role counts and physical counts as though they were disjoint resources.

For node `n` and hard resource `r`, use this admission expression:

```text
charged(n,r) = observed_physical(n,r)
             + unmaterialized_growth_of_existing_actions(n,r)
             + peak_increment_of_candidate(n,r)
             + policy_reserve(n,r)

require charged(n,r) <= verified_capacity(n,r)
```

`policy_reserve` is resource-specific and explicit, not an arbitrary percentage
silently added by each controller. Do not reserve a full additional failed-node
image on every node: model each declared failure scenario separately and verify
its complete assignment. Actual admitted actions and retained state appear in
every scenario in which they still occupy that node.

Each action has a reservation ledger keyed by permit, node, process/profile
revision and resource. Its lifecycle is:

| State | Accounting rule |
| --- | --- |
| Reserved, not initialized | Charge the full peak incremental bound |
| Materializing | Keep the bound; reduce its unmaterialized part only against attributed physical observations |
| Serving target | Charge observed occupancy plus any remaining tail/staging/worker obligations |
| Complete or aborted | Release unused growth only after physical workers join and terminal group proof exists |
| Retained source or failed target | Continue charging physical bytes, occupied slots and retained runtime owners |

A reservation release and a physical sample must not both credit the same bytes.
An older sample cannot overwrite a newer high-water observation for the same
action. A sample from an earlier process is not evidence that the current
process released memory. If attribution is incomplete, keep the conservative
charge and report why. Real host admission can still refuse a catalog-approved
plan; that refusal never permits exceeding a host bound.

Example: a node has 250 serving groups and 50 retained histories, consuming 300
engine slots. Two new actions require one slot each, and policy reserves another
100. Charge 402 against a 500-slot bound, not 352. Byte and memory ledgers must
pass independently. A history with a retired binding is ineligible on that engine
even if its other 98 slots are free; a fresh member ID does not reset that slot.

## A complete plan includes a legal execution path

Snapshot the following inputs before planning:

- Root, policy, trusted-node and inventory revisions; active operations and exact
  reservations, including uncertain actions and pending births/retirements.
- Each node's immutable storage identity, process/profile revision, eligibility,
  occupied-history set, hard resource bounds and complete observation windows.
- Each live group's scope, current configuration/revision, existing repair,
  intended leader and conservative image/log/staging estimates.
- Requested final node set, failure-domain policy, recovery contract, objective,
  planning/search limits and observation uncertainty margins.

Store their digests in the preview. Missing identities, incomplete scans and
unknown occupancy are missing prerequisites, not optimistically empty nodes.
For large inventories, stream immutable pages through the bounded metadata
budget; persist a cursor instead of retaining all decoded proof objects.

The planner must produce both a final assignment and a move order. Final fit
alone is insufficient: destination staging, retained source slots or existing
reservations can make every first move illegal. Such a plan cannot start by
assuming that later moves have already released resources.

```text
preview(request, root, observations):
    validate complete input and current policy
    pin final keep-set and every departing node
    reconcile existing admitted work into the starting ledger
    build eligible destination sets, excluding retired bindings
    find complete final RF3 assignment under bounded search
    check immediate quorum survival for each declared failure scenario
    find RF3 restoration assignment for each scenario required by policy
    simulate move prefixes, including fourth copies and retained state
    reject if no legal staged path or search budget is exhausted
    return input digests, move DAG, resource peaks and limiting constraints
```

Use fewest eligible destinations first, then largest reservation and stable
scope ID. Candidate ordering prefers lower projected pressure and fewer bytes
to copy, with stable node ID as the final tie break. This ordering makes repeated
planning deterministic; it is not a proof that greedy placement always finds a
solution. Search exhaustion remains distinct from demonstrated infeasibility.

The move DAG records dependencies such as learner completion before source
retirement, or preceding evacuation verification before another planned stop.
Simulate bounded concurrent execution, not all planned moves in parallel. The
initial profile has two active membership moves; actual permits are reserved
incrementally. Do not reserve all 300 moves merely because the plan lists them.

Before each permit, reread its proof-relevant inputs. Changed configuration,
target process/profile, occupancy, policy or reservations requires a new decision.
Unrelated root writes can retry CAS without changing the request identity. A new
failure or material workload change can require a revised remaining plan; keep
already committed effects and expose the new plan digest. A stale preview cannot
override target exclusion or manufacture additional slots.

For `A,B,C -> B,C,D`, validate the old and new voter triples against their domain
policy. The temporary union can contain four members without requiring four
distinct zones when the stable policy requires three. Learner D still needs a
distinct eligible node/host according to policy. During joint consensus, require
the old majority and the new majority using the actual committed configurations;
do not substitute a generic union-majority rule. A lifecycle exclusion never
removes an old voter's quorum obligation before its configuration change commits.

## Spread correlated failures as well as counts

Balance leaders and serving copies separately. For an equal five-node fixture,
place 50 cells on each of the ten possible three-node voter sets. This yields
300 copies per node and 100 leaders each after leader distribution. It also makes
the failure test reproducible rather than accidentally protecting the chosen
failed pair with a favorable placement.

For any failed pair in that exact fixture:

| Surviving copies of a cell | Number of cells | Ordinary behavior |
| --- | --- | --- |
| Three | 50 | Continue normally |
| Two | 300 | Retain quorum; repair when eligible capacity is feasible |
| One | 150 | Refuse quorum operations; restore original durable members |

All 500 cells survive any single-node loss with quorum. Arbitrary two-node loss
is not an RF3 guarantee. Do not describe restoring 300 quorum-capable groups as
recovery of the 150 minority groups. Reachable RustFS archives do not change this
classification. Uniform pair exposure limits concentration; domain constraints,
image sizes and heterogeneous capacity can require a different distribution.

Include maximum shared-pair exposure in placement diagnostics and use it as a
tie breaker among otherwise equivalent feasible assignments. V1's hard contract
remains RF3 plus the declared node/domain failure policy. It does not promise
that optional pair-spread optimization improves TPS or masks two failures.

## Worked scale-out: five to six nodes

Assume 500 equal cells, compatible nodes A–F, feasible domains, sufficient
physical headroom and no existing retired-binding conflicts. A–E begin with 300
serving copies and 100 leaders each. F begins with an authenticated empty-host
inventory and a verified 1-vCPU/3-GiB profile.

1. Register and qualify F without changing membership. If qualification fails,
   A–E continue serving and F remains excluded.
2. Activate F, then submit `balanced` capacity convergence. The preview targets
   250 copies per node and approximately 83/84 leaders. `capacity_only` is a
   different requested result and can leave F unused.
3. Choose 250 groups whose source replacements can populate F under domain and
   occupancy constraints. Each original node relinquishes about 50 serving
   copies. This is the minimum number of replacements for exact equal-copy
   placement because F must receive 250 copies; additional policy constraints
   may require more work.
4. Execute the learner/joint protocol at the bounded rate. New-cell births can
   use F once admitted and reduce the redistribution still required. Recompute
   the remaining assignment rather than copying a fixed stale list.
5. Once F has caught-up voters, transfer selected leadership to them. A route
   refresh or successful seed download alone never makes F a leader.
6. Verify the declared placement tolerance over three fresh windows, settle
   permits, verify archive cuts and publish the rooted terminal receipt.

One balanced endpoint has 1,500 serving copies and up to 250 additional retained
source histories: approximately 300 physical slots remain on each original node,
plus 250 on F. Further staging/log/retention overhead is separate. An operator
request to redistribute replicas can succeed without lowering old-node disk
occupancy. It cannot claim throughput improvement without a matched measurement.

### Exact equal-cell redistribution schedule

The following assignment is a concrete reference fixture, not a universal
production placement. Start with 50 cells on each A–E voter triple, enforce node
separation, and assume all proposed targets have unused history bindings and
sufficient staging/retention capacity. In each row, deterministically select 25
of that triple's 50 scopes and replace the indicated voter with F. The other 25
keep their original triple. Actual admission still executes bounded permits,
not a 25-group simultaneous transition.

| Original triple | Copies replaced | Departing voter | New triple |
| --- | --- | --- | --- |
| A B C | 25 | C | A B F |
| A B D | 25 | B | A D F |
| A B E | 25 | A | B E F |
| A C D | 25 | A | C D F |
| A C E | 25 | E | A C F |
| A D E | 25 | D | A E F |
| B C D | 25 | D | B C F |
| B C E | 25 | B | C E F |
| B D E | 25 | E | B D F |
| C D E | 25 | C | D E F |

This produces exactly 25 cells on each of the 20 six-node voter triples:
250 serving copies per node, 250 replacements total and 50 serving copies
relinquished by each original node. Spread leadership separately to four nodes
with 83 leaders and two with 84, subject to caught-up eligible voters. No cell
changes history and no replica-factor reduction is involved.

The exact symmetric fixture reduces the number of groups containing any given
node pair from 150 to 100. Any failed pair then initially leaves 100 cells with
three surviving voters, 300 with two and 100 with one. This spreads exposure;
the 100 minority groups still cannot progress through ordinary RF3 recovery.
Strict host/zone policies can forbid this table, and unequal cells require a
weighted assignment. Neither the count change nor pair spreading proves a TPS
or p99 improvement.

## Worked scale-in: five to four nodes

Assume the same equal fixture and choose E to leave. Pin A–D as the keep-set.
An alternative node choice is reported as a different preview; do not silently
replace an explicitly named victim.

1. Check all 500 histories, current reservations and E's approximately 300
   memberships. Prove final RF3 fit and the configured one-node-failure recovery
   scenario on A–D, including retained and temporary resources.
2. Exclude E as a destination, acquire the disruption slot and settle older
   incoming work. E remains connected for its current consensus and copy work.
3. Move those 300 memberships to eligible nodes outside each group. Equal
   placement adds roughly 75 serving copies to each survivor. Transfer E's
   leaders to eligible caught-up voters when possible.
4. Check complete absence, settle/fence every E-related action and verify fixed
   published/archive cuts. Finalize evacuation, then separate removal/revocation.
5. Only the terminal removal result authorizes platform decommission. Preserve
   E's volume and identity tombstones; survivors now average 375 serving copies
   and 125 leaders, before considering unequal cell costs.

The failed-node scenario on this four-node final fleet needs 500 copies on each
of three survivors. A nominal 500-slot limit has no extra room for retained
histories or slot reserves and can therefore fail admission. The count equality
is not a promise that this reduction fits the 1-vCPU/3-GiB resource envelope.

### Exact equal-cell evacuation schedule

For the original five-node fixture, the six triples containing E each have 50
cells. Split each row into two deterministic 25-cell subsets and replace E as
shown. The four triples already wholly on A–D keep their original 50 cells.
This is a separate five-to-four example, not the reverse of the six-node table.

| Original triple | First 25-cell target | Other 25-cell target |
| --- | --- | --- |
| A B E | C, producing A B C | D, producing A B D |
| A C E | B, producing A B C | D, producing A C D |
| A D E | B, producing A B D | C, producing A C D |
| B C E | A, producing A B C | D, producing B C D |
| B D E | A, producing A B D | C, producing B C D |
| C D E | A, producing A C D | B, producing B C D |

Each remaining triple receives 75 cells and ends with 125. Each survivor receives
75 new copies and ends with 375; E relinquishes 300. All 300 target histories
were absent from their destinations in this fresh fixture, which avoids exact
history rebinding. Each group still needs ordinary seed, catch-up and committed
membership transitions. E retains its old 300 histories until separately
authorized retention/reclamation permits otherwise.
In this fresh fixture the final physical footprint can therefore include 1,500
serving copies plus E's 300 retained histories, before log/image staging overhead.

Leadership can finish at 125 per survivor, but image sizes, load and policy may
justify unequal leader counts. Losing one of these four nodes then requires
500 serving copies per remaining node for RF3 restoration. The complete planner
must still charge growth, retained state and recovery reserve; the table does
not grant a 500-slot node enough additional headroom by arithmetic alone.

For several departures, pin the final keep-set first and exclude all departing
nodes from new placement. Evacuate only one at a time. An earlier admitted permit
may still finish on a future departing node; reconcile it and move that result
away. No new plan copies a group onto the next departing node merely because it
is still serving. A changed keep-set requires explicit replanning and the same
single-failure checks.

## When the number of cells falls

Distinguish lower request rate from authorized cell retirement. An idle cell
keeps RF3, its group identity and its durability obligations. No fleet controller
infers deletion from an empty traffic sample or an application-level row count.

For actual retirement, the application lifecycle operation reserves the intent,
commits the terminal marker after accepted commands, verifies its application
on all current voters and its archive dependencies, then roots the retirement
receipt/tombstone. Until that completes, count it as live and include unresolved
actions. Missing voters require repair or a blocked outcome. Keep retained local
slots and bytes charged after logical retirement.

Use cell count only as an initial sizing bound. If all nodes had `K` usable
serving slots after physical obligations/reserves, RF3 restoration after one
failure would require:

```text
N >= max(4, 1 + ceil(3 * L / K))
```

This bound ignores bytes, domains, unequal load and per-history eligibility; the
complete planner is authoritative. With `K=480`, 500 cells need at least five
nodes under that bound, whereas 300 cells pass the four-node count bound. If
retiring 200 cells leaves 300 occupied histories on each old node, those old
nodes do not suddenly obtain the storage/memory footprint of a fresh 300-cell
fleet. A smaller final tier can still require fresh replacement nodes or waiting
for separately supported reclamation.

Here `K` excludes retained/dormant occupancy and required reserves. Existing
serving copies belong to the `3 * L` demand; do not subtract them again when
calculating the usable serving limit. Heterogeneous nodes require the complete
resource assignment rather than substituting an average `K` into this bound.

Report three independent recommendations: keep current capacity, propose a
horizontal evacuation, or propose a vertical resize. Include retained footprint,
single-failure fit, observed workload and estimated movement in each preview.
Operator approval of a target is a normal management mutation; the controller
does not provision infrastructure or lower resource limits automatically.

## Size heterogeneous nodes from measured role costs

Treat fixed host cost, per-serving-copy cost and incremental leader cost
separately. For workload estimates, an initial model is:

```text
estimated node demand = fixed host work
                      + sum(follower-equivalent work for every local voter)
                      + sum(incremental execution work for local leaders)
                      + admitted archive/seed/controller work
```

Estimate those terms over the same complete window and retain an uncertainty
margin. Do not count an entire cell's apply cost again when it becomes leader.
Do not subtract follower apply work when only leadership moves. Physical CPU,
throttling, disk and network samples bound model usefulness; a shared RustFS
bottleneck is not spare node CPU.

A 2-vCPU/6-GiB node can receive more obligations only after its observed profile
is accepted. It does not automatically deserve twice the leaders, twice the
copies or twice the TPS of a 1-vCPU/3-GiB node. Solve capacity-weighted placement
within hard constraints, then measure whether leader movement actually reduces
the congested resource. A single hot cell retains one leader and one ordered
history; node-count expansion does not split its write execution.

For memory reduction, preview the exact target startup configuration, fixed
pools, decoder/image bounds, retained engine group count and joined-worker cut.
Moving leadership may lower CPU pressure while doing little for these bounds.
When physical fit fails, keep the original limits and return
`RetainedStateCapacity`. Do not use a successful low-RSS interval as proof that
the next snapshot or recovery fits a smaller limit.

## Removal proof: monotonic absence and fresh health

No global simultaneous snapshot of 500 independent Raft groups is required.
Instead, establish an exclusion boundary, eliminate every pre-boundary action,
then accumulate per-group proofs whose absence remains true under that boundary:

1. Root the node's exclusion and operation epoch. Subsequent birth/placement
   admissions cannot target it. Keep earlier reservations discoverable.
2. Enumerate the complete managed inventory and conservative node references,
   including pending births, learners, outgoing joint voters and unresolved
   actions. A cached list of serving leaders is insufficient.
3. For each affected history, settle its exact permit and get a fresh quorum
   barrier proving a stable configuration without the departing node. Finish
   joint state; fence unused permits before releasing them.
4. Bind a committed verification cut to that configuration and check three
   surviving published positions plus archive coverage. New writes need not
   stop at this fixed cut. Recheck configuration changes before finalization.
5. Join reachable seed workers. For unreachable workers, prove their old
   activation/membership authority fenced; retain their uncertain physical
   occupancy until process exit or infrastructure isolation is established.
6. Enter `Finalizing` only with a complete scan, zero relevant admissions and
   current policy/exclusion inputs. Publish the candidate receipt and terminal
   CAS using the existing three-step protocol.

Absence can remain valid while the node stays excluded and old capabilities
remain fenced. Positive health and capacity observations still expire. A later
failure invalidates survivor-readiness checks even when the absent-node proof
remains true; revalidate before issuing completion. An unrelated business write
does not invalidate the fixed committed cut. A new relevant reservation, policy
change or untracked history does invalidate the attempted certificate.

The terminal receipt is historical proof of safe removal at finalization, not
a promise that survivors will never fail afterward. `Evacuated` remains excluded
until removal or an explicit reconciled lifecycle result; it cannot receive new
memberships between certificate and removal. Returning its hardware as general
capacity uses the fresh node/disk/engine path and retains the old volume.

## Required implementation and qualification checks

The [task inventory](tasks.json) names the owning native cases. In addition to
existing happy paths, require staged-path capacity refusal, reservation/sample
non-duplication, retired-binding exclusion, failed-pair classification, exclusion
closure during finalization, and scale-in after cell retirement. These are future
runtime acceptance cases until their packages and retained evidence pass.

The qualification report must show requested versus actual live placement,
retained occupancy, worst intermediate reservations, refusal reasons and the
configured recovery policy. Report TPS/p99 before, during and after movement at
the same offered load and resource tier. Counts and mathematical examples in
this document are not measured performance results.
