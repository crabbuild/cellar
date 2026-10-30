<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Fleet implementation history

Purpose: Preserve the detailed fleet capability snapshot and its original evidence limits from the former package overview.

[Package index](README.md) · [Current task inventory](tasks.json)

This retained snapshot describes its recorded sources. Use the task inventory and linked progress records for delivery status.

```mermaid
flowchart LR
    A["Pinned source"] --> B["Component observations"]
    B --> C["Recorded qualification boundary"]
```

- Status: implementation in progress.
- The design began at source `e0035689dd1eaf059fca250f2eaf05b3280899bc`; `baseline.json` tracks the reviewed successor for source preflight.
- The [implementation progress](progress.md) records the catalog, durable membership guard, trusted enrollment and terminal removal, with local verification.
- Intact operator maintenance now includes independent stop/return/resume grants and terminal operator cleanup.
- The [progress log](progress.md) records exact selected test counts, retained failures and qualification limits.
- Deployed qualification remains open.
- FO-01 through FO-03 are complete; FO-04, FO-06, FO-07, FO-08 and FO-10 are in progress.
- Healthy assignment and the explicit resumable topology controller are implemented.
- The operator SDK/admin/CLI now exposes keep-set rebalance, status, progress, wait and cancellation.
- Explicit add, status, qualification and activation commands pass local end-to-end verification; graceful evacuation, exact joined stop and terminal removal now have native SDK/API/CLI coverage too.
- Audited cancel-and-restart recovery after post-finalization inventory churn passes native end-to-end verification.
- The catalog now implements M1 intact-restart preparation, a reversible physical admission gate and synced cancellation.
- Accepted application work drains while consensus continues.
- Pending incoming work can now be inventoried, lost birth capabilities recovered from the admitted journal, and existing exact-boot moves continued through restart preparation.
- Resource-only obligations stay charged.
- Owned bounded settlement turns now retain birth decisions across controller loss, block cancellation while their outcomes are unresolved, atomically root the actual winner, and advance admitted movements with explicit service/debit blockers.
- Finish versus Cancel is an explicit immutable action choice.
- A bounded runtime inspection walk now checks the complete target physical inventory and both current survivors per voting history.
- Retired copies stay accounted.
- Owned reconciliation can now materialize missing original voters of an already-admitted Live birth, one node per turn.
- It reuses permanent member claims, verifies physical attachment after the reply, and retains the pending history across interruption.
- Claimed blank engines, dynamic replacements and changed rooted inputs refuse initialization.
- Actual both-survivor quorum and publication checks still precede historical candidate completion.
- Owned reconciliation now checks both survivors' matched progress at the actual leader, requests an eligible transfer when needed, and retains the history until a survivor is observed as leader.
- Queued work rechecks the synced gate and an expiry measured by the Raft owner's clock, tightened by actual peer sample age.
- The live matched walk can now publish M5 RestartReady after repeating physical quiescence/inventory and checking every voting sample at the Root CAS.
- Clock values stay ephemeral.
- Exact historical receipts survive lost confirmation and controller restart; generic catalog writes cannot mint or delete readiness.
- The target remains quiesced in RestartPending with placement excluded and its disruption slot held.
- Separate Root stop consent now lets the receiving Node renew physical inventory, both-survivor quorum and matched leadership before syncing its exact maintenance stop guard.
- Actual signed joined closure can be confirmed offline without releasing capacity or the disruption slot.
- M8 now admits a separate return intent and boots the retained engine under a fresh process, syncing quiescence before endpoint publication.
- Public and SDK writes remain blocked while consensus catches up.
- Exact unknown startup confirmation retains its admitted nonce.
- M9 now verifies the returned boot, actual profile, complete current inventory and all current voters' matched progress.
- It hands off regained paused leadership, records separate consent and renews proof at the receiving Node before consuming its exact synced quiescence.
- Physical reopening retains capacity exclusion and the disruption slot; fresh terminal verification restores Active capacity and releases only that slot.
- Same-scope consent refresh handles Root drift before and after reopening.
- Durable SDK preparation and cancellation now resume journaled child attempts across controller loss and competing writers.
- Gate admission and physical quiescence are separate durable steps; terminal cancellation requires the actual SDK fence/receipt and preserves capacity exclusion.
- SDK stop-consent refresh now retains the original physical selector across fresh Ready assessments, lost confirmation and actual joined closure.
- Read-only operator status reports canonical progress without driving work or restoring health proof.
- Management SDK/admin/CLI prepare/cancel/status commands and explicit Node-owned periodic preparation now have end-to-end coverage, including requester loss, Node cancellation/join, unknown admission and independent permissions.
- Waiting polls only read-only status; a cancelled preparation cannot report Ready success.
- The complete management stop/return/resume sequence now retains exact children through controller loss and unknown replies.
- Actual signed join refuses later cancellation without releasing the slot; fresh terminal resume restores capacity and clears only its own operator pointer.
- Rolling/failure/grace policy and resize remain delivery targets.
- Graceful departure now reports typed proof waiting without consuming a history or releasing the disruption slot.
- Status retains local progress only at the same Root; CLI waiting stops on unknown mutation outcomes.
- Current native qualification passes, while the underlying intermittent observation cause and deployed qualification remain open; the [progress log](progress.md) records their exact limits.
- `tasks.json` retains exact package status.
- Controller-driven spare selection, admission and settlement after trusted isolation are implemented as an explicit SDK service.
- Independent required repair can use a remaining slot during an admitted action's retry/backoff, without releasing that action's debt.
- An explicit Node-owned failure monitor now classifies exact-boot connectivity with bounded authenticated probes, grace and sample expiry.
- An explicit recovery service now roots permanent exact-boot exclusion, reconciles trusted isolation and drives the existing repair executor.
- It resumes its own intent after controller loss, never adopts operator work and can isolate during retry windows with both movement slots full.
- After repair, bounded complete-inventory walks verify fresh RF3 observations and physical archive replay before automatic finalization and terminal removal.
- Controller loss and unknown acknowledgements reconcile through canonical receipts.
- Only the terminal transition releases the owning disruption slot; physical storage stays retained.
- A Docker isolation adapter now supplies durable exact-container binding and the bounded shutdown effect for exclusively owned, single-use native nodes.
- Real Engine tests verify paused-container isolation, retained data and adapter recreation.
- The [managed Colima harness](../../../../tools/perf/raft_fleet.md) now wires fresh startup, capacity operations, exact-container binding and opt-in recovery into the native deployment.
- Five nodes reach measured Active capacity with verified CPU/memory/filesystem bounds.
- A 15-cell RF3 campaign now passes single-node detection, isolation, nine replacement movements and terminal removal under foreground writes, followed by all 45 replica/archive readbacks.
- The [progress log](progress.md) records the approximately 90-second campaign and exact limits; large-fleet, repeated-failure and performance qualification remain open.

- Extent-backed managed namespaces now retain tagged publication cuts and stored receipts, exact producer image origins, and independent physical archive proofs.
- Dynamic registration selects the node's extent owner; empty managed hosts require that owner before boot admission.
- Existing namespace import, populated-image movement/removal and intact maintenance use the same lifecycle contracts as materialized images.
- Canonical-empty cold repair now uses actual resource-backed exact-boot permits and can finish before the first business write.
- The [progress log](progress.md) records the completed selected checks and remaining terminal-wait/large-fleet qualification limits.
- These capabilities remain opt-in.
