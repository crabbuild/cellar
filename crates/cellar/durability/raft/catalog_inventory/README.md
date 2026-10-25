# Namespace birth admission inventory

Every Raft namespace has an authoritative mutable admission root and immutable
hash-radix pages. A canonical birth intent enters that root before its live or
cancelled decision can be published. The index reserves each cell name
permanently and retains pending, live and cancelled intents. It never infers
completeness from object listing or currently serving nodes.

The fleet catalog's [namespace import runtime](../fleet_catalog/imports/README.md)
uses this frozen set to settle decisions, verify committed group enrollment and
execute an exact-owner handoff. The inventory alone does **not** certify complete
fleet enrollment, authorize evacuation or release reservations. Fleet-wide
namespace coverage and lifecycle gates remain required for maintenance.

## Admission and immutable decisions

Admission confirms the permanent namespace group-ID claim, writes a canonical
intent object and bounded index pages, then conditionally replaces the inventory
root. Only an intent observed in that root may publish a decision. Orphan claims,
intent objects and pages grant no birth or voting authority. Never delete them
to reuse an identity after a losing or ambiguous admission.

The first admitted intent chooses the cell's canonical scope and initial
placement. Concurrent creators follow it. Birth and cancellation still contend
at the existing immutable per-cell decision key. A cancellation naming a
different incarnation cannot cancel a pending canonical intent; it must resolve
the admitted identity first. An existing decision returns its exact winner.

Decoding a decision also checks its canonical intent, index and permanent group
claim. Validly framed bucket objects without an indexed intent cannot mint a
`GroupBirth`. Already obtained capabilities retain their ordinary scope/owner
checks; this inventory adds no object-store call to business write acknowledgement.

## Freeze and reconciliation

`freeze_births(operation, timeout)` CAS-orders freeze against new admission.
If admission wins, its canonical intent appears in the frozen set even when its
writer has not published yet. If freeze wins, a delayed admission cannot publish
a new birth. Existing admitted writers may finish. Resolve every intent at its
exact decision key, or conditionally cancel its exact pending proposal, before
using the set as an import input. Missing replies and missing decision keys are
not absence proof.

Retry the same freeze operation after an uncertain response or adapter restart.
A different operation conflicts. The fleet runtime can claim the exact cut and
adopt it after rooted quorum-coverage verification; stopping retains the claim
and frozen references. There is no automatic unfreeze. Fresh managed namespaces
retain their immutable fleet owner and refuse ordinary freeze.

`frozen_birth_page` reads only the fixed root through opaque cuts and cursors.
Entries expose the canonical proposal and expected **live** decision digest; a
cancellation tombstone has a different digest and must be reconciled as such.
The reported count includes pending and cancelled names. A frozen birth cut is
neither a complete-import certificate nor a current-membership proof.

## Bounds and storage failures

- Checked `u64` counts retain all permanent names, including cancelled names.
  Per-call work stays bounded independently of that count. The fleet catalog's
  separate 10,000-history limit still constrains managed import admission.
- At most eight CAS attempts per admission/freeze call; an operation deadline
  also covers freeze and page reads. Unresolved results retain their identities.
- At most sixteen intents per returned page. Logical namespace, intent, birth
  and claim records obey `CatalogOptions.max_record_bytes`; immutable index
  pages have a separate fixed 64-KiB bound and depth/shape validation.
- The namespace and fleet catalogs share the same bounded radix implementation.
  Their storage adapters keep separate key spaces, error types and CAS roots.

The inventory head and permanent genesis pin are prepared before publishing a
fresh namespace root. An existing namespace cannot recreate missing inventory
state. Missing heads, pins, intent objects or pages refuse authority; a missing
occupied namespace root cannot be replaced with an empty namespace. Initialization
can resume only its exact unoccupied genesis. A backend without a usable CAS
token refuses admission. Conditional-write and exact-read semantics still need
deployed-backend qualification.

The embedding supplies a conditional-capable provider client without hidden
mutation retries, as in the native Raft performance harness. The adapter itself
does not overwrite or silently replace a failed CAS with another identity.

After adoption, the immutable namespace configuration remains unchanged. New
intents require the adopted fleet's rooted birth authorization and carry its
explicit owner in their canonical birth record. New managed births cannot use
bare bootstrap. Original imported births retain their earlier identity and
replay their committed enrollment during intact-engine recovery.

The internal protocol hard-cuts to `/fleet-quorum-1/fleet-cuts-1/empty-archives-1/archive-cuts-1`. Use fresh fleet roots,
fresh namespaces and matching binaries. Numeric format markers stay at 1;
there is no compatibility reader or state migration.
