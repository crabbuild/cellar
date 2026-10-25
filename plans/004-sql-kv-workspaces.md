<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# SQL and KV workspaces

Status: DONE. Started against `35fc539` on 2026-10-14.

## Requested result

- SQL: persistent schema navigation, separate query tabs, a resizable results
  pane, SQLite syntax highlighting and schema completion, run-selection keyboard
  shortcuts, bounded session query history, and EXPLAIN QUERY PLAN.
- Results: sorting and filtering of the loaded rows, full-value inspection,
  CSV and JSON export, with existing node-side execution/result budgets intact.
- KV: a persistent prefix/key list beside a selected-key editor. Explicit drafts
  load value, metadata, storage tag and absolute expiry together. Text, JSON and
  binary views, JSON validation/formatting, byte counts, preserve/never/absolute
  expiry choices, and a before/after review precede writes.
- KV concurrency: the owner compares the observed revision inside the serialized
  mutation. SDK, bulk and operator puts all assign revisions. Delete/recreate,
  expiration, blob values and owner changes cannot authorize stale edits. Existing
  unconditional SDK/CLI operations retain their public signatures and semantics.

SQL parameter binding, arbitrary-query continuation and editable SQL table rows
are later extensions described in the request, not part of this workbench pass.

## Implementation boundaries

Compose dedicated SQL and KV workspace components under the existing resource
route. Keep query/draft/history data in session memory and scoped to the selected
fleet, application and binding. Load the browser code editor only for data
workspaces; keep the framework free of JavaScript application engines.

KV revisions and mutations remain in the namespace cell's SQLite transaction and
output gate. Inspection captures bytes and revision from the same stored row.
Conditional writes use explicit preconditions; missing preconditions remain
unconditional for existing callers. Preserve expiration and storage tags in the
owner, rather than approximating absolute expiry with a new TTL in the browser.
Requests pin the inspected binding identity across deployments. Definite owner
revision conflicts are audited as rejected; lost write replies remain unknown.
All owners must run the revision-aware framework before conditional console edits
are enabled; mixed old/new framework ownership is not qualified.

## Verification and completion evidence

- Browser behavior: tab isolation, schema/column completion, selection shortcuts,
  query plans/history, resizable keyboard controls, sorting/filtering, exact
  CSV/JSON downloads, full-value dialog, narrow/light/dark layouts.
- KV browser behavior: selecting and drafting text/JSON/binary values, metadata,
  expiry preservation, absolute expiry, diff review, cancel/discard, stale reads,
  conflict recovery without automatic mutation retry, and permissions.
- Native KV: revisions across SDK and operator writers, conditional put/delete,
  stale and absent conditions, delete/recreate, metadata/tag/expiry preservation,
  expired keys and large-value publication. Exercise handoff through the existing
  authenticated management integration surface.
- Admin: typed input validation, data permissions, definite conflict versus
  uncertain transport outcomes, operation audit and owner-routed requests.

Use the checkout's stable writable target directory from AGENTS.md and run:

```sh
cargo format --check
python3 tools/check_native_dependencies.py
python3 tools/docs/check.py
cargo check --workspace --all-targets --all-features --locked
cargo test -p cellar --test native_kv --test native_node --test native_cli --test native_artifacts --locked
cargo test -p cellar-admin --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
npm --prefix packages/console ci
npm --prefix packages/console run build
npm --prefix packages/console run lint
npm --prefix packages/console run test:ui
```

### Completed checks — 2026-10-14

- Frontend dependency installation, production build and lint pass. All 27
  Playwright fixture tests pass, including schema/table/column completion, query
  tab isolation, selected/whole-editor shortcuts, plans/history, keyboard resize,
  numeric sorting, column filtering, full values and exact CSV/JSON downloads.
  KV coverage includes JSON formatting/validation, binary bytes and UTF-8 BOM,
  expiry policies, metadata null versus absence, review/cancel/discard, stale
  reads, conflict comparison/rebase, oversized values and read-only permissions.
- All 12 management API tests pass. Typed KV inputs and grants are checked before
  dispatch; a changed namespace binding rejects the old draft before dispatch.
  Omitted identity fields retain the legacy operation fingerprint.
- Native KV passes with SDK/bulk/operator revision changes, conditional edits and
  deletes, absence, expiry, delete/recreate, exact expiry/metadata preservation,
  blob publication and reactivation. A private inactive fixture prepared with the
  pre-revision schema is evolved by its owner and preserves its stored bytes.
- All 23 native node tests pass in a serial run. Real two-node management tests
  retain the revision across graceful and forced ownership changes, accept a
  current edit on the successor, and reject stale puts/deletes. They also execute
  the workbench schema query and EXPLAIN through the protected SQL console.
  One unrelated HTTP deadline test timed out in a parallel run and passed both
  its isolated rerun and the complete serial suite.
- Native CLI (2), artifacts (5), SQL (1), and the typed KV validation test pass.
  Workspace all-target/all-feature check and Clippy with warnings denied pass.
  Cargo format, native dependency and documentation guards pass. The all-feature
  dependency graph has no JavaScript application engine. Cargo.lock is unchanged.
- The private Docker preview runs eight nodes with 1 vCPU and 2 GiB each.
  Nodes had fenced after local object-store errors when the Docker VM filled.
  Reclaiming this checkout's compilation cache and restarting retained all data
  volumes; no other checkout's cache or fleet data was removed. All 10,000 saved
  counter receipts were read back unchanged, with eight current leases afterward.
  The live browser browsed guestbook schema/rows and saved/reloaded a JSON KV draft.
  A real intervening SDK put caused HTTP 409 and an audited `rejected` operation;
  conditional deletion of that temporary proof key also passed.
- Live light/dark desktop and 390px mobile review found no page overflow. Primary
  SQL and KV mobile controls measure 44px high. Browser viewport and theme were
  restored after verification. Screenshots remain private under the ignored
  preview directory; the deployment has no fixture data provider.

Browser fixtures alone do not prove owner or durability behavior. Full SDK
workspace tests/doctests and an external simulation/conformance harness were not
run for this pass. Mixed old/new framework ownership is not qualified; upgrade
all owners before enabling conditional edits. SQL row editing, parameters and
arbitrary-query continuation remain separate work.
