# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Synthetic histories check the evidence boundary; they do not qualify voters."""

from copy import deepcopy
import json
import unittest

import cell_groups
from read_cohort_evidence import CASE, PREFIX, observations


def fixture(mode, full):
    run = f"extents={str(mode).lower()},full={str(full).lower()}"
    rows = []

    def emit(event, **fields):
        row = dict(schema=1, case=CASE.removeprefix("rustfs_"), run=run, event=event, **fields)
        rows.append(row)
        return row

    def history(cell):
        return f'CellHistory {{ cell: CellId("Counter:{cell}") }}'

    receipts = []
    for index in range(10 if full else 4):
        cell = "one" if index % 2 == 0 else "two"
        receipts.append(emit("receipt", id=f"command-{index}", value=index + 1,
                             version=f"CellVersion {{ history: {history(cell)}, sequence: MutationSequence(2) }}",
                             witness=f"exact-witness-{index}", required="S3", achieved="S3"))
    if full:
        rows.extend(deepcopy(receipts[:3]))
    calls = []
    if full:
        for ingress in ("Sdk", "Native", "Axum"):
            calls.extend((ingress, status) for status in ["selected"] * 7 + ["not_leader"] * 2 + ["cancelled"])
        for ingress in ("Sdk", "Native"):
            calls.extend([(ingress, "selected"), (ingress, "not_leader")])
    else:
        for ingress in ("Sdk", "Axum"):
            calls.extend([(ingress, "selected"), (ingress, "not_leader")])
    for ordinal, (ingress, status) in enumerate(calls, 1):
        identifier = f"{ordinal:032x}"
        cell = "one" if ordinal % 2 else "two"
        emit("request", invocation=identifier, ingress=ingress, history=f"Ok({history(cell)})",
             required="linearizable_fresh_quorum", timeout_ms=5000, max_attempts=1,
             attempt_ms=5000 if ingress == "Sdk" else 4000, requested_member=1)
        if status == "cancelled":
            emit("caller_cancelled", invocation=identifier, waiter_joined=True)
        else:
            result = emit("result", invocation=identifier, actual_invocation=identifier, disposition=status)
            if status == "selected":
                proof = receipts[0 if cell == "one" else 1]
                result.update(cell="Counter:" + cell, value=proof["value"], sequence=2)
                emit("verified_result", invocation=identifier, value=proof["value"], version=proof["version"],
                     witness=proof["witness"], served_by=1)
    if full:
        for _ in range(12):
            emit("invalid_request", status=400)
        for _ in range(3):
            emit("pending_barrier_cut", before=[0, 0], after=[2, 0], callbacks_before=0, callbacks_after=0)

    counters = [[0, 0], [0, 0]]
    queries = 0
    term = 1
    membership = 1

    def owner(phase, local=0, count=0):
        nodes = []
        for index in range(3):
            def budget(items, maximum):
                return dict(items=items, bytes=items * 4096, max_items=maximum,
                            max_bytes=64 * 1024 * 1024 if maximum == 256 else 65536)
            nodes.append(dict(node=f"{index + 1:032x}", disk=f"{index + 11:032x}",
                              process=f"{index + 21:032x}", fleet_boot=f"boot-{index}",
                              group=dict(history="exact group history", term=term, membership_revision=membership),
                              owners=dict(runtime=dict(read_pins=budget(count if index == local else 0, 256)),
                                          host=dict(controller=dict(ordinary=budget(count if index == local else 0, 2)))),
                              counters=dict(cell_linearizable_read_cohorts_started=counters[min(index, 1)][0],
                                            cell_linearizable_read_cohort_joins=counters[min(index, 1)][1])))
        emit("owners", phase=phase, request_node=local, nodes=nodes, callbacks=dict(queries=queries))

    if full:
        for ingress in ("Sdk", "Native", "Axum"):
            for cancel in (False, True):
                owner(f"prequeued_start_{ingress}_cancel={str(cancel).lower()}")
                owner("prequeued_admitted_behind_sync", count=2)
                counters[0] = [value + 1 for value in counters[0]]
                owner("shared_barrier_independent_pins", count=2)
                if cancel:
                    owner("cancelled_waiter_retains_physical_pins", count=2)
                queries += 2
                owner("prequeued_drained")
            owner(f"late_start_{ingress}")
            counters[0][0] += 2
            owner("late_arrival_has_second_barrier_and_pin", count=2)
            queries += 2
            owner("late_drained")
            owner(f"pending_late_start_{ingress}", local=1)
            owner("pending_late_sync_entered", local=1)
            counters[1][0] += 1
            owner("pending_late_first_barrier_held", local=1, count=1)
            counters[1][0] += 1
            owner("pending_late_two_barriers_before_any_callback", local=1, count=2)
            queries += 2
            owner("pending_late_drained", local=1)
            owner(f"term_start_{ingress}")
            owner("term_reads_admitted_behind_sync", count=2)
            term += 1
            owner("term_refused_and_drained")
    owner("configuration_start_[Sdk, Native]" if full else "configuration_start_[Axum, Sdk]")
    owner("configuration_reads_admitted_behind_apply", count=2)
    owner("configuration_committed_and_data_admitted_before_apply", count=2)
    membership += 1
    owner("configuration_refused_and_drained")
    queries += 2
    owner("configuration_fresh_reads_verified")
    emit("completed", joined=True, external_store=True, commands=10 if full else 4, queries=queries)
    return rows


def history():
    return [row for mode in (False, True) for full in (True, False) for row in fixture(mode, full)]


def output(rows):
    return "\n".join(PREFIX + json.dumps(row) for row in rows)


class ReadCohortEvidenceTests(unittest.TestCase):
    def refused(self, rows):
        with self.assertRaises(ValueError):
            observations(output(rows), CASE)

    def test_named_suite_requires_four_complete_joined_fixtures(self):
        rows = history()
        self.assertEqual(cell_groups.SUITES["routing-read-cohorts"][1], (CASE,))
        self.assertEqual(cell_groups.routing_observations(output(rows), CASE), (rows, 4))

    def test_partial_duplicate_and_memory_fixtures_refuse(self):
        rows = history()
        for bad in ([], rows[:-1], rows + [rows[-1]], rows[:184], rows[:-1] + [rows[0]]):
            self.refused(bad)
        rows[-1]["external_store"] = False
        self.refused(rows)

    def test_wrong_schema_join_and_original_budgets_refuse(self):
        for event, field, value in [("completed", "schema", True), ("completed", "joined", False),
                                    ("completed", "commands", True), ("request", "timeout_ms", 6000),
                                    ("request", "max_attempts", 2), ("request", "attempt_ms", 6000),
                                    ("request", "requested_member", True), ("invalid_request", "status", 200)]:
            with self.subTest(event=event, field=field):
                rows = history()
                next(row for row in rows if row["event"] == event)[field] = value
                self.refused(rows)

    def test_sdk_recovery_cannot_replace_native_or_axum_recovery(self):
        for ingress in ("Native", "Axum"):
            rows = history()
            row = next(row for row in rows if row["event"] == "request" and row["ingress"] == ingress)
            row.update(ingress="Sdk", attempt_ms=5000)
            self.refused(rows)

    def test_native_ledger_requires_explicit_memory_scope(self):
        rows = history()
        for row in rows:
            if row["event"] == "completed":
                row["external_store"] = False
        self.refused(rows)
        self.assertEqual(observations(output(rows), CASE, external_store=False), (rows, 4))

    def test_request_result_and_receipt_identity_refuse(self):
        changes = [("result", "actual_invocation", "ff" * 16), ("result", "disposition", "error"),
                   ("result", "sequence", 3), ("result", "cell", "Counter:foreign"),
                   ("verified_result", "witness", "foreign proof"), ("verified_result", "served_by", 2),
                   ("receipt", "required", "Quorum"), ("receipt", "value", True),
                   ("caller_cancelled", "waiter_joined", False)]
        for event, field, value in changes:
            with self.subTest(event=event, field=field):
                rows = history()
                next(row for row in rows if row["event"] == event)[field] = value
                self.refused(rows)

    def test_lost_physical_pin_or_premature_callback_refuses(self):
        for phase in ("cancelled_waiter_retains_physical_pins", "pending_late_two_barriers_before_any_callback"):
            rows = history()
            row = next(row for row in rows if row.get("phase") == phase)
            row["nodes"][row["request_node"]]["owners"]["runtime"]["read_pins"]["items"] = 1
            self.refused(rows)
        rows = history()
        next(row for row in rows if row.get("phase") == "pending_late_first_barrier_held")["callbacks"]["queries"] += 1
        self.refused(rows)

    def test_late_barrier_term_and_membership_cuts_refuse(self):
        for phase, field in [("late_arrival_has_second_barrier_and_pin", "barrier"),
                             ("term_refused_and_drained", "term"),
                             ("configuration_refused_and_drained", "membership_revision")]:
            rows = history()
            row = next(row for row in rows if row.get("phase") == phase)
            node = row["nodes"][row["request_node"]]
            if field == "barrier":
                node["counters"]["cell_linearizable_read_cohorts_started"] -= 1
            else:
                node["group"][field] -= 1
            self.refused(rows)

    def test_missing_or_foreign_physical_identity_refuses(self):
        rows = history()
        row = next(row for row in rows if row["event"] == "owners")
        row["nodes"][0]["process"] = "ff" * 16
        self.refused(rows)
        rows = history()
        row = next(row for row in rows if row["event"] == "owners")
        row["nodes"][0]["group"]["history"] = "foreign history"
        self.refused(rows)


if __name__ == "__main__":
    unittest.main()
