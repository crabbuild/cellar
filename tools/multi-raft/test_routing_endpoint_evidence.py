# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Synthetic ledgers exercise refusal rules; they provide no runtime qualification."""

import json
import unittest

import cell_groups
from routing_endpoint_evidence import INFLIGHT, READ


def rows_for(case):
    rows = []
    for caller in ("Sdk", "Native", "Axum"):
        for mode in (False, True):
            for consistency in (("Strong", "AtLeast") if case == READ else (None,)):
                row = {
                    "schema": 1, "case": case.removeprefix("rustfs_"), "event": "verified",
                    "caller": caller, "extents": mode,
                    "old_process": "01" * 16, "current_process": "02" * 16,
                    "directory_before": "03" * 32, "directory_after": "03" * 32,
                    "membership_revision": "same membership", "version": "covering version",
                    "witness": "verified witness", "routing_before": [1, 1, 0, 0, 0, 0, 0],
                }
                if case == READ:
                    row.update(
                        consistency=consistency, routing_after=[1, 1, 0, 0, 1, 1, 0],
                        node="04" * 16, disk="05" * 16, invocation="06" * 16,
                        queries_before=0, queries_after=1, commands=1, timeout_ms=5000,
                        maximum_attempts=4, selected_member=1, leader_fallback=False,
                        barriers_before=2, barriers_after=3 if consistency == "Strong" else 2,
                        minimum="minimum version", reply={"cell": "Counter:one", "value": 1,
                                                         "served_by": 1, "version_token": "token"},
                    )
                else:
                    row.update(
                        routing_after=[2, 2, 0, 0, 0, 0, 0], callbacks_before=1,
                        callbacks_after=2, required="S3", achieved="S3", leader_unchanged=True,
                        retry_until_ms=100, command="original", result_be="0000000000000002",
                    )
                rows.append(row)
    return rows


def output(rows, case):
    prefix = "ENDPOINT_READ_EVIDENCE " if case == READ else "ENDPOINT_INFLIGHT_EVIDENCE "
    return "\n".join(prefix + json.dumps(row) for row in rows)


class EndpointEvidenceTests(unittest.TestCase):
    def assert_refused(self, rows, case):
        with self.assertRaises(ValueError):
            cell_groups.routing_observations(output(rows, case), case)

    def test_named_suite_accepts_both_complete_ledgers(self):
        for case, count in ((READ, 12), (INFLIGHT, 6)):
            with self.subTest(case=case):
                self.assertIn(case, cell_groups.SUITES["routing-endpoint"][1])
                rows = rows_for(case)
                self.assertEqual(cell_groups.routing_observations(output(rows, case), case), (rows, count))

    def test_partial_duplicate_and_wrong_consistency_coverage_refuse(self):
        for case in (READ, INFLIGHT):
            rows = rows_for(case)
            for bad in ([], rows[:-1], rows + [rows[0]], rows[:-1] + [rows[0]]):
                with self.subTest(case=case, length=len(bad)):
                    self.assert_refused(bad, case)
        rows = rows_for(READ)
        rows[-1]["consistency"] = "Strong"
        self.assert_refused(rows, READ)

    def test_schema_identity_and_counter_types_refuse(self):
        changes = (("schema", True), ("event", "prepared"), ("caller", ["Sdk"]),
                   ("extents", 0), ("old_process", "02" * 16), ("current_process", "unverified"),
                   ("directory_after", "04" * 32), ("membership_revision", ""),
                   ("version", ""), ("witness", ""), ("routing_after", [True] * 7))
        for case in (READ, INFLIGHT):
            for field, value in changes:
                with self.subTest(case=case, field=field):
                    rows = rows_for(case)
                    rows[0][field] = value
                    self.assert_refused(rows, case)

    def test_reads_refuse_extra_work_wrong_target_and_budget_expansion(self):
        changes = (("routing_after", [1, 1, 1, 0, 1, 1, 0]),
                   ("routing_after", [1, 1, 0, 0, 2, 1, 0]),
                   ("queries_after", 2), ("commands", 2), ("timeout_ms", 5001),
                   ("maximum_attempts", 5), ("selected_member", 2),
                   ("leader_fallback", True), ("minimum", ""), ("invocation", ""))
        for field, value in changes:
            with self.subTest(field=field):
                rows = rows_for(READ)
                rows[0][field] = value
                self.assert_refused(rows, READ)

    def test_reads_require_strong_barrier_and_at_least_without_new_barrier(self):
        for index, after in ((0, 2), (1, 3)):
            rows = rows_for(READ)
            rows[index]["barriers_after"] = after
            self.assert_refused(rows, READ)
        for field, value in (("value", True), ("served_by", 2), ("version_token", "")):
            rows = rows_for(READ)
            rows[0]["reply"][field] = value
            self.assert_refused(rows, READ)

    def test_inflight_requires_unchanged_leader_exact_callback_and_s3_receipt(self):
        changes = (("routing_after", [2, 2, 0, 0, 1, 1, 0]),
                   ("routing_after", [2, 6, 0, 0, 0, 0, 0]),
                   ("routing_after", [2, 0, 0, 0, 0, 0, 0]),
                   ("callbacks_after", 3), ("callbacks_before", 2**64),
                   ("required", "Quorum"), ("achieved", "Quorum"),
                   ("retry_until_ms", True), ("leader_unchanged", False),
                   ("result_be", "0000000000000003"), ("command", ""))
        for field, value in changes:
            with self.subTest(field=field):
                rows = rows_for(INFLIGHT)
                rows[0][field] = value
                self.assert_refused(rows, INFLIGHT)

    def test_foreign_case_and_malformed_rows_refuse(self):
        rows = rows_for(INFLIGHT)
        rows[0]["case"] = READ.removeprefix("rustfs_")
        self.assert_refused(rows, INFLIGHT)
        for case in (READ, INFLIGHT):
            self.assert_refused([None], case)
            with self.assertRaises(ValueError):
                cell_groups.routing_observations(output(rows_for(case), case).replace('"schema": 1', '"schema":'), case)


if __name__ == "__main__":
    unittest.main()
