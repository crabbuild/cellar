# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Synthetic output checks refuse missing coverage; they qualify no runtime."""

import copy
import json
import unittest

import cell_groups


MEMBERSHIP = "rustfs_membership_refresh_keeps_range"


def member_rows():
    return [{"schema": 1, "case": MEMBERSHIP.removeprefix("rustfs_"),
             "caller": caller, "extents": mode, "event": event}
            for caller in ("Sdk", "Native", "Axum") for mode in (False, True)
            for event in ("warm", "membership_repaired", "fences_and_receipts_verified")]


def output(rows, prefix="MEMBERSHIP_ROUTING_EVIDENCE "):
    return "\n".join(prefix + json.dumps(row) for row in rows)


class RoutingEvidenceTests(unittest.TestCase):
    def test_complete_membership_includes_all_three_callers_and_phases(self):
        rows = member_rows()
        self.assertEqual(cell_groups.routing_observations(output(rows), MEMBERSHIP), (rows, 6))

    def test_missing_extent_phase_refuses(self):
        with self.assertRaises(ValueError):
            cell_groups.routing_observations(output(member_rows()[:-1]), MEMBERSHIP)

    def test_duplicate_replacing_missing_phase_refuses_at_same_count(self):
        rows = member_rows()
        rows[-1] = copy.deepcopy(rows[0])
        with self.assertRaises(ValueError):
            cell_groups.routing_observations(output(rows), MEMBERSHIP)

    def test_additional_duplicate_refuses(self):
        rows = member_rows()
        with self.assertRaises(ValueError):
            cell_groups.routing_observations(output(rows + [rows[0]]), MEMBERSHIP)

    def test_wrong_case_schema_and_nonboolean_mode_refuse(self):
        for field, value in (("case", "cached_leader_direct_write"), ("schema", 2),
                             ("extents", 0), ("extents", "false")):
            with self.subTest(field=field, value=value):
                rows = member_rows()
                rows[0][field] = value
                with self.assertRaises(ValueError):
                    cell_groups.routing_observations(output(rows), MEMBERSHIP)

    def test_unknown_or_missing_output_refuses(self):
        for case in (MEMBERSHIP, "rustfs_unknown"):
            with self.subTest(case=case), self.assertRaises(ValueError):
                cell_groups.routing_observations("", case)

    def test_malformed_record_refuses(self):
        with self.assertRaises(ValueError):
            cell_groups.routing_observations("MEMBERSHIP_ROUTING_EVIDENCE {", MEMBERSHIP)

    def test_split_and_old_lookup_have_their_own_expected_coverage(self):
        for case, callers in (("rustfs_split_repairs_only_overlap", ("SdkNativeAxum",)),
                              ("rustfs_old_lookup_cannot_resurrect_parent", ("Sdk", "Native", "Axum"))):
            with self.subTest(case=case):
                rows = [{"schema": 1, "case": case.removeprefix("rustfs_"),
                         "caller": caller, "extents": mode, "event": "verified"}
                        for caller in callers for mode in (False, True)]
                log = output(rows, "RANGE_ROUTING_EVIDENCE ")
                self.assertEqual(cell_groups.routing_observations(log, case), (rows, len(callers) * 2))
                with self.assertRaises(ValueError):
                    cell_groups.routing_observations(log, MEMBERSHIP)

    def test_success_output_still_requires_the_exact_test_to_execute(self):
        log = (f"test {MEMBERSHIP} ... ok\n\nsuccesses:\n"
               + output(member_rows())
               + "\n\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 13 filtered out; finished in 1.00s\n")
        self.assertTrue(cell_groups.exact_case_passed(log, MEMBERSHIP))
        self.assertFalse(cell_groups.exact_case_passed(log.replace("1 passed", "0 passed"), MEMBERSHIP))
        self.assertFalse(cell_groups.exact_case_passed(log, "rustfs_split_repairs_only_overlap"))


if __name__ == "__main__":
    unittest.main()
