# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Synthetic output checks refuse missing coverage; they qualify no runtime."""

import copy
import json
import unittest

import cell_groups


MEMBERSHIP = "rustfs_membership_refresh_keeps_range"
AUTHORITY = "rustfs_cached_advice_respects_origin_revocation"
ENDPOINT = "rustfs_endpoint_refresh_keeps_membership"
FOLLOWER = "rustfs_unrelated_follower_keeps_leader"


def member_rows():
    return [{"schema": 1, "case": MEMBERSHIP.removeprefix("rustfs_"),
             "caller": caller, "extents": mode, "event": event}
            for caller in ("Sdk", "Native", "Axum") for mode in (False, True)
            for event in ("warm", "membership_repaired", "fences_and_receipts_verified")]


def output(rows, prefix="MEMBERSHIP_ROUTING_EVIDENCE "):
    return "\n".join(prefix + json.dumps(row) for row in rows)


def authority_rows():
    return [{"schema": 1, "case": AUTHORITY.removeprefix("rustfs_"),
             "caller": caller, "extents": mode, "event": "verified",
             "callbacks_before": 1, "callbacks_after_refusal": 1, "callbacks_after": 2,
             "sql_stages_before": 3, "sql_stages_after_refusal": 3,
             "routing_before": [3, 1, 1, 0, 1, 3], "routing_after_refusal": [4, 1, 1, 0, 2, 4],
             "required": "S3", "achieved": "S3", "result_be": "0000000000000002",
             "command": "original", "group": "group", "origin": "node",
             "proven_rejections": 1, "unconfirmed_replies": 0,
             "version": "covering version", "witness": "witness",
             "ambiguous": True, "sdk_ambiguous": True if caller == "Sdk" else None}
            for caller in ("Sdk", "Native", "Axum") for mode in (False, True)]


def endpoint_rows():
    return [{"schema": 1, "case": ENDPOINT.removeprefix("rustfs_"),
             "caller": caller, "extents": mode, "event": "verified",
             "callbacks_before": 1, "callbacks_after": 2,
             "sql_stages_before": 3, "sql_stages_at_release": 3,
             "routing_before": [1, 1, 1, 1, 1, 1, 0],
             "routing_after": [2, 2, 1, 1, 2, 2, 0],
             "node": "01" * 16, "disk": "02" * 16,
             "old_process": "03" * 16, "current_process": "04" * 16,
             "directory_before": "05" * 32, "directory_after": "05" * 32,
             "required": "S3", "achieved": "S3", "result_be": "0000000000000002",
             "command": "original", "route": "same route", "membership_revision": "same",
             "retry_until_ms": 100, "version": "covering version", "witness": "witness"}
            for caller in ("Sdk", "Native", "Axum") for mode in (False, True)]


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
                             ("schema", True), ("extents", 0), ("extents", "false")):
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

    def test_complete_authority_requires_all_callers_and_both_images(self):
        rows = authority_rows()
        log = output(rows, "AUTHORITY_ROUTING_EVIDENCE ")
        self.assertEqual(cell_groups.routing_observations(log, AUTHORITY), (rows, 6))
        for missing in (rows[:-1], rows + [rows[0]], rows[:-1] + [rows[0]]):
            with self.subTest(rows=len(missing)), self.assertRaises(ValueError):
                cell_groups.routing_observations(output(missing, "AUTHORITY_ROUTING_EVIDENCE "), AUTHORITY)

    def test_authority_refuses_callback_sql_and_proof_downgrades(self):
        for field, value in (("callbacks_after_refusal", 2), ("callbacks_after", 3),
                             ("callbacks_before", 2**64), ("unconfirmed_replies", 1),
                             ("sql_stages_after_refusal", 4), ("achieved", "Quorum"),
                             ("required", "Quorum"), ("result_be", "0000000000000003"),
                             ("witness", ""), ("sdk_ambiguous", None), ("ambiguous", False),
                             ("routing_after_refusal", [5, 1, 1, 0, 2, 4])):
            rows = authority_rows()
            rows[0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                cell_groups.routing_observations(output(rows, "AUTHORITY_ROUTING_EVIDENCE "), AUTHORITY)

    def test_authority_rows_cannot_stand_in_for_a_different_case(self):
        with self.assertRaises(ValueError):
            cell_groups.routing_observations(output(authority_rows(), "AUTHORITY_ROUTING_EVIDENCE "), MEMBERSHIP)

    def test_endpoint_requires_exact_complete_caller_image_coverage(self):
        rows = endpoint_rows()
        self.assertEqual(cell_groups.routing_observations(
            output(rows, "ENDPOINT_ROUTING_EVIDENCE "), ENDPOINT), (rows, 6))
        for missing in (rows[:-1], rows + [rows[0]], rows[:-1] + [rows[0]]):
            with self.subTest(rows=len(missing)), self.assertRaises(ValueError):
                cell_groups.routing_observations(output(missing, "ENDPOINT_ROUTING_EVIDENCE "), ENDPOINT)

    def test_endpoint_refuses_placement_repair_unbounded_work_and_early_entry(self):
        changes = (("callbacks_after", 3), ("sql_stages_at_release", 4),
                   ("old_process", "04" * 16), ("current_process", "unverified"),
                   ("directory_after", "06" * 32), ("retry_until_ms", True),
                   ("required", "Quorum"), ("achieved", "Quorum"), ("witness", ""),
                   ("routing_after", [2, 2, 2, 1, 2, 2, 0]),
                   ("routing_after", [2, 2, 1, 2, 2, 2, 0]),
                   ("routing_after", [2, 2, 1, 1, 6, 2, 0]),
                   ("routing_after", [2, 2, 1, 1, 2, 0, 0]))
        for field, value in changes:
            rows = endpoint_rows()
            rows[0][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                cell_groups.routing_observations(output(rows, "ENDPOINT_ROUTING_EVIDENCE "), ENDPOINT)

    def test_unrelated_follower_requires_zero_endpoint_demand_and_unreleased_fault(self):
        rows = endpoint_rows()
        for row in rows:
            row.update(case=FOLLOWER.removeprefix("rustfs_"), sql_stages_before=None,
                       sql_stages_at_release=None, metadata_released=False,
                       routing_after=[2, 2, 1, 1, 1, 1, 0])
        self.assertEqual(cell_groups.routing_observations(
            output(rows, "ENDPOINT_ROUTING_EVIDENCE "), FOLLOWER), (rows, 6))
        for field, value in (("metadata_released", True), ("sql_stages_at_release", 3),
                             ("routing_after", [2, 2, 1, 1, 2, 1, 0])):
            bad = copy.deepcopy(rows)
            bad[0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                cell_groups.routing_observations(output(bad, "ENDPOINT_ROUTING_EVIDENCE "), FOLLOWER)

    def test_unrelated_follower_cannot_replace_the_selected_leader_repair_case(self):
        rows = endpoint_rows()
        with self.assertRaises(ValueError):
            cell_groups.routing_observations(output(rows, "ENDPOINT_ROUTING_EVIDENCE "), FOLLOWER)


if __name__ == "__main__":
    unittest.main()
