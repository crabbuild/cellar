# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""A passing test exit cannot substitute for both completed fairness profiles."""
import json
import unittest

import cell_groups


def profiles():
    return [{"schema": 1, "case": cell_groups.ARCHIVE_FAIRNESS_CASE.removeprefix("rustfs_"),
             "extents": mode, "groups": 3, "restored_images": 14, "checkpoint_limit": 1,
             "shared_extent_roots_min": 14 if mode else None,
             "tail_operations_after_close": 0, "runtime_joined": True,
             "foreign_hold_refused": True, "corrupt_tail_refused": True,
             "healthy_sibling_unchanged": True, "repaired_tail_same": True}
            for mode in (False, True)]


def output(rows):
    return "\n".join("ARCHIVE_FAIRNESS_EVIDENCE " + json.dumps(row) for row in rows)


class ArchiveFairnessEvidenceTests(unittest.TestCase):
    def check(self, rows):
        return cell_groups.archive_fairness_observations(output(rows), cell_groups.ARCHIVE_FAIRNESS_CASE)

    def test_both_complete_profiles_are_preserved(self):
        rows = profiles()
        self.assertEqual(self.check(rows), rows)
        self.assertEqual(self.check(list(reversed(rows))), list(reversed(rows)))

    def test_test_success_alone_cannot_claim_both_images(self):
        log = f"test {cell_groups.ARCHIVE_FAIRNESS_CASE} ... ok\ntest result: ok. 1 passed; 0 failed;"
        with self.assertRaises(ValueError):
            cell_groups.archive_fairness_observations(log, cell_groups.ARCHIVE_FAIRNESS_CASE)

    def test_missing_duplicate_and_extra_profiles_refuse(self):
        rows = profiles()
        for selected in ([], rows[:1], [rows[0], rows[0]], rows + [rows[1]]):
            with self.subTest(rows=selected), self.assertRaises(ValueError):
                self.check(selected)

    def test_mode_schema_and_resource_types_are_strict(self):
        for key, value in (("extents", 0), ("extents", "true"), ("schema", True),
                           ("groups", 2), ("restored_images", 12), ("checkpoint_limit", True),
                           ("checkpoint_limit", 2), ("tail_operations_after_close", 1)):
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                rows = profiles()
                rows[0][key] = value
                self.check(rows)

    def test_extent_profile_requires_a_shared_physical_inventory(self):
        for value in (None, True, 6, "14"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                rows = profiles()
                rows[1]["shared_extent_roots_min"] = value
                self.check(rows)
        rows = profiles()
        rows[0]["shared_extent_roots_min"] = 14
        with self.assertRaises(ValueError):
            self.check(rows)
        rows = profiles()
        del rows[0]["shared_extent_roots_min"]
        with self.assertRaises(ValueError):
            self.check(rows)

    def test_all_owner_and_isolation_proofs_are_required(self):
        for key in ("runtime_joined", "foreign_hold_refused", "corrupt_tail_refused",
                    "healthy_sibling_unchanged", "repaired_tail_same"):
            for value in (False, 1, None):
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    rows = profiles()
                    rows[0][key] = value
                    self.check(rows)

    def test_foreign_case_and_non_object_rows_refuse(self):
        for row in ([], None, {**profiles()[0], "case": "another_case"}):
            with self.subTest(row=row), self.assertRaises(ValueError):
                self.check([row, profiles()[1]])
        with self.assertRaises(ValueError):
            cell_groups.archive_fairness_observations(output(profiles()), "rustfs_other")

    def test_malformed_and_oversized_json_refuse(self):
        for payload in ("{", json.dumps({"padding": "x" * 8192})):
            with self.subTest(length=len(payload)), self.assertRaises(ValueError):
                cell_groups.archive_fairness_observations(
                    "ARCHIVE_FAIRNESS_EVIDENCE " + payload, cell_groups.ARCHIVE_FAIRNESS_CASE)


if __name__ == "__main__":
    unittest.main()
