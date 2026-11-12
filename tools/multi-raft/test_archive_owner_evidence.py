# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Synthetic evidence controls do not qualify archive I/O or reclamation."""
import copy
import json
import unittest

from archive_owner_evidence import CASE, PREFIX, observations
import cell_groups


def profiles():
    return [{"schema": 1, "case": CASE.removeprefix("rustfs_"), "extents": mode,
             "history": "full group history", "old_position": 7, "new_position": 11,
             "old_root": "11" * 32, "new_root": "22" * 32,
             "old_page_sha256": "33" * 32, "old_image_sha256": "44" * 32,
             "held_page_sha256": "33" * 32, "held_image_sha256": "44" * 32,
             "old_image_bytes": 65536, "held_source_bytes": 1024, "held_archive_bytes": 2048,
             "held_sources": 1, "held_archive_operations": 1, "cancelled_indexes": 0,
             "released_source_bytes": 0, "released_archive_bytes": 0,
             "old_page_removed": True, "old_image_removed": True, "fresh_checkpoint_verified": True}
            for mode in (False, True)]


def output(rows):
    return "\n".join(PREFIX + json.dumps(row) for row in rows)


class PhysicalArchiveOwnerEvidenceTests(unittest.TestCase):
    def test_both_profiles_and_fixed_suite(self):
        rows = profiles()
        self.assertEqual(observations(output(rows), CASE), rows)
        self.assertEqual(observations(output(rows[::-1]), CASE), rows[::-1])
        self.assertEqual(cell_groups.SUITES["archive-owners"][:2],
                         ("native_cell_groups_archive", (CASE,)))

    def test_success_without_complete_observations_refuses(self):
        for rows in ([], profiles()[:1], [profiles()[0]] * 2, profiles() + profiles()):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                observations(output(rows), CASE)
        with self.assertRaises(ValueError):
            observations(f"test {CASE} ... ok\ntest result: ok. 1 passed; 0 failed;", CASE)

    def test_missing_field_or_wrong_type_refuses(self):
        for field, value in profiles()[0].items():
            with self.subTest(field=field):
                rows = profiles(); del rows[0][field]
                with self.assertRaises(ValueError):
                    observations(output(rows), CASE)
                rows = profiles(); rows[0][field] = str(value) if type(value) is not str else None
                with self.assertRaises(ValueError):
                    observations(output(rows), CASE)

    def test_admission_credit_and_premature_delete_proofs_refuse(self):
        for field, value in (("schema", True), ("held_sources", True),
                             ("new_position", 7), ("new_root", "11" * 32),
                             ("held_archive_operations", 0), ("held_source_bytes", 0),
                             ("held_archive_bytes", 32 * 1024**2 + 1),
                             ("released_source_bytes", 1), ("released_archive_bytes", 1),
                             ("cancelled_indexes", 1), ("held_page_sha256", "55" * 32),
                             ("held_image_sha256", "55" * 32), ("old_image_removed", False),
                             ("old_page_removed", False), ("fresh_checkpoint_verified", False),
                             ("old_image_sha256", "bad")):
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                rows = copy.deepcopy(profiles()); rows[0][field] = value
                observations(output(rows), CASE)

    def test_foreign_malformed_and_oversized_refuse(self):
        with self.assertRaises(ValueError):
            observations(output(profiles()), "rustfs_other")
        for payload in ("{", "[]", "null", json.dumps({"padding": "x" * 8192})):
            with self.subTest(payload=payload[:40]), self.assertRaises(ValueError):
                observations(PREFIX + payload, CASE)


if __name__ == "__main__":
    unittest.main()
