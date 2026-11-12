# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Case success cannot replace all four named-binding policy profiles."""
import unittest

import cell_groups


def profile_lines(recovery):
    return [f"named binding configuration recovery={str(recovery).lower()} "
            f"extents={str(mode).lower()} source={policy}: PASS"
            for mode in (False, True) for policy in ("Quorum", "S3")]


class NamedBindingEvidenceTests(unittest.TestCase):
    def check(self, lines, case):
        return cell_groups.named_binding_observations("\n".join(lines), case)

    def test_both_cases_require_their_own_complete_profiles(self):
        for case, recovery in cell_groups.NAMED_BINDING_CASES.items():
            with self.subTest(case=case):
                rows = self.check(profile_lines(recovery), case)
                self.assertEqual(len(rows), 4)
                self.assertTrue(all(row["recovery"] is recovery for row in rows))
                self.assertEqual({(row["extents"], row["source_durability"]) for row in rows},
                                 {(mode, policy) for mode in (False, True) for policy in ("Quorum", "S3")})

    def test_test_success_alone_cannot_claim_profiles(self):
        for case in cell_groups.NAMED_BINDING_CASES:
            with self.subTest(case=case), self.assertRaises(ValueError):
                self.check([f"test {case} ... ok", "test result: ok. 1 passed; 0 failed;"], case)

    def test_missing_duplicate_and_extra_profiles_refuse(self):
        for case, recovery in cell_groups.NAMED_BINDING_CASES.items():
            rows = profile_lines(recovery)
            for selected in ([], rows[:2], rows[:3], [rows[0]] * 4, rows + [rows[0]]):
                with self.subTest(case=case, rows=selected), self.assertRaises(ValueError):
                    self.check(selected, case)

    def test_foreign_recovery_and_unknown_case_refuse(self):
        for case, recovery in cell_groups.NAMED_BINDING_CASES.items():
            with self.subTest(case=case), self.assertRaises(ValueError):
                self.check(profile_lines(not recovery), case)
        with self.assertRaises(ValueError):
            self.check(profile_lines(True), "rustfs_other")

    def test_failed_or_malformed_profile_refuses(self):
        for case, recovery in cell_groups.NAMED_BINDING_CASES.items():
            for old, new in (("PASS", "FAILED: original deadline"), ("extents=false", "extents=0"),
                             ("source=Quorum", "source=quorum"), ("source=Quorum", "source=Other")):
                with self.subTest(case=case, replacement=new), self.assertRaises(ValueError):
                    rows = profile_lines(recovery)
                    rows[0] = rows[0].replace(old, new)
                    self.check(rows, case)

    def test_legacy_success_cannot_claim_the_new_case_scope(self):
        for case, recovery in cell_groups.NAMED_BINDING_CASES.items():
            rows = [line.replace(f"recovery={str(recovery).lower()} ", "")
                    for line in profile_lines(recovery)]
            with self.subTest(case=case), self.assertRaises(ValueError):
                self.check(rows, case)

    def test_libtest_prefix_and_profile_order_are_preserved(self):
        for case, recovery in cell_groups.NAMED_BINDING_CASES.items():
            rows = list(reversed(profile_lines(recovery)))
            rows[0] = f"test {case} ... " + rows[0]
            self.assertEqual(self.check(rows, case)[0]["source_durability"], "S3")


if __name__ == "__main__":
    unittest.main()
