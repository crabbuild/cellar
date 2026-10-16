"""A failing shutdown must not abandon remaining owned processes/stores."""
from contextlib import redirect_stderr
import io
import unittest

from faults import AuditError, cleanup_all


class CleanupTests(unittest.TestCase):
    def actions(self,observed):
        def broken():observed.append("broken");raise RuntimeError("shutdown failed")
        return [("first",lambda:observed.append("first")),("node",broken),("store",lambda:observed.append("store")),("ledger",lambda:observed.append("ledger"))]

    def test_all_actions_run_before_reporting_cleanup_failure(self):
        observed=[]
        with self.assertRaisesRegex(AuditError,"shutdown failed"):cleanup_all(self.actions(observed))
        self.assertEqual(observed,["first","broken","store","ledger"])

    def test_original_failure_survives_cleanup_errors(self):
        observed=[];original=AuditError("original readiness failure");diagnostics=io.StringIO()
        try:
            with redirect_stderr(diagnostics):
                try:raise original
                finally:cleanup_all(self.actions(observed))
        except AuditError as error:self.assertIs(error,original)
        self.assertEqual(observed,["first","broken","store","ledger"])
        self.assertIn("shutdown failed",diagnostics.getvalue())

    def test_successful_cleanup_adds_no_failure(self):
        observed=[];cleanup_all([("store",lambda:observed.append("store"))]);self.assertEqual(observed,["store"])


if __name__=="__main__":unittest.main()
