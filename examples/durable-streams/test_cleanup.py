"""A failing shutdown must not abandon remaining owned processes/stores."""
from contextlib import redirect_stderr
import io
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

import consumer_recovery
import ack_faults
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

    def test_consumer_driver_keeps_original_failure_and_cleans_every_resource(self):
        observed=[];original=AuditError("primary readiness failure")
        def process(_binary,_root,node,*args,**kwargs):
            def ready():
                observed.append(node+"-ready")
                if node=="producer":raise original
                return "http://private"
            def stop():
                observed.append(node+"-stop")
                if node=="producer":raise RuntimeError("secondary stop failure")
            return SimpleNamespace(node=node,ready=ready,stop=stop)
        store=SimpleNamespace(start=lambda:observed.append("store-start"),close=lambda:observed.append("store-close"))
        ledger=SimpleNamespace(close=lambda:observed.append("ledger-close"))
        caught=None;diagnostics=io.StringIO()
        with tempfile.TemporaryDirectory() as directory,redirect_stderr(diagnostics), \
             patch.object(consumer_recovery,"Ledger",return_value=ledger), \
             patch.object(consumer_recovery,"NetworkStore",return_value=store), \
             patch.object(consumer_recovery,"FleetProcess",side_effect=process), \
             patch.object(consumer_recovery,"free_address",return_value=("127.0.0.1",1234)), \
             patch.object(consumer_recovery,"read",return_value={"records":[]}):
            try:consumer_recovery.exercise(Path("unused"),Path(directory)/"fixture",rustfs_image="private-fixture")
            except Exception as error:caught=error
        self.assertIs(caught,original)
        self.assertEqual(observed,["store-start","destination-ready","producer-ready","producer-stop","destination-stop","store-close","ledger-close"])
        self.assertIn("secondary stop failure",diagnostics.getvalue())

    def test_ack_driver_keeps_readiness_failure_after_proxy_cleanup_failure(self):
        observed=[];original=AuditError("primary destination readiness failure")
        def ready():observed.append("destination-ready");raise original
        def close_proxy():observed.append("proxy-close");raise RuntimeError("secondary proxy failure")
        process=SimpleNamespace(node="destination",ready=ready,stop=lambda:observed.append("destination-stop"))
        proxy=SimpleNamespace(address=("127.0.0.1",1235),release=SimpleNamespace(set=lambda:observed.append("proxy-release")),outage=False,close=close_proxy)
        store=SimpleNamespace(start=lambda:observed.append("store-start"),close=lambda:observed.append("store-close"))
        ledger=SimpleNamespace(close=lambda:observed.append("ledger-close"))
        caught=None;diagnostics=io.StringIO()
        with tempfile.TemporaryDirectory() as directory,redirect_stderr(diagnostics), \
             patch.object(ack_faults,"Ledger",return_value=ledger), \
             patch.object(ack_faults,"NetworkStore",return_value=store), \
             patch.object(ack_faults,"PeerProxy",return_value=proxy), \
             patch.object(ack_faults,"FleetProcess",return_value=process), \
             patch.object(ack_faults,"free_address",return_value=("127.0.0.1",1234)):
            try:ack_faults.exercise(Path("unused"),Path(directory)/"fixture",rustfs_image="private-fixture")
            except Exception as error:caught=error
        self.assertIs(caught,original)
        self.assertEqual(observed,["store-start","destination-ready","proxy-release","destination-stop","proxy-close","store-close","ledger-close"])
        self.assertIn("secondary proxy failure",diagnostics.getvalue())


if __name__=="__main__":unittest.main()
