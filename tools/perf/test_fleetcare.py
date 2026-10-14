"""Fail-closed lifecycle and regression guards for the repeatable suite."""
import copy
import io
import json
import pathlib
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import fleetcare
import fleetcare_report


class PressurePolicyTests(unittest.TestCase):
    def test_each_host_must_report_the_actual_selected_watermarks(self):
        for mode in ('off','auto'):
            expected = fleetcare.pressure_policy(mode)
            fleetcare.validate_pressure({'pressure_config':expected}, mode)
            for value in (None, {}, {'high_bytes':1,'rss_hard_bytes':2}):
                with self.subTest(mode=mode, value=value), self.assertRaises(RuntimeError):
                    fleetcare.validate_pressure({'pressure_config':value}, mode)
            with self.assertRaises(RuntimeError):
                fleetcare.validate_pressure({}, mode)

    def test_policy_matches_the_pure_80_percent_and_95_percent_watermarks(self):
        self.assertEqual(fleetcare.pressure_policy('off'), {'high_bytes':None,'rss_hard_bytes':None})
        self.assertEqual(fleetcare.pressure_policy('auto'), {'high_bytes':1503238552,'rss_hard_bytes':1785095695})


class StoreIsolationTests(unittest.TestCase):
    def test_filesystem_observation_retains_bytes_and_inodes(self):
        value = fleetcare.store_filesystem('size used avail itotal iused iavail\n80 40 35 100 10 90\n')
        self.assertEqual(value, {'size_bytes':80, 'used_bytes':40, 'available_bytes':35,
                                'inodes':100, 'used_inodes':10, 'available_inodes':90})

    def test_unavailable_or_inconsistent_observation_fails_closed(self):
        for text in ('', 'unavailable', '80 40 35 100 10', '0 0 0 100 10 90',
                     '80 40 -1 100 10 90', '80 40 50 100 10 90', '80 40 35 100 10 101'):
            with self.subTest(text=text), self.assertRaises(RuntimeError):
                fleetcare.store_filesystem(text)

    def harness(self):
        h = fleetcare.FleetCareHarness.__new__(fleetcare.FleetCareHarness)
        h.tag = 'private-run'
        h.vm_root = '/mnt/lima-colima/private-run'
        h.nodes = ['private-run-node1', 'private-run-node2', 'private-run-node3']
        h.store = 'private-run-rustfs'
        return h

    def test_insufficient_bytes_or_inodes_cannot_start_a_case(self):
        value = fleetcare.store_filesystem(f'{80*1024**3} {40*1024**3} {35*1024**3} 10000 1000 9000')
        fleetcare.require_store_headroom(value, 25)
        for field, insufficient in (('available_bytes', 24*1024**3), ('available_inodes', 999)):
            observed = dict(value, **{field:insufficient})
            with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, 'free before a case'):
                fleetcare.require_store_headroom(observed, 25)

    def test_live_node_or_client_prevents_store_reset(self):
        for running, method in ((name, method) for name in ('private-run-node1', 'private-run-load')
                                for method in ('_reset_store', 'clear_disks')):
            calls = []
            def command(*args, **kwargs):
                calls.append(args)
                return running + '\nforeign-node'
            with self.subTest(running=running, method=method), patch.object(fleetcare, 'command', command):
                with self.assertRaisesRegex(RuntimeError, 'live'):
                    h = self.harness()
                    h._reset_store('fixture') if method == '_reset_store' else h.clear_disks()
            self.assertEqual(len(calls), 1)

    def test_clear_failure_cannot_trim_filesystems(self):
        with patch.object(fleetcare, 'command', return_value='foreign-node') as command, \
             patch.object(fleetcare.Harness, 'clear_disks', side_effect=RuntimeError('clear failed')):
            with self.assertRaisesRegex(RuntimeError, 'clear failed'):
                self.harness().clear_disks()
            self.assertEqual(command.call_count, 1)

    def test_store_reset_cannot_delete_another_directory(self):
        h = self.harness()
        h.vm_root = '/foreign-directory'
        with patch.object(fleetcare, 'command') as command:
            with self.assertRaisesRegex(RuntimeError, 'private'):
                h._reset_store('fixture')
            command.assert_not_called()


class CleanupTests(unittest.TestCase):
    def harness(self):
        h = fleetcare.FleetCareHarness.__new__(fleetcare.FleetCareHarness)
        h.tag = h.network = "private-run"
        h.vm_root = "/private-fixture"
        h.disks_created = True
        h.args = SimpleNamespace(sandbox_image="fixture-image")
        return h

    def command(self, names="foreign-run-node1", network="foreign-network", children="", tables=None):
        def run(*args, **kwargs):
            if args[:3] == ("docker", "ps", "-a"):
                return names
            if args[:4] == ("docker", "network", "ls", "--format"):
                return network
            if "--filter" in args:
                return children
            if args[:2] == ("docker", "run"):
                return json.dumps({"nftables":tables or [{"table":{"name":"foreign-table"}}]})
            return ""
        return run

    def test_other_runs_do_not_prevent_private_cleanup(self):
        with patch.object(fleetcare, "command", self.command()):
            self.harness()._verify_cleanup()

    def test_remaining_private_resources_fail_closed(self):
        h = self.harness()
        key = fleetcare.hashlib.sha256(b"node1\0state:/private-fixture/node1/state").hexdigest()
        for values in ({"names":"private-run-node1"}, {"network":"private-run"},
                       {"children":"private-child"}, {"tables":[{"table":{"name":f"cellar_{key[:24]}"}}]}):
            with self.subTest(values=values), patch.object(fleetcare, "command", self.command(**values)):
                with self.assertRaises(RuntimeError):
                    h._verify_cleanup()

    def test_unavailable_inspection_is_not_successful_cleanup(self):
        with patch.object(fleetcare, "command", side_effect=OSError("host file exhaustion")):
            with self.assertRaises(OSError):
                self.harness()._verify_cleanup()


class PolicyRecoveryTests(unittest.TestCase):
    def check(self, observed, expected=None):
        h = fleetcare.FleetCareHarness.__new__(fleetcare.FleetCareHarness)
        h.args = SimpleNamespace(profile="smoke", port=39100)
        h.token = "fixture"
        h.checks = []
        responses = [io.BytesIO(json.dumps(value).encode()) for value in observed]
        with tempfile.TemporaryDirectory() as directory:
            h.output = pathlib.Path(directory)
            with patch.object(fleetcare.urllib.request, "urlopen", side_effect=responses):
                return h.policy_snapshot("fixture", "restore", expected)

    def values(self):
        return [{"policy":{"version":i+1,"policy":"standard"},"metadata":{"version":1}} for i in range(2)]

    def test_exact_latest_values_and_metadata_survive(self):
        expected = self.check(self.values())
        self.assertEqual(self.check(self.values(), expected), expected)

    def test_missing_changed_or_type_changed_values_fail(self):
        expected = self.check(self.values())
        for target, value in (("policy", None), ("policy", {"version":99}),
                              ("metadata", {"version":2}), ("policy", {"version":True,"policy":"standard"})):
            observed = self.values()
            observed[0][target] = value
            with self.subTest(target=target,value=value), self.assertRaises(RuntimeError):
                self.check(observed, expected)


class ShutdownTests(unittest.TestCase):
    def proof(self, host="embedded", mode="preserve"):
        return {"event": f"{host}_node_stopped", "outcome": "stopped", "mode": mode,
                "ingress_drained": True, "application_work_drained": True, "cells_drained": True,
                "log_quiesced": True, "local_durability_closed": True, "caller_executors_alive": True,
                "pending_application_events": 0, "pending_dispatches": 0,
                "drain_timed_out": False, "deadline_elapsed": False, "clean_reload_prepared": True}

    def test_each_actual_host_proof_required(self):
        for host in ("embedded", "daemon"):
            p = self.proof(host)
            self.assertEqual(fleetcare.validate_stop(json.dumps(p), "preserve", host, 0), p)
            with self.assertRaises(RuntimeError):
                fleetcare.validate_stop(json.dumps(p), "preserve", "daemon" if host == "embedded" else "embedded", 0)

    def test_each_drain_phase_fails_closed(self):
        for field in ("ingress_drained", "application_work_drained", "cells_drained", "log_quiesced",
                      "local_durability_closed", "caller_executors_alive", "clean_reload_prepared"):
            p = self.proof()
            p[field] = False
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                fleetcare.validate_stop(json.dumps(p), "preserve", "embedded", 0)

    def test_pending_timeout_exit_and_duplicate_report(self):
        for field in ("pending_application_events", "pending_dispatches", "drain_timed_out", "deadline_elapsed"):
            p = self.proof()
            p[field] = 1 if field.startswith("pending") else True
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                fleetcare.validate_stop(json.dumps(p), "preserve", "embedded", 0)
        p = json.dumps(self.proof())
        for text, code in ((p, 3), (p+"\n"+p, 0), ("", 0)):
            with self.assertRaises(RuntimeError):
                fleetcare.validate_stop(text, "preserve", "embedded", code)


class ComparisonTests(unittest.TestCase):
    def report(self):
        metric = {"p99_ms": 100, "successful_rps": 10, "success_fraction": 1,
                  "scheduling_debt_p99_ms": 1}
        metadata = {k: {} for k in ("matrix", "policy", "resources", "generator")}
        metadata.update(schema_version=2, workload_contract="fleetcare-v2", profile="full", suite="standard", architecture="aarch64", rustfs_image="exact-digest")
        return {"completed": True, "metadata": metadata, "measurements": [
            {"host": "embedded", "durability": "bucket", "mix": "core", "rate": 25,
             "skew": "uniform", "summary": {"devices_write": metric}}]}

    def test_equal_baseline_passes(self):
        self.assertEqual(fleetcare.compare(self.report(), self.report()), [])

    def test_each_incompatible_field_rejected(self):
        for field in fleetcare.compatibility(self.report()["metadata"]):
            baseline = self.report()
            baseline["metadata"][field] = "changed"
            with self.subTest(field=field), self.assertRaises(ValueError):
                fleetcare.compare(self.report(), baseline)

    def test_partial_or_missing_measurements_rejected(self):
        for partial in ("completed", "measurements"):
            baseline = self.report()
            baseline[partial] = False if partial == "completed" else []
            with self.assertRaises(ValueError):
                fleetcare.compare(self.report(), baseline)
        current = self.report()
        current["measurements"] = []
        with self.assertRaises(ValueError):
            fleetcare.compare(current, current)

    def test_unsent_or_missing_samples_fail_without_crashing(self):
        for field in ("p99_ms", "scheduling_debt_p99_ms"):
            current = self.report()
            current["measurements"][0]["summary"]["devices_write"][field] = None
            self.assertTrue(fleetcare.compare(current, self.report()))

    def test_latency_throughput_errors_and_generator_debt_regressions(self):
        for metric, actual in (("p99_ms", 130), ("successful_rps", 8), ("success_fraction", .99),
                               ("scheduling_debt_p99_ms", 20)):
            current = self.report()
            current["measurements"][0]["summary"]["devices_write"][metric] = actual
            with self.subTest(metric=metric):
                self.assertTrue(fleetcare.compare(current, self.report()))

    def test_histogram_percentiles_include_failures(self):
        self.assertIsNone(fleetcare.quantile({}, .99))
        self.assertEqual(fleetcare.quantile({"1000": 99, "100000": 1}, .99), 1)
        self.assertEqual(fleetcare.quantile({"1000": 99, "100000": 1}, 1), 100)


class QualificationTests(unittest.TestCase):
    def report(self):
        value = {"attempts":10000,"successful":10000,"successful_rps":100,
            "p99_ms":100,"scheduling_debt_p99_ms":1,"outcomes":{"ok":10000},"p99_confident":True}
        return {"completed":True,"measurements":[{"host":"embedded","durability":"fleet","mix":"write","rate":100,
            "skew":"uniform","summary":{"devices_write":value}}]}

    def test_every_repeat_must_meet_slo_and_generator_validity(self):
        for field,value in [("successful",9000),("p99_ms",2000),("scheduling_debt_p99_ms",20),
            ("p99_ms",None),("scheduling_debt_p99_ms",None),("attempts",0),
            ("outcomes",{"ok":9999,"generator_saturated":1}),("outcomes",{"error:protocol":1})]:
            report = self.report()
            repeat = copy.deepcopy(report["measurements"][0])
            repeat["summary"]["devices_write"][field] = value
            report["measurements"].append(repeat)
            with self.subTest(field=field):
                self.assertFalse(fleetcare_report.rows(report)[0]["qualified"])

    def test_small_samples_remain_provisional(self):
        report = self.report()
        self.assertTrue(fleetcare_report.rows(report)[0]["confident"])
        report["measurements"][0]["summary"]["devices_write"]["p99_confident"] = False
        self.assertFalse(fleetcare_report.rows(report)[0]["confident"])

    def test_failed_correctness_suite_never_qualifies_capacity(self):
        report = self.report()
        report["completed"] = False
        self.assertFalse(fleetcare_report.rows(report)[0]["qualified"])


if __name__ == "__main__":
    unittest.main()
