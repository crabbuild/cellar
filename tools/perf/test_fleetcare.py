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


class ControlledSetupClockTests(unittest.TestCase):
    def test_real_minute_window_leaves_time_for_http_prewarm(self):
        for phase, expected in ((0, 2000), (1999, 1), (2000, 0),
                                (4999, 0), (5000, 57000), (59999, 2001)):
            for minute in (0, 29700000):
                with self.subTest(phase=phase, minute=minute):
                    self.assertEqual(fleetcare.controlled_setup_delay_ms(minute*60000+phase), expected)

    def test_start_gate_rechecks_real_clock_after_wait(self):
        h = fleetcare.FleetCareHarness.__new__(fleetcare.FleetCareHarness)
        with tempfile.TemporaryDirectory() as d:
            h.output = pathlib.Path(d)
            with patch.object(fleetcare, 'command', side_effect=['59000', '62000']) as command, \
                 patch.object(fleetcare.time, 'sleep') as sleep:
                h.wait_controlled_setup('embedded-fleet')
            sleep.assert_called_once_with(3.0)
            self.assertEqual(command.call_count, 2)
            record = json.loads((h.output/'embedded-fleet-setup-clock.json').read_text())
            self.assertEqual(record['start_utc_ms'], 62000)
            self.assertFalse(record['clock_changed'])


class SourceDigestTests(unittest.TestCase):
    def test_moved_application_changes_invalidate_build_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            files = ('Cargo.toml', 'Cargo.lock', 'tools/perf/fleetcare.Dockerfile',
                     'examples/fleetcare/Cargo.toml', 'examples/fleetcare/src/lib.rs',
                     'examples/fleetcare/tests/projection_recovery.rs')
            for name in files:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('original')
            with patch.object(fleetcare, 'ROOT', root):
                original = fleetcare.source_digest()
                self.assertEqual(original, fleetcare.source_digest())
                for name in files[3:]:
                    with self.subTest(path=name):
                        path = root / name
                        path.write_text('changed')
                        self.assertNotEqual(original, fleetcare.source_digest())
                        path.write_text('original')


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


class CapacityResourcesTests(unittest.TestCase):
    def test_cpu_sets_do_not_overlap_and_main_container_gets_full_requested_limits(self):
        sets = []
        for index in range(3):
            value = fleetcare.node_resources(SimpleNamespace(node_resources="capacity"), index)
            self.assertEqual((value["cpus"], value["memory"], value["threads"]), ("2", "4096m", "2"))
            sets.append(set(map(int, value["cpu_set"].split(","))))
        self.assertEqual(set.union(*sets), set(range(6)))
        self.assertEqual(sum(map(len, sets)), 6)

    def test_capacity_inventory_and_tenant_count(self):
        self.assertEqual(fleetcare.PROFILE_COUNTS["capacity"], (3000, 6))


class ServiceLivenessTests(unittest.TestCase):
    def test_stopped_backend_or_node_prevents_new_load_and_saves_evidence(self):
        for stopped in range(4):
            states = [{"Name":name,"State":{"Running":index != stopped, "OOMKilled":index == stopped, "ExitCode":137 if index == stopped else 0}}
                      for index,name in enumerate(("/store","/node1","/node2","/node3"))]
            h = fleetcare.FleetCareHarness.__new__(fleetcare.FleetCareHarness)
            h.store, h.nodes = "store", ["node1","node2","node3"]
            with tempfile.TemporaryDirectory() as directory:
                h.output = pathlib.Path(directory)
                with patch.object(fleetcare, "command", return_value=json.dumps(states)) as command:
                    with self.assertRaisesRegex(RuntimeError, "required service stopped"):
                        h.client("load", "failed-dependency")
                command.assert_called_once_with("docker", "inspect", "store", "node1", "node2", "node3")
                observed = json.loads((h.output / "failed-dependency-service-states.json").read_text())
                self.assertTrue(observed[stopped]["oom_killed"])
                self.assertFalse(observed[stopped]["running"])


class IngressRoutingTests(unittest.TestCase):
    def test_client_uses_its_vm_bridge_instead_of_colimas_macos_gateway(self):
        h = fleetcare.FleetCareHarness.__new__(fleetcare.FleetCareHarness)
        h.args = SimpleNamespace(port=42100, concurrency=128, verification_concurrency=8, bytes=1024,
                                 generator_cpu_set="8,9", image="fixture-image")
        h.tag = h.network = "private-fixture"
        h.vm_root = "/private-fixture"
        h.host, h.durability, h.token = "embedded", "bucket", "fixture-token"
        calls = []
        def command(*args, **kwargs):
            calls.append(args)
            if args[:3] == ("docker", "network", "inspect"):
                return json.dumps([{"IPAM":{"Config":[{"Gateway":"172.28.0.1"}]}}])
            if args[:2] == ("docker", "create"):
                raise RuntimeError("fixture capture")
            return ""
        with patch.object(fleetcare, "command", command), self.assertRaisesRegex(RuntimeError, "fixture capture"):
            h.client("receipts", "fixture")
        created = calls[-1]
        for node in range(1,4):
            self.assertIn(f"node{node}:172.28.0.1", created)
        self.assertFalse(any("host-gateway" in value for value in created))
        self.assertIn("nofile=65536:65536", created)



class CriticalOwnershipTests(unittest.TestCase):
    def harness(self, queue_nodes=(0, 1, 2)):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        harness = fleetcare.FleetCareHarness.__new__(fleetcare.FleetCareHarness)
        harness.args = SimpleNamespace(profile='capacity', ingress='haproxy')
        harness.output = pathlib.Path(directory.name)
        harness.host, harness.durability = 'embedded', 'fleet'
        harness.checks = []
        harness.snapshot = lambda *args, **kwargs: None
        states = {f'node{i+1}': {'owned_cells':1000, 'residents':[]} for i in range(3)}
        cells, expected = [], {}
        for kind, count in [('queue', 3), ('dlq', 3), ('sql', 18), ('inventory', 6)]:
            for index in range(count):
                scope = f'{kind}:{index}'
                node = queue_nodes[index] if kind == 'queue' else index % 3
                cells.append({'kind':kind, 'scope':scope, 'tenant':index, 'id':0 if kind=='inventory' else index,
                              'path':f'/api/{index}/inventory/0' if kind=='inventory' else f'/system/{kind}/{index}/0'})
                states[f'node{node+1}']['residents'].append(scope)
                expected[scope] = f'node{index%3+1}'
        ordinal = 0
        for node, state in states.items():
            while len(state['residents']) < 1000:
                scope = f'devices:{ordinal}'
                ordinal += 1
                state['residents'].append(scope)
                cells.append({'kind':'devices', 'scope':scope, 'path':f'/api/0/devices/{ordinal}'})
            (harness.output / f'embedded-fleet-seed-{node}-state.json').write_text(json.dumps(state))
        (harness.output / 'embedded-fleet-manifest.json').write_text(json.dumps({'count':3000, 'cells':cells}))
        harness.expected_critical_owners = expected
        return harness

    def test_prewarm_pins_exact_owners_from_each_initial_balancer_cursor(self):
        maps = []
        for initial in range(3):
            with self.subTest(initial=initial):
                harness = self.harness()
                manifest = json.loads((harness.output / 'embedded-fleet-manifest.json').read_text())
                harness.balancer_stats_url = 'fixture'
                cursor = initial
                selected = {f'node{i+1}':0 for i in range(3)}
                owners = {}
                def setup(targets, label):
                    nonlocal cursor
                    rows = []
                    for target in targets:
                        node = f'node{cursor%3+1}'
                        cursor += 1
                        selected[node] += 1
                        value = manifest if target['path']=='/manifest' else {}
                        if target.get('scope'):
                            owners[target['scope']] = node
                            if target['kind']=='inventory':
                                value = {'scope':target['scope'], 'owner':node,
                                         'state':{'version':0, 'commands':0}}
                        rows.append({**target, 'backend':node, 'status':200, 'value':value})
                    return rows
                def states(*args):
                    return {node:{'residents':[scope for scope, owner in owners.items() if owner==node]}
                            for node in selected}
                def stats(*args):
                    return {node:{'status':'UP', 'lbtot':str(count)} for node,count in selected.items()}
                harness._setup_http = setup
                harness._critical_states = states
                with patch.object(fleetcare.balancer, 'statistics', stats):
                    harness.prewarm_critical('embedded-fleet')
                roots = fleetcare.balancer.critical_roots(manifest)
                expected = {root['scope']:f'node{(i+1)%3+1}' for i,root in enumerate(roots)}
                self.assertEqual(owners, expected)
                maps.append(owners)
        self.assertTrue(all(mapping==maps[0] for mapping in maps))

    def test_balanced_root_totals_cannot_hide_concentrated_primary_queues(self):
        harness = self.harness(queue_nodes=(0, 0, 0))
        with self.assertRaisesRegex(RuntimeError, 'primary queue'):
            harness.ownership_check('embedded-fleet', 'seed')


    def test_balanced_critical_owners_are_recorded(self):
        harness = self.harness()
        harness.ownership_check('embedded-fleet', 'seed')
        observations = [c for c in harness.checks if c['action'] == 'critical_ownership']
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0]['owners'], harness.expected_critical_owners)
        self.assertEqual(observations[0]['counts']['sql'], {'node1':6, 'node2':6, 'node3':6})
        self.assertEqual(observations[0]['counts']['inventory'], {'node1':2, 'node2':2, 'node3':2})

    def test_balanced_counts_cannot_hide_owner_identity_drift(self):
        harness = self.harness()
        expected = harness.expected_critical_owners
        expected['sql:0'], expected['sql:1'] = expected['sql:1'], expected['sql:0']
        with self.assertRaisesRegex(RuntimeError, 'changed from'):
            harness.ownership_check('embedded-fleet', 'seed')

    def test_missing_or_duplicate_critical_owners_refuse_measurement(self):
        for duplicate in (False, True):
            with self.subTest(duplicate=duplicate):
                harness = self.harness()
                path = harness.output / 'embedded-fleet-seed-node1-state.json'
                state = json.loads(path.read_text())
                state['residents'].remove('queue:0')
                path.write_text(json.dumps(state))
                if duplicate:
                    # Duplicated ownership cannot qualify a stable placement.
                    for node in (2, 3):
                        path = harness.output / f'embedded-fleet-seed-node{node}-state.json'
                        state = json.loads(path.read_text())
                        state['residents'].append('queue:0')
                        path.write_text(json.dumps(state))
                with self.assertRaisesRegex(RuntimeError, 'missing or duplicated'):
                    harness.ownership_check('embedded-fleet', 'seed')


if __name__ == "__main__":
    unittest.main()
