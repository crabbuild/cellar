"""Store-outage evidence, lifecycle cleanup and shared runner regressions."""
import copy
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from baseline_analyze import analyze as analyze_baseline
from baseline_history import position, reconcile
from baseline_run import effective_profile
from compare_runs import compare_verified, normalized
from raft_faults import fault_evidence, validate_scenario, verify_fault
from raft_run import read_json, summarize_ledger
from raft_workload import Ledger, input_digest, model, run_load
from store_faults import StorePause, validate_store_fault, validate_store_scenario


def fixture(adapter='raft', durability='s3'):
    profile = read_json(Path(__file__).with_name('raft_profiles.json'))['profiles']['local-q0-s3']
    profile['timing']['measurement_seconds'] = 1
    profile['timing']['warmup_seconds'] = 0
    profile['cells'] = {'total': 1, 'active': 1}
    profile['read_fraction'] = 0
    if adapter != 'raft':
        profile = effective_profile(profile, adapter)
    else:
        profile['durability'] = durability
    config = {'format': 1, 'target': 'object-store', 'pause_ms': 10, 'max_pause_ms': 500,
              'interval_ms': 1000, 'start_after_seconds': 0, 'stop_before_seconds': 0, 'max_start_lateness_ms': 100}
    report = {'adapter': adapter, 'profile': profile, 'scenario': 's3-outage', 'fault_profile': config,
              'fault_profile_sha256': input_digest(config), 'run_id': 'owned', 'prefix': 'cellar-mr14-owned',
              'measurement': {'started_monotonic_seconds': 100, 'elapsed_seconds': 1},
              'store_recovery_verified_monotonic_seconds': 101.1}
    events = []
    for order, (kind, stamp, paused) in enumerate((('begin', 100.01, False), ('held', 100.02, True),
                                                 ('release', 100.16, True), ('released', 100.18, False)), 1):
        events.append({'order': order, 'cycle': 0, 'event': kind, 'monotonic_seconds': stamp,
                       'container': report['prefix'] + '-store', 'container_id': 'a' * 64,
                       'state': {'owner': 'owned', 'running': True, 'paused': paused}})
    events[0]['scheduled_seconds'] = 0
    operations = [
        {'event': 'invoke', 'phase': 'measurement', 'id': 'm:0', 'cell': 0, 'operation': 'command',
         'command_id': 'client:2', 'invoke_seconds': .03},
        {'event': 'complete', 'phase': 'measurement', 'id': 'm:0', 'cell': 0, 'operation': 'command',
         'status': 200, 'outcome': 'completed', 'sent_seconds': .04, 'complete_seconds': .20,
         'response': {'durability': profile['durability']}},
    ]
    return report, events, operations


class Evidence(unittest.TestCase):
    def test_common_injection_preserves_distinct_acknowledgement_guarantees(self):
        for adapter, durability, required in (('raft', 's3', True), ('raft', 'quorum', False),
                                               ('bucket', 'bucket', True), ('fleet', 'fleet-or-bucket', False)):
            with self.subTest(adapter=adapter, durability=durability):
                report, events, operations = fixture(adapter, durability)
                result = verify_fault(report, events, operations)
                self.assertEqual(result['object_store_required_at_ack'], required)
                self.assertEqual(result['witnesses'][0]['acknowledged_during_hold'], 0)
                self.assertAlmostEqual(result['recovery_upper_seconds'], .94)
                operations[-1]['complete_seconds'] = .08
                if required:
                    with self.assertRaisesRegex(ValueError, 'while store was paused'):
                        verify_fault(report, events, operations)
                else:
                    self.assertEqual(verify_fault(report, events, operations)['witnesses'][0]['acknowledged_during_hold'], 1)

    def test_invalid_observations_cannot_qualify(self):
        changes = {
            'foreign_owner': lambda r, e, o: e[1]['state'].update(owner='other'),
            'wrong_target': lambda r, e, o: e[1].update(container=r['prefix'] + '-node1'),
            'reused_name': lambda r, e, o: e[2].update(container_id='b' * 64),
            'invalid_id': lambda r, e, o: e[0].update(container_id=''),
            'missing_pause': lambda r, e, o: e[1]['state'].update(paused=False),
            'early_resume': lambda r, e, o: e[2]['state'].update(paused=False),
            'still_paused': lambda r, e, o: e[3]['state'].update(paused=True),
            'wrong_type': lambda r, e, o: e[1]['state'].update(paused=1),
            'unbounded': lambda r, e, o: e[-1].update(monotonic_seconds=101),
            'too_short': lambda r, e, o: e[1].update(monotonic_seconds=100.155),
            'late': lambda r, e, o: r['measurement'].update(started_monotonic_seconds=99),
            'missing_release': lambda r, e, o: e.pop(),
            'wrong_order': lambda r, e, o: e[2].update(order=1),
            'no_live_traffic': lambda r, e, o: o[0].update(invoke_seconds=.01),
            'unsent_during_pause': lambda r, e, o: o[1].update(sent_seconds=.19),
            'invalid_clock': lambda r, e, o: o[1].update(sent_seconds=float('nan')),
            'negative_service': lambda r, e, o: o[1].update(sent_seconds=.3),
            'wrong_cell': lambda r, e, o: o[1].update(cell=1),
            'wrong_policy': lambda r, e, o: o[1]['response'].update(durability='quorum'),
            'ambiguous': lambda r, e, o: o[1].update(status=None, outcome='ambiguous'),
            'rejected_only': lambda r, e, o: o[1].update(status=503, outcome='rejected'),
            'incomplete': lambda r, e, o: o.pop(),
            'duplicate': lambda r, e, o: o.insert(1, copy.deepcopy(o[0])),
            'early_recovery': lambda r, e, o: r.update(store_recovery_verified_monotonic_seconds=100.5),
            'unbounded_recovery': lambda r, e, o: r.update(store_recovery_verified_monotonic_seconds=200),
            'changed_profile': lambda r, e, o: r['fault_profile'].update(pause_ms=11),
        }
        for name, change in changes.items():
            with self.subTest(name=name):
                report, events, operations = fixture()
                change(report, events, operations)
                with self.assertRaises(ValueError):
                    verify_fault(report, events, operations)

    def test_healthy_control_has_same_schedule_without_physical_pause(self):
        report, events, operations = fixture()
        report['scenario'] = 'healthy'
        for event in events:
            event['state']['paused'] = False
        operations[-1]['complete_seconds'] = .08
        self.assertEqual(verify_fault(report, events, operations)['witnesses'][0]['acknowledged_during_hold'], 1)
        report['scenario'] = 's3-outage'
        with self.assertRaises(ValueError):
            verify_fault(report, events, operations)

    def test_each_scheduled_window_needs_its_own_submitted_command(self):
        report, events, operations = fixture()
        report['profile']['timing']['measurement_seconds'] = 2
        report['measurement']['elapsed_seconds'] = 2
        report['store_recovery_verified_monotonic_seconds'] = 102.1
        next_cycle = copy.deepcopy(events)
        for event in next_cycle:
            event.update(order=event['order'] + 4, cycle=1, monotonic_seconds=event['monotonic_seconds'] + 1)
        next_cycle[0]['scheduled_seconds'] = 1
        events.extend(next_cycle)
        with self.assertRaisesRegex(ValueError, 'no successful new command'):
            verify_fault(report, events, operations)
        next_command = copy.deepcopy(operations)
        for event in next_command:
            event['id'] = 'm:1'
            for clock in ('invoke_seconds', 'sent_seconds', 'complete_seconds'):
                if clock in event:
                    event[clock] += 1
        next_command[0]['command_id'] = 'client:3'
        operations.extend(next_command)
        self.assertEqual(verify_fault(report, events, operations)['cycles'], 2)

    def test_comparison_includes_baseline_fault_profile_and_preserves_contract_refusal(self):
        verified = {'verified': True, 'summary': {'completed_per_second': 10}}
        report, _, _ = fixture('bucket')
        report.update(vm={}, limits=[], build={'build_profile': 'lab', 'source_manifest_sha256': 'source', 'binary_sha256': 'binary'})
        left = normalized(report, verified)
        report['fault_profile'] = {**report['fault_profile'], 'pause_ms': 20}
        result = compare_verified(left, normalized(report, verified))
        self.assertFalse(result['matched_contract'])
        self.assertIn('workload_contract', result['differences'])
        report['adapter'] = 'raft'
        report['profile']['durability'] = 's3'
        result = compare_verified(left, normalized(report, verified))
        self.assertFalse(result['matched_contract'])
        self.assertIn('durability_contract', result['differences'])
        self.assertNotIn('throughput_ratio', result)


class Profiles(unittest.TestCase):
    def test_both_clis_require_valid_store_profile_before_resources(self):
        from raft_run import main as raft_main
        from baseline_run import main as baseline_main
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'run'
            for name, main, extra in (('raft_run', raft_main, []), ('baseline_run', baseline_main, ['--mode', 'bucket', '--image', 'unused'])):
                args = [name, '--profile', str(Path(__file__).with_name('raft_profiles.json')),
                        '--scenario', 's3-outage', '--output', str(output), *extra]
                with self.subTest(runner=name), patch('sys.argv', args), patch(name + '.subprocess.run') as docker:
                    with self.assertRaisesRegex(ValueError, 'requires a store fault profile'):
                        main()
                    docker.assert_not_called()
                    self.assertFalse(output.exists())
                invalid = Path(directory) / 'invalid.json'
                invalid.write_text(json.dumps({'format': 1, 'target': 'object-store'}))
                with self.subTest(runner=name, invalid=True), patch('sys.argv', args + ['--fault-profile', str(invalid)]), patch(name + '.subprocess.run') as docker:
                    with self.assertRaisesRegex(ValueError, 'invalid store fault fields'):
                        main()
                    docker.assert_not_called()
                    self.assertFalse(output.exists())

    def test_unavailable_capabilities_and_bad_bounds_refuse_before_resources(self):
        report, _, _ = fixture()
        profile, config = report['profile'], report['fault_profile']
        self.assertEqual(validate_store_fault(config, profile), [0])
        for key, value in [('format', True), ('target', 'node1'), ('pause_ms', 0), ('max_pause_ms', 1000),
                           ('start_after_seconds', 2), ('unknown', 1), ('interval_ms', False)]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_store_fault({**config, key: value}, profile)
        with self.assertRaises(ValueError):
            validate_store_fault(config, {**profile, 'read_fraction': 1})
        for scenario, config in [('s3-outage', None), ('snapshot-crash', config), ('slow-follower', config)]:
            with self.subTest(scenario=scenario), self.assertRaises(ValueError):
                validate_scenario(scenario, config, profile)
        with self.assertRaises(ValueError):
            validate_store_scenario('slow-follower', None, profile)


class Control:
    """Test-only freezer adapter; no Docker or native durability claim."""
    def __init__(self, mode=None):
        self.identity = 'a' * 64
        self.mode = mode
        self.state = {'owner': 'owned', 'running': True, 'paused': False}
        self.actions = []
        self.held = threading.Event()
        self.available = threading.Event()
        self.available.set()
        self.closed = False

    def __call__(self, *args):
        assert args[-1] == 'cellar-mr14-owned-store'
        self.actions.append(args[0])
        if args[0] == 'inspect':
            return json.dumps(self.state)
        self.state['paused'] = args[0] == 'pause'
        if self.state['paused']:
            self.available.clear()
            self.held.set()
        else:
            self.available.set()
        if args[0] == self.mode:
            raise TimeoutError('lost control response')
        return ''

    def close(self):
        self.closed = True


class Lifecycle(unittest.TestCase):
    def test_ambiguous_pause_or_release_is_inspected_and_fails_qualification(self):
        for lost in ('pause', 'unpause'):
            with self.subTest(lost=lost), tempfile.TemporaryDirectory() as directory:
                report, _, _ = fixture()
                control = Control(lost)
                owner = StorePause(directory, report['scenario'], report['fault_profile'], report['profile'], report['prefix'], report['run_id'], control)
                owner.start(time.monotonic())
                with self.assertRaisesRegex(ValueError, 'lost control response'):
                    owner.finish()
                owner.close()
                self.assertFalse(control.state['paused'])
                self.assertTrue(control.closed)
                self.assertIn('unpause', control.actions)
                self.assertEqual(control.actions[-1], 'inspect')
                self.assertFalse(owner.thread.is_alive())

    def test_cancel_joins_and_unpauses_the_owned_store(self):
        with tempfile.TemporaryDirectory() as directory:
            report, _, _ = fixture()
            report['fault_profile']['pause_ms'] = 200
            control = Control()
            owner = StorePause(directory, report['scenario'], report['fault_profile'], report['profile'], report['prefix'], report['run_id'], control)
            owner.start(time.monotonic())
            self.assertTrue(control.held.wait(1))
            owner.close()
            self.assertFalse(control.state['paused'])
            self.assertFalse(owner.thread.is_alive())
            self.assertIsNotNone(owner.failure)

    def test_foreign_or_already_paused_store_is_never_mutated(self):
        for change in ({'owner': 'other'}, {'paused': True}):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                report, _, _ = fixture()
                control = Control()
                control.state.update(change)
                owner = StorePause(directory, report['scenario'], report['fault_profile'], report['profile'], report['prefix'], report['run_id'], control)
                owner.start(time.monotonic())
                with self.assertRaises(ValueError):
                    owner.finish()
                owner.close()
                self.assertEqual(control.actions, ['inspect'])

    def test_baseline_ledger_and_fault_owner_integrate_without_retrying_mutations(self):
        report, _, _ = fixture('bucket')
        profile = report['profile']
        profile['arrival']['rate_per_second'] = 10
        profile['arrival']['max_lateness_ms'] = 1000
        report['fault_profile'].update(pause_ms=200, max_pause_ms=900, max_start_lateness_ms=500)
        report['fault_profile_sha256'] = input_digest(report['fault_profile'])
        initial = {'kind': 'initialize', 'database_bytes': 4096}
        state, changed = model(None, initial)
        import hashlib
        client = hashlib.sha256(b'owned:0').hexdigest()[:32]
        response = {'scope': 'owned:/cells/0', 'state': state, 'changed': changed, 'sequence': 1,
                    'command_id': client + ':1', 'owner': 'node1', 'durability': 'bucket', 'receipt_kind': 'application-transaction'}
        current = {0: {'state': state, 'next_sequence': 2, 'sequence': 1}}
        control = Control()
        submitted = []
        def send(cell, operation, body, headers):
            submitted.append(headers['command_id'])
            if not control.available.wait(2):
                raise TimeoutError('fixture store unavailable')
            state, changed = model(current[cell]['state'], body)
            return 200, {**response, 'command_id': headers['command_id'], 'sequence': current[cell]['next_sequence'], 'state': state, 'changed': changed}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            owner = StorePause(root, report['scenario'], report['fault_profile'], profile, report['prefix'], report['run_id'], control)
            ledger = Ledger(root / 'operations.jsonl')
            try:
                ledger.append({'event': 'seed_invoke', 'cell': 0, 'input': initial, 'input_sha256': input_digest(initial), 'command_id': client + ':1'})
                ledger.append({'event': 'seed_complete', 'cell': 0, 'status': 200, 'response': response})
                def start(began):
                    owner.start(began)
                    self.assertTrue(control.held.wait(1))
                    # Wait for the durable held observation before admitting the
                    # first arrival; this test checks evidence, not throughput.
                    deadline = time.monotonic() + 1
                    while len((root / 'faults.jsonl').read_text().splitlines()) < 2:
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(.001)
                report['measurement'] = run_load(profile, 'measurement', 1, current, ledger, send, 'owned', position=position, on_start=start)
                owner.finish()
            finally:
                owner.close()
                ledger.close()
            report['store_recovery_verified_monotonic_seconds'] = time.monotonic()
            history = reconcile(root / 'operations.jsonl', 'owned', 'bucket')
            self.assertEqual(history['commands_including_seed'], len(submitted) + 1)
            self.assertEqual(len(set(submitted)), len(submitted))
            result = fault_evidence(root, report, summarize_ledger)
            self.assertGreater(result['witnesses'][0]['submitted_during_hold'], 0)
            self.assertEqual(result['witnesses'][0]['acknowledged_during_hold'], 0)
            # Exercise the independent baseline analyzer through raw files; no
            # native proof counter, cgroup or final-state validation is bypassed.
            source = read_json(Path(__file__).with_name('raft_profiles.json'))['profiles']['local-q0-s3']
            for key in ('timing', 'cells', 'arrival', 'read_fraction'):
                source[key] = copy.deepcopy(profile[key])
            report.update(source_profile=source, profile_sha256=input_digest(profile), completed=True, reconciled=True,
                          summary=summarize_ledger(root / 'operations.jsonl', 'measurement', 1), fault_result=result, limitations=['Python fixture only'])
            final = history['final'][0]
            (root / 'owner-reads.json').write_text(json.dumps([{'cell': 0, 'ingress': n, 'response': {'scope': final['scope'], 'sequence': final['sequence'], 'state': final['state'], 'read_contract': 'owner', 'owner': 'node1'}} for n in (1, 2, 3)]))
            report['cgroups'] = [{'node': n, 'cpu_max': '100000 100000', 'memory_max': 3 * 1024**3, 'swap_max': 0} for n in (1, 2, 3)]
            before = [{'node': n, 'sample': {'output_proofs': {k: 0 for k in ('started', 'bucket', 'fleet', 'errors', 'elapsed_us')}}} for n in (1, 2, 3)]
            after = copy.deepcopy(before)
            after[0]['sample']['output_proofs'].update(started=len(submitted), bucket=len(submitted))
            report['proof_samples'] = {'measurement': {'before': before, 'after': after}}
            (root / 'report.json').write_text(json.dumps(report))
            self.assertTrue(analyze_baseline(root)['verified'])
            report['fault_result']['witnesses'][0]['acknowledged_during_hold'] = 100
            (root / 'report.json').write_text(json.dumps(report))
            with self.assertRaisesRegex(ValueError, 'differs from raw'):
                analyze_baseline(root)


if __name__ == '__main__':
    unittest.main()
