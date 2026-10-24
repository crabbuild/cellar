# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Fault claims require owned physical injection, exact witnesses and recovery."""
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from raft_faults import SlowFollower, preflight_pause, validate_fault, validate_scenario, verify_fault
from raft_run import read_json
from raft_workload import input_digest
from raft_probe_http import ProbeTrace


def fixture():
    profile = read_json(Path(__file__).with_name('raft_profiles.json'))['profiles']['local-q0']
    profile['timing']['measurement_seconds'] = 1
    profile['cells'] = {'total': 3, 'active': 3}
    config = {'format': 1, 'node': 3, 'pause_ms': 10, 'max_pause_ms': 500,
              'interval_ms': 1000, 'start_after_seconds': 0, 'stop_before_seconds': 0, 'max_start_lateness_ms': 100}
    report = {'scenario': 'slow-follower', 'profile': profile, 'fault_profile': config,
              'fault_profile_sha256': input_digest(config), 'run_id': 'owned',
              'measurement': {'started_monotonic_seconds': 100, 'elapsed_seconds': 1}, 'replicas_verified_monotonic_seconds': 101.1}
    state = {'owner': 'owned', 'running': True, 'paused': False}
    response = {'cell': 0, 'node': 1, 'leader': 1, 'role': 'Leader', 'term': 2,
                'replication': {'group': 'group', 'incarnation': 'history', 'term': 2, 'committed': 5,
                                'members': [{'member': n, 'matched': 3 if n == 3 else 5, 'voter': True} for n in (1, 2, 3)]}}
    events = [
        {'event': 'begin', 'monotonic_seconds': 100.01, 'scheduled_seconds': 0, 'state': state},
        {'event': 'held', 'monotonic_seconds': 100.02, 'state': {**state, 'paused': True}},
        {'event': 'probe', 'monotonic_seconds': 100.15, 'started_monotonic_seconds': 100.14, 'cell': 0, 'status': 200, 'response': response},
        {'event': 'release', 'monotonic_seconds': 100.16, 'state': {**state, 'paused': True}},
        {'event': 'released', 'monotonic_seconds': 100.18, 'state': state},
    ]
    events = [{**event, 'order': n, 'cycle': 0} for n, event in enumerate(events, 1)]
    operations = [
        {'event': 'invoke', 'phase': 'measurement', 'id': 'm:0', 'cell': 0, 'operation': 'command', 'command_id': 'client:2', 'invoke_seconds': .03},
        {'event': 'complete', 'phase': 'measurement', 'id': 'm:0', 'cell': 0, 'operation': 'command', 'status': 200, 'complete_seconds': .08,
         'response': {'group': 'group', 'incarnation': 'history', 'term': 2, 'index': 5}},
    ]
    return report, events, operations


class Profiles(unittest.TestCase):
    def test_bounded_schedule_and_probe_cohorts(self):
        report, _, _ = fixture()
        probes, offsets = validate_fault(report['fault_profile'], report['profile'])
        self.assertEqual((probes, offsets), ([0], [0.0]))
        for key, value in [('node', 4), ('pause_ms', True), ('max_pause_ms', 5), ('interval_ms', 1),
                           ('start_after_seconds', 2), ('format', 2), ('unknown', 1)]:
            config = {**report['fault_profile'], key: value}
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_fault(config, report['profile'])
        with self.assertRaisesRegex(ValueError, 'requires a fault profile'):
            validate_scenario('slow-follower', None, report['profile'])
        with self.assertRaisesRegex(ValueError, 'fault capability'):
            validate_scenario('disk-stall', None, report['profile'])


class Evidence(unittest.TestCase):
    def test_majority_progress_is_bound_to_the_observed_pause_and_receipt(self):
        report, events, operations = fixture()
        result = verify_fault(report, events, operations)
        self.assertEqual(result['healthy_majority_witnesses'], [{'cycle': 0, 'cell': 0, 'command_id': 'client:2', 'index': 5}])
        self.assertEqual(result['lag_samples'], [{'cycle': 0, 'cell': 0, 'entries': 2}])
        self.assertEqual(result['cohorts'], {'target_leader': [2], 'target_query_follower': [1], 'target_third_voter': [0], 'unaffected': []})
        self.assertAlmostEqual(result['recovery_upper_seconds'], .94)

    def test_missing_injection_wrong_proof_and_unbounded_recovery_refuse(self):
        changes = {
            'no_pause': lambda r, e, o: e[1]['state'].update(paused=False),
            'early_resume': lambda r, e, o: e[-2]['state'].update(paused=False),
            'foreign_owner': lambda r, e, o: e[1]['state'].update(owner='other'),
            'numeric_pause': lambda r, e, o: e[1]['state'].update(paused=1),
            'paused_after_cleanup': lambda r, e, o: e[-1].update(state={**e[-1]['state'], 'paused': True}),
            'missing_release': lambda r, e, o: e.pop(),
            'late': lambda r, e, o: r['measurement'].update(started_monotonic_seconds=99),
            'long_pause': lambda r, e, o: e[-1].update(monotonic_seconds=101),
            'short_pause': lambda r, e, o: e[1].update(monotonic_seconds=100.155),
            'no_lag': lambda r, e, o: e[2]['response']['replication']['members'][2].update(matched=5),
            'no_majority': lambda r, e, o: e[2]['response']['replication']['members'][1].update(matched=4),
            'old_request': lambda r, e, o: o[0].update(invoke_seconds=.01),
            'late_response': lambda r, e, o: o[1].update(complete_seconds=.2),
            'wrong_scope': lambda r, e, o: o[1]['response'].update(incarnation='other'),
            'wrong_term': lambda r, e, o: o[1]['response'].update(term=1),
            'wrong_voters': lambda r, e, o: e[2]['response']['replication']['members'][1].update(member=3),
            'wrong_leader': lambda r, e, o: e[2]['response'].update(leader=3),
            'no_recovery': lambda r, e, o: r.update(replicas_verified_monotonic_seconds=200),
            'nan_clock': lambda r, e, o: e[2].update(monotonic_seconds=float('nan')),
            'nan_command_clock': lambda r, e, o: o[0].update(invoke_seconds=float('nan')),
            'early_recovery': lambda r, e, o: r.update(replicas_verified_monotonic_seconds=100.5),
            'changed_config': lambda r, e, o: r['fault_profile'].update(pause_ms=11),
            'extra_cycle': lambda r, e, o: e[-1].update(cycle=1),
        }
        for name, change in changes.items():
            report, events, operations = fixture()
            change(report, events, operations)
            with self.subTest(name=name), self.assertRaises(ValueError):
                verify_fault(report, events, operations)

    def test_failed_probe_cannot_be_hidden_by_a_later_success(self):
        report, events, operations = fixture()
        events.insert(2, {'event': 'probe_failed', 'cycle': 0, 'monotonic_seconds': 100.13,
                         'started_monotonic_seconds': 100.03, 'cell': 0,
                         'timeout_seconds': .1, 'elapsed_seconds': .1, 'error_type': 'TimeoutError'})
        for order, event in enumerate(events, 1):
            event['order'] = order
        with self.assertRaisesRegex(ValueError, 'incomplete fault cycle observations'):
            verify_fault(report, events, operations)

    def test_healthy_control_cannot_be_relabelled_as_an_injected_run(self):
        report, events, operations = fixture()
        report['scenario'] = 'healthy'
        events[1]['state']['paused'] = False
        events[-2]['state']['paused'] = False
        self.assertEqual(verify_fault(report, events, operations)['cycles'], 1)
        report['scenario'] = 'slow-follower'
        with self.assertRaises(ValueError):
            verify_fault(report, events, operations)
        report.update(scenario='healthy', fault_profile=None)
        report['fault_profile_sha256'] = input_digest(None)
        with self.assertRaises(ValueError):
            verify_fault(report, events, operations)
        self.assertIsNone(verify_fault(report, [], []))


class Ownership(unittest.TestCase):
    def test_capability_probe_cleans_up_after_success_or_ambiguous_actions(self):
        for lost in (None, 'run', 'pause'):
            with self.subTest(lost=lost):
                state = {'owner': 'owned', 'running': True, 'paused': False}
                actions, existing = [], False
                def docker(*args):
                    nonlocal existing
                    actions.append(args)
                    if args[0] == 'info':
                        return 'linux'
                    if args[0] == 'run':
                        existing = True
                        self.assertIn('none', args)
                    elif args[0] == 'inspect':
                        return json.dumps(state)
                    elif args[0] == 'ps':
                        return 'container-id' if existing else ''
                    elif args[0] in ('pause', 'unpause'):
                        state['paused'] = args[0] == 'pause'
                    elif args[0] == 'rm':
                        existing = False
                    if args[0] == lost:
                        raise TimeoutError('ambiguous Docker action')
                    return ''
                with patch('raft_faults.secrets.token_hex', return_value='owned'):
                    if lost:
                        with self.assertRaises(TimeoutError):
                            preflight_pause(docker, 'image')
                    else:
                        self.assertTrue(preflight_pause(docker, 'image')['removed'])
                self.assertFalse(existing)
                self.assertFalse(state['paused'])
                self.assertIn(('rm', '-f', 'cellar-mr14-preflight-owned'), actions)

    def run_owner(self, mode):
        report, events, _ = fixture()
        state = {'owner': 'other' if mode == 'foreign' else 'owned', 'running': True, 'paused': False}
        actions = []
        def docker(*args):
            actions.append(args[0])
            self.assertEqual(args[-1], 'fixture-node3')
            if args[0] == 'inspect':
                return json.dumps(state)
            state['paused'] = args[0] == 'pause'
            if mode == 'lost_pause_response' and args[0] == 'pause':
                raise TimeoutError('lost pause response')
            return ''
        probe = Mock(return_value=(200, events[2]['response']))
        if mode == 'probe_failure':
            def fail_probe(cell, timeout, probe_id, trace):
                def stalled_headers():
                    raise TimeoutError('probe timeout')
                return trace.call('response_headers', stalled_headers)
            probe.side_effect = fail_probe
        with tempfile.TemporaryDirectory() as directory:
            owner = SlowFollower(directory, 'healthy' if mode == 'control' else 'slow-follower',
                                 report['fault_profile'], report['profile'], 'fixture', 'owned', docker, probe)
            owner.drive(time.monotonic())
            if mode in ('control', 'probe_failure'):
                probe.assert_called_once()
                self.assertEqual(probe.call_args.args[:3], (0, .1, '0:0'))
                self.assertIsInstance(probe.call_args.args[3], ProbeTrace)
            owner.close()
            recorded = [json.loads(line) for line in (Path(directory) / 'faults.jsonl').read_text().splitlines()]
        return state, actions, recorded, owner.failure

    def test_lost_pause_response_and_probe_failure_still_unpause_exact_owner(self):
        for mode in ('lost_pause_response', 'probe_failure'):
            with self.subTest(mode=mode):
                state, actions, recorded, failure = self.run_owner(mode)
                self.assertFalse(state['paused'])
                self.assertIn('unpause', actions)
                self.assertIsNotNone(failure)
                self.assertEqual(recorded[-1]['event'], 'failure')

    def test_probe_failure_records_attempt_before_release(self):
        state, actions, recorded, failure = self.run_owner('probe_failure')
        errors = [row for row in recorded if row['event'] == 'probe_failed']
        self.assertEqual(len(errors), 1)
        error = errors[0]
        self.assertEqual(error['probe_id'], '0:0')
        self.assertEqual((error['cycle'], error['cell'], error['error_type']), (0, 0, 'TimeoutError'))
        self.assertEqual(error['timeout_seconds'], .1)
        self.assertEqual(error['http_trace']['events'][0]['phase'], 'response_headers')
        self.assertEqual(error['http_trace']['events'][0]['error_type'], 'TimeoutError')
        self.assertGreaterEqual(error['elapsed_seconds'], 0)
        self.assertGreaterEqual(error['monotonic_seconds'] - error['started_monotonic_seconds'], error['elapsed_seconds'])
        released = next(row for row in recorded if row['event'] == 'released')
        self.assertLess(error['order'], released['order'])
        self.assertFalse(state['paused'])
        self.assertIn('unpause', actions)
        self.assertEqual(failure, 'probe timeout')

    def test_foreign_target_is_never_mutated_and_control_never_pauses(self):
        for mode in ('foreign', 'control'):
            state, actions, _, failure = self.run_owner(mode)
            self.assertTrue(all(action == 'inspect' for action in actions))
            self.assertFalse(state['paused'])
            self.assertEqual(failure is None, mode == 'control')

    def test_close_cancels_and_joins_a_scheduled_owner(self):
        report, _, _ = fixture()
        report['profile']['timing']['measurement_seconds'] = 6
        report['fault_profile']['start_after_seconds'] = 5
        with tempfile.TemporaryDirectory() as directory:
            docker = Mock()
            owner = SlowFollower(directory, 'slow-follower', report['fault_profile'], report['profile'], 'fixture', 'owned', docker, Mock())
            owner.start(time.monotonic())
            owner.close()
            self.assertFalse(owner.thread.is_alive())
            docker.assert_not_called()
            self.assertIsNotNone(owner.failure)


if __name__ == '__main__':
    unittest.main()
