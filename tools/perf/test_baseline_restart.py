"""Fencing, restart identity, pre-retry loss detection and resumed workload checks."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from baseline_analyze import analyze
from baseline_history import position, reconcile
from baseline_restart import SCENARIO, restart_nodes, verify_restart
from baseline_resume import audit_before_retry, continuation_events, resume_writes, verify_audit
from compare_runs import normalized
from raft_faults import fault_evidence
from raft_run import read_json, summarize_ledger
from raft_workload import input_digest, model
from store_faults import validate_store_scenario
from test_workload_recovery import fixture, write_events
from workload_recovery import apply_resolutions, recover_commands


def setup(root, *, present=False):
    report, operations, response, initial = fixture(root, 'bucket')
    report.update(scenario=SCENARIO, build={'binary_sha256': 'b' * 64}, processes_before=[])
    config = read_json(Path(__file__).with_name('store_fenced_recovery_profile.json'))
    report.update(fault_profile=config, fault_profile_sha256=input_digest(config))
    faults = [json.loads(line) for line in (root / 'faults.jsonl').read_text().splitlines()]
    for row, stamp in zip(faults, (103.005, 103.01, 115.02, 115.03)):
        row['monotonic_seconds'] = stamp
    faults[0]['scheduled_seconds'] = 3
    write_events(root / 'faults.jsonl', faults)
    rows, stopped = [], []
    for n in (1, 2, 3):
        node = f"{report['prefix']}-node{n}"
        start = {'event': 'embedded_node_started', 'lease_ttl_ms': 10000, 'lease_generation': str(n) * 64, 'executors': 'caller_owned'}
        old = {'id': str(n) * 64, 'name': '/' + node, 'owner': report['run_id'], 'image': 'sha256:' + 'a' * 64,
               'restart_policy': 'no', 'mounts': [{'Name': node, 'Destination': '/data', 'Type': 'volume', 'RW': True}],
               'state': {'Running': True, 'Paused': False, 'OOMKilled': False, 'ExitCode': 0, 'StartedAt': '2026-10-22T00:00:01Z'},
               'events': [start], 'monotonic_seconds': 90 + n}
        report['processes_before'].append(copy.deepcopy(old))
        old['state'].update(Running=False, ExitCode=1)
        old['events'].append({'event': 'embedded_node_stopped', 'outcome': 'fenced', 'reason': 'node_lease_expired', 'fence_code': 3, 'caller_executors_alive': True})
        old['monotonic_seconds'] = 120 + n / 10
        stopped.append(old)
        rows.append({'event': 'fenced', 'node': n, 'observation': copy.deepcopy(old), 'monotonic_seconds': 120 + n / 10})
    rows.extend([{'event': 'start_invoked', 'container_ids': [row['id'] for row in stopped], 'monotonic_seconds': 121},
                 {'event': 'start_returned', 'response_lost': False, 'monotonic_seconds': 122}])
    for n, old in enumerate(stopped, 1):
        row = copy.deepcopy(old)
        row['state'].update(Running=True, ExitCode=0, StartedAt='2026-10-22T00:01:01Z')
        row['events'].append({**row['events'][0], 'lease_generation': str(n + 3) * 64})
        row.update(health_status=200, binary_sha256='b' * 64, monotonic_seconds=123 + n / 10)
        rows.append({'event': 'ready', 'node': n, 'observation': row, 'monotonic_seconds': row['monotonic_seconds']})
    write_events(root / 'restart.jsonl', rows)
    report['restart'] = verify_restart(root, report)
    observed = response if present else operations[1]['response']
    query = {key: observed[key] for key in ('state', 'scope', 'sequence', 'owner')}
    query['read_contract'] = 'owner'
    report['before_recovery'] = audit_before_retry(root, report, Mock(return_value=(200, query)), clock=lambda: 124)
    return report, operations, response, initial, rows


def resolved(root, report, response, initial):
    report['recovery'], resolutions = recover_commands(root, report, Mock(return_value=(200, response)), clock=lambda: 126, wall=lambda: 100)
    state = {0: {'state': initial, 'next_sequence': 2, 'blocked': True}}
    apply_resolutions(state, resolutions, position)
    def send(cell, kind, body, headers):
        expected, changed = model(state[cell]['state'], body)
        return 200, {**response, 'state': expected, 'changed': changed,
                     'command_id': headers['command_id'], 'sequence': state[cell]['next_sequence']}
    resume_writes(root, report, state, send, clock=lambda: 127, wall=lambda: 100)
    return resolutions, state, continuation_events(root, report, resolutions)


class Restart(unittest.TestCase):
    def test_pre_retry_audit_distinguishes_present_and_absent_unknown_effect(self):
        for present in (True, False):
            with self.subTest(present=present), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                report, _, response, initial, _ = setup(root, present=present)
                original = (root / 'operations.jsonl').read_bytes()
                resolutions, state, continuation = resolved(root, report, response, initial)
                self.assertEqual(report['before_recovery']['ambiguous_cells_present'], [0] if present else [])
                checked = reconcile(root / 'operations.jsonl', 'owned', 'bucket', resolutions=resolutions, continuation=continuation)
                self.assertEqual(checked['commands_including_seed'], 3)
                self.assertEqual(checked['final'][0]['state'], state[0]['state'])
                self.assertEqual(checked['final'][0]['sequence'], 3)
                self.assertEqual((root / 'operations.jsonl').read_bytes(), original)
                self.assertEqual(summarize_ledger(root / 'operations.jsonl', 'measurement', 20)['counts']['ambiguous'], 1)

    def test_restart_requires_exact_physical_owner_and_joined_fence(self):
        mutations = {
            'running': lambda r, e: e[0]['observation']['state'].update(Running=True),
            'oom': lambda r, e: e[0]['observation']['state'].update(OOMKilled=True),
            'wrong_failure': lambda r, e: e[0]['observation']['events'][1].update(outcome='replication_failed'),
            'wrong_fence': lambda r, e: e[0]['observation']['events'][1].update(reason='node_lease_missing'),
            'not_joined': lambda r, e: e[0]['observation']['events'][1].update(caller_executors_alive=False),
            'wrong_owner': lambda r, e: e[0]['observation'].update(owner='elsewhere'),
            'wrong_container': lambda r, e: e[0]['observation'].update(id='f' * 64),
            'volume_changed': lambda r, e: e[5]['observation']['mounts'][0].update(Name='other'),
            'image_changed': lambda r, e: e[5]['observation'].update(image='sha256:' + 'c' * 64),
            'binary_changed': lambda r, e: e[5]['observation'].update(binary_sha256='c' * 64),
            'generation_reused': lambda r, e: e[5]['observation']['events'][2].update(lease_generation='1' * 64),
            'process_reused': lambda r, e: e[5]['observation']['state'].update(StartedAt='2026-10-22T00:00:01Z'),
            'old_start_time': lambda r, e: e[5]['observation']['state'].update(StartedAt='2026-10-21T00:00:01Z'),
            'start_target_changed': lambda r, e: e[3].update(container_ids=['f' * 64]),
            'late': lambda r, e: e[-1].update(monotonic_seconds=150),
            'early': lambda r, e: e[0].update(monotonic_seconds=119),
            'missing_owner': lambda r, e: e.pop(),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                report, _, _, _, rows = setup(root)
                mutate(report, rows)
                write_events(root / 'restart.jsonl', rows)
                with self.assertRaises(ValueError):
                    verify_restart(root, report)

    def test_lost_start_reply_is_inspected_without_replaying_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report, _, _, _, rows = setup(root)
            (root / 'restart.jsonl').unlink()
            observations = [copy.deepcopy(row['observation']) for row in rows[:3] + rows[5:]]
            for row in observations:
                row['monotonic_seconds'] = 123.5
            def docker(*args):
                if args[0] == 'start':
                    raise ValueError('response lost after start')
                self.assertEqual(args[0], 'exec')
                return 'b' * 64 + '  /proc/1/exe'
            send = Mock(side_effect=docker)
            with patch('baseline_restart.observe', side_effect=observations):
                checked = restart_nodes(root, report, send, Mock(return_value=200), clock=lambda: 123.5)
            self.assertEqual(checked['restarted_containers'], 3)
            self.assertEqual(sum(call.args[0] == 'start' for call in send.call_args_list), 1)
            self.assertTrue(json.loads((root / 'restart.jsonl').read_text().splitlines()[4])['response_lost'])

    def test_no_start_until_every_old_owner_is_proven_fenced(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report, _, _, _, rows = setup(root)
            (root / 'restart.jsonl').unlink()
            rows[1]['observation']['events'][1]['outcome'] = 'core_failed'
            docker = Mock()
            with patch('baseline_restart.observe', side_effect=[row['observation'] for row in rows[:3]]):
                with self.assertRaisesRegex(ValueError, 'expiry fence'):
                    restart_nodes(root, report, docker, Mock(), clock=lambda: 123.5)
            docker.assert_not_called()

    def test_acknowledged_prefix_cannot_be_repaired_by_replay(self):
        for change in ({'sequence': 0}, {'sequence': 3}, {'scope': 'other'}, {'state': {}}, {'owner': 'node4'}):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                report, _, response, _, _ = setup(root)
                rows = [json.loads(line) for line in (root / 'before-recovery.jsonl').read_text().splitlines()]
                rows[0]['response'].update(change)
                write_events(root / 'before-recovery.jsonl', rows)
                with self.assertRaises(ValueError):
                    verify_audit(root, report)
                send = Mock(return_value=(200, response))
                with self.assertRaises(ValueError):
                    recover_commands(root, report, send, clock=lambda: 126, wall=lambda: 100)
                send.assert_not_called()

    def test_failed_new_command_keeps_original_and_partial_resumed_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report, _, response, initial, _ = setup(root)
            report['recovery'], resolutions = recover_commands(root, report, Mock(return_value=(200, response)), clock=lambda: 126, wall=lambda: 100)
            state = {0: {'state': initial, 'next_sequence': 2, 'blocked': True}}
            apply_resolutions(state, resolutions, position)
            send = Mock(side_effect=TimeoutError('lost response'))
            with self.assertRaisesRegex(ValueError, 'post-restart mutation failed'):
                resume_writes(root, report, state, send, clock=lambda: 127, wall=lambda: 100)
            self.assertEqual(send.call_count, 1)
            with self.assertRaises(ValueError):
                continuation_events(root, report, resolutions)

    def test_independent_analysis_never_bridges_reset_counters_or_compares_performance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report, operations, response, initial, _ = setup(root)
            for index in range(200):
                if index not in (51, 120):
                    operations.append({'event': 'reject', 'phase': 'measurement', 'id': f'measurement:{index}', 'cell': 0,
                                       'operation': 'command', 'reason': 'ambiguous_cell', 'scheduled_seconds': index / 10})
            write_events(root / 'operations.jsonl', operations)
            resolutions, state, _ = resolved(root, report, response, initial)
            report.update(completed=True, reconciled=True, traffic_objectives_met=False, measurement_proofs_complete=False,
                          store_recovery_verified_monotonic_seconds=128, limitations=['Synthetic restart fixture.'])
            report['source_profile'] = copy.deepcopy(report['profile'])
            report['source_profile'].update(durability='s3', read_consistency='at-least')
            report['source_profile']['topology']['voters'] = 3
            report['profile_sha256'] = input_digest(report['profile'])
            report['measurement']['offered'] = 200
            report['summary'] = summarize_ledger(root / 'operations.jsonl', 'measurement', 20)
            report['fault_result'] = fault_evidence(root, report, summarize_ledger)
            report['cgroups'] = [{'node': n, 'cpu_max': '100000 100000', 'memory_max': 3 * 1024**3, 'swap_max': 0} for n in (1, 2, 3)]
            before = [{'node': n, 'sample': {'output_proofs': {k: 0 for k in ('started', 'bucket', 'fleet', 'errors', 'elapsed_us')}}} for n in (1, 2, 3)]
            after = copy.deepcopy(before)
            after[0]['sample']['output_proofs'].update(started=1, bucket=1)
            report['proof_samples'] = {'measurement': {'before': before, 'after': None},
                                      'recovery': {'before': before, 'after': before}, 'resumed': {'before': before, 'after': after}}
            reads = [{'cell': 0, 'ingress': n, 'response': {**response, 'sequence': 3, 'state': state[0]['state'], 'read_contract': 'owner'}} for n in (1, 2, 3)]
            (root / 'owner-reads.json').write_text(json.dumps(reads))
            (root / 'report.json').write_text(json.dumps(report))
            checked = analyze(root)
            self.assertTrue(checked['verified'])
            self.assertIsNone(checked['proof_delta'])
            self.assertEqual(checked['summary']['completed_per_second'], 0)
            self.assertEqual(checked['commands_including_seed'], 3)
            with self.assertRaisesRegex(ValueError, 'incomplete measurement counters'):
                normalized(report, checked)
            report['proof_samples']['measurement']['after'] = after
            (root / 'report.json').write_text(json.dumps(report))
            with self.assertRaisesRegex(ValueError, 'presented as a benchmark'):
                analyze(root)

    def test_scenario_keeps_original_lease_and_resource_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            report, _, _, _, _ = setup(Path(directory))
            validate_store_scenario(SCENARIO, report['fault_profile'], report['profile'])
            for policy in ('s3', 'quorum', 'fleet-or-bucket'):
                with self.assertRaises(ValueError):
                    validate_store_scenario(SCENARIO, report['fault_profile'], {**report['profile'], 'durability': policy})
            for pause in (7000, '12000', None, True):
                with self.assertRaises(ValueError):
                    validate_store_scenario(SCENARIO, {**report['fault_profile'], 'pause_ms': pause}, report['profile'])


if __name__ == '__main__':
    unittest.main()
