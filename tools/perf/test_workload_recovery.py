"""An unknown outcome can be resolved only with unchanged identity and proof."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from baseline_analyze import analyze as analyze_baseline, proof_delta
from baseline_history import reconcile as baseline_history, position
from baseline_run import effective_profile
from raft_faults import fault_evidence, validate_scenario
from raft_run import read_json, summarize_ledger
from raft_workload import input_digest, model, raft_position, reconcile as raft_history
from store_faults import validate_store_scenario
from workload_recovery import apply_resolutions, recover_commands, recovery_evidence


def write_events(path, events):
    path.write_text(''.join(json.dumps({**event, 'order': order}) + '\n' for order, event in enumerate(events, 1)))


def fixture(root, adapter='raft', *, no_op=False):
    source = read_json(Path(__file__).with_name('raft_profiles.json'))['profiles']['local-q0-s3']
    source['cells'] = {'total': 1, 'active': 1}
    source['read_fraction'] = 0
    profile = source if adapter == 'raft' else effective_profile(source, adapter)
    config = read_json(Path(__file__).with_name('store_recovery_profile.json'))
    report = {'adapter': adapter, 'scenario': 's3-recovery', 'run_id': 'owned', 'prefix': 'cellar-mr14-owned',
              'profile': profile, 'fault_profile': config, 'fault_profile_sha256': input_digest(config),
              'measurement': {'started_monotonic_seconds': 100, 'elapsed_seconds': 20, 'ambiguous': 1, 'blocked_cells': [0]},
              'store_recovery_verified_monotonic_seconds': 122}
    seed = {'kind': 'initialize', 'database_bytes': 4096}
    initial, _ = model(None, seed)
    client = hashlib.sha256(b'owned:0').hexdigest()[:32]
    def reply(state, changed, sequence):
        shared = {'state': state, 'changed': changed, 'durability': profile['durability']}
        if adapter == 'raft':
            return {**shared, 'index': sequence, 'term': 1, 'group': 'group', 'incarnation': 'history', 'version_token': f'token-{sequence}', 'served_by': None}
        return {**shared, 'sequence': sequence, 'command_id': f'{client}:{sequence}', 'receipt_kind': 'application-transaction', 'scope': 'owned:/cells/0', 'owner': 'node1'}
    change = {'kind': 'transfer', 'expected_revision': 0, 'value': 1000001 if no_op else 100, 'payload': 'x'}
    expected, changed = model(initial, change)
    response = reply(expected, changed, 2)
    operations = [
        {'event': 'seed_invoke', 'cell': 0, 'input': seed, 'input_sha256': input_digest(seed), 'command_id': client + ':1', 'retry_until_ms': 300000},
        {'event': 'seed_complete', 'cell': 0, 'status': 200, 'response': reply(initial, True, 1)},
        {'event': 'invoke', 'phase': 'measurement', 'id': 'measurement:51', 'cell': 0, 'operation': 'command', 'input': change, 'input_sha256': input_digest(change),
         'command_id': client + ':2', 'retry_until_ms': 300000, 'invoke_seconds': 5.1, 'scheduled_seconds': 5.1},
        {'event': 'complete', 'phase': 'measurement', 'id': 'measurement:51', 'cell': 0, 'operation': 'command', 'status': None, 'outcome': 'ambiguous', 'response': {'transport_error': 'TimeoutError'},
         'sent_seconds': 5.11, 'complete_seconds': 11.11, 'timed_out': True, 'latency_ms': 6010},
        {'event': 'reject', 'phase': 'measurement', 'id': 'measurement:120', 'cell': 0, 'operation': 'command', 'reason': 'ambiguous_cell', 'scheduled_seconds': 12},
    ]
    faults = []
    for kind, stamp, paused in (('begin', 105.005, False), ('held', 105.01, True), ('release', 112.02, True), ('released', 112.03, False)):
        faults.append({'event': kind, 'cycle': 0, 'monotonic_seconds': stamp, 'container': 'cellar-mr14-owned-store', 'container_id': 'a' * 64,
                       'state': {'owner': 'owned', 'running': True, 'paused': paused}})
    faults[0]['scheduled_seconds'] = 5
    write_events(root / 'operations.jsonl', operations)
    write_events(root / 'faults.jsonl', faults)
    return report, operations, response, initial


def recover(root, report, send):
    return recover_commands(root, report, send, clock=lambda: 121.0, wall=lambda: 100.0)


def history(root, report, resolutions=()):
    if report['adapter'] == 'raft':
        return raft_history(root / 'operations.jsonl', resolutions=resolutions)
    return baseline_history(root / 'operations.jsonl', report['run_id'], report['profile']['durability'], resolutions=resolutions)


class Recovery(unittest.TestCase):
    def test_committed_or_absent_original_executes_once_and_retains_timeout(self):
        for adapter in ('raft', 'bucket'):
            for committed in (True, False):
                with self.subTest(adapter=adapter, committed=committed), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    report, operations, response, initial = fixture(root, adapter)
                    before = (root / 'operations.jsonl').read_bytes()
                    calls = int(committed)
                    retained = committed
                    def send(cell, kind, body, headers):
                        nonlocal calls, retained
                        self.assertEqual((cell, kind, body), (0, 'command', operations[2]['input']))
                        self.assertEqual(headers, {k: operations[2][k] for k in ('command_id', 'retry_until_ms')})
                        if not retained:
                            calls += 1
                            retained = True
                        return 200, copy.deepcopy(response)
                    with self.assertRaisesRegex(ValueError, 'ambiguous history'):
                        history(root, report)
                    result, resolutions = recover(root, report, send)
                    self.assertEqual(calls, 1)
                    self.assertEqual(result['recovery_requests'], 2)
                    self.assertEqual((root / 'operations.jsonl').read_bytes(), before)
                    checked = history(root, report, resolutions)
                    self.assertEqual(checked['recovered_commands'], 1)
                    self.assertEqual(checked['commands_including_seed'], 2)
                    self.assertEqual(checked['final'][0]['state'], response['state'])
                    summary = summarize_ledger(root / 'operations.jsonl', 'measurement', 20)
                    self.assertEqual(summary['counts']['ambiguous'], 1)
                    self.assertEqual(summary['counts']['timed_out'], 1)
                    self.assertEqual(summary['completed_per_second'], 0)
                    report['recovery'] = result
                    faults = fault_evidence(root, report, summarize_ledger)
                    self.assertEqual(faults['witnesses'][0]['acknowledged_during_hold'], 0)
                    state = {0: {'state': initial, 'next_sequence': 2, 'blocked': True}}
                    apply_resolutions(state, resolutions, raft_position if adapter == 'raft' else position)
                    self.assertFalse(state[0]['blocked'])
                    self.assertEqual(state[0]['next_sequence'], 3)

    def test_no_op_still_requires_one_stable_receipt(self):
        for adapter in ('raft', 'bucket'):
            with self.subTest(adapter=adapter), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                report, _, response, initial = fixture(root, adapter, no_op=True)
                result, resolutions = recover(root, report, Mock(return_value=(200, response)))
                self.assertFalse(response['changed'])
                self.assertEqual(response['state'], initial)
                self.assertEqual(history(root, report, resolutions)['commands_including_seed'], 2)
                rows = [json.loads(line) for line in (root / 'recovery.jsonl').read_text().splitlines()]
                rows[-1]['response']['index' if adapter == 'raft' else 'sequence'] = 3
                write_events(root / 'recovery.jsonl', rows)
                with self.assertRaises(ValueError):
                    recovery_evidence(root, report)

    def test_raft_token_can_be_renewed_only_with_the_same_durable_receipt(self):
        for changes in ({'version_token': 'renewed-token'}, {'version_token': ''},
                        {'version_token': 'renewed-token', 'term': 2},
                        {'version_token': 'renewed-token', 'index': 3}):
            with self.subTest(changes=changes), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                report, _, response, _ = fixture(root)
                second = {**response, **changes}
                send = Mock(side_effect=[(200, response), (200, second)])
                if changes == {'version_token': 'renewed-token'}:
                    checked, resolutions = recover(root, report, send)
                    self.assertEqual(checked['resolved_commands'], 1)
                    self.assertEqual(history(root, report, resolutions)['recovered_commands'], 1)
                    rows = [json.loads(line) for line in (root / 'recovery.jsonl').read_text().splitlines()]
                    self.assertEqual(rows[1]['response']['version_token'], response['version_token'])
                    self.assertEqual(rows[3]['response']['version_token'], 'renewed-token')
                    self.assertEqual(resolutions[0]['response']['version_token'], response['version_token'])
                else:
                    with self.assertRaises(ValueError):
                        recover(root, report, send)

    def test_failed_recovery_or_confirmation_does_not_retry_or_resolve(self):
        for attempt in (1, 2):
            for failed in (TimeoutError('lost response'), (503, {'ambiguous': True})):
                with self.subTest(attempt=attempt, failed=failed), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    report, _, response, _ = fixture(root)
                    send = Mock(side_effect=([(200, response)] if attempt == 2 else []) + [failed])
                    with self.assertRaisesRegex(ValueError, 'remains unresolved'):
                        recover(root, report, send)
                    self.assertEqual(send.call_count, attempt)
                    with self.assertRaises(ValueError):
                        recovery_evidence(root, report)
                    with self.assertRaisesRegex(ValueError, 'ambiguous history'):
                        history(root, report)

    def test_expired_identity_or_exhausted_recovery_window_never_sends(self):
        for clock, wall in ((121, 299), (140, 100), (119, 100)):
            with self.subTest(clock=clock, wall=wall), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                report, _, _, _ = fixture(root)
                send = Mock()
                with self.assertRaises(ValueError):
                    recover_commands(root, report, send, clock=lambda: clock, wall=lambda: wall)
                send.assert_not_called()

    def test_journal_delay_cannot_send_past_either_deadline(self):
        for clock, wall in (([121, 140], [100, 100]), ([121, 121], [100, 299])):
            with self.subTest(clock=clock, wall=wall), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                report, _, _, _ = fixture(root)
                send = Mock()
                with self.assertRaisesRegex(ValueError, 'journal sync exhausted'):
                    recover_commands(root, report, send, clock=Mock(side_effect=clock), wall=Mock(side_effect=wall))
                send.assert_not_called()
                self.assertEqual(len((root / 'recovery.jsonl').read_text().splitlines()), 1)
                with self.assertRaises(ValueError):
                    recovery_evidence(root, report)

    def test_recovery_proof_rejects_changed_identity_scope_clock_or_result(self):
        mutations = {
            'command': lambda r, e: e[0].update(command_id='b' * 32 + ':2'),
            'deadline': lambda r, e: e[0].update(retry_until_ms=400000),
            'input': lambda r, e: e[0]['input'].update(value=200),
            'scope': lambda r, e: e[1]['response'].update(incarnation='other'),
            'old_index': lambda r, e: e[1]['response'].update(index=1),
            'policy': lambda r, e: e[1]['response'].update(durability='quorum'),
            'result': lambda r, e: e[1]['response']['state'].update(left=0),
            'boolean_result': lambda r, e: e[1]['response'].update(changed=1),
            'unconfirmed': lambda r, e: e.pop(),
            'duplicate_order': lambda r, e: e[2].update(attempt=1),
            'early': lambda r, e: e[0].update(monotonic_seconds=119),
            'late': lambda r, e: e[0].update(monotonic_seconds=140),
            'nan': lambda r, e: e[1].update(sent_monotonic_seconds=float('nan')),
            'expired': lambda r, e: e[0].update(wall_ms=300000),
            'expired_at_dispatch': lambda r, e: e[1].update(sent_wall_ms=300000),
            'changed_confirmation': lambda r, e: e[3]['response'].update(index=3, version_token='token-3'),
            'wrong_quarantine': lambda r, e: r['measurement'].update(blocked_cells=[]),
            'boolean_count': lambda r, e: r['measurement'].update(ambiguous=True),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                report, _, response, _ = fixture(root)
                recover(root, report, Mock(return_value=(200, response)))
                rows = [json.loads(line) for line in (root / 'recovery.jsonl').read_text().splitlines()]
                mutate(report, rows)
                write_events(root / 'recovery.jsonl', rows)
                with self.assertRaises(ValueError):
                    recovery_evidence(root, report)

    def test_work_after_ambiguity_or_unknown_completion_cannot_be_folded_away(self):
        for adapter in ('raft', 'bucket'):
            with self.subTest(adapter=adapter), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                report, operations, response, _ = fixture(root, adapter)
                result, resolutions = recover(root, report, Mock(return_value=(200, response)))
                operations[-1] = {**operations[2], 'id': 'measurement:120', 'invoke_seconds': 12}
                write_events(root / 'operations.jsonl', operations)
                with self.assertRaises(ValueError):
                    recovery_evidence(root, report)
                with self.assertRaises(ValueError):
                    history(root, report, resolutions)

    def test_no_ambiguity_cannot_qualify_as_a_recovery_drill(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report, operations, response, _ = fixture(root)
            operations[3].update(status=200, outcome='completed', response=response)
            report['measurement'].update(ambiguous=0, blocked_cells=[])
            write_events(root / 'operations.jsonl', operations)
            send = Mock()
            with self.assertRaisesRegex(ValueError, 'no ambiguous commands'):
                recover(root, report, send)
            send.assert_not_called()


    def test_independent_baseline_analyzer_keeps_measurement_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report, operations, response, _ = fixture(root, 'bucket')
            for index in range(200):
                if index not in (51, 120):
                    operations.append({'event': 'reject', 'phase': 'measurement', 'id': f'measurement:{index}',
                                       'cell': 0, 'operation': 'command', 'reason': 'ambiguous_cell', 'scheduled_seconds': index / 10})
            write_events(root / 'operations.jsonl', operations)
            report['recovery'], _ = recover(root, report, Mock(return_value=(200, response)))
            report.update(completed=True, reconciled=True, limitations=['Synthetic analyzer fixture; no physical qualification.'])
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
            after[0]['sample']['output_proofs'].update(started=1, errors=1)
            recovered = copy.deepcopy(after)
            recovered[0]['sample']['output_proofs'].update(started=3, bucket=2)
            report['proof_samples'] = {'measurement': {'before': before, 'after': after}, 'recovery': {'before': after, 'after': recovered}}
            reads = [{'cell': 0, 'ingress': n, 'response': {**response, 'read_contract': 'owner'}} for n in (1, 2, 3)]
            (root / 'owner-reads.json').write_text(json.dumps(reads))
            (root / 'report.json').write_text(json.dumps(report))
            checked = analyze_baseline(root)
            self.assertTrue(checked['verified'])
            self.assertEqual(checked['summary']['counts']['ambiguous'], 1)
            self.assertEqual(checked['summary']['completed_per_second'], 0)
            self.assertEqual(checked['proof_delta']['errors'], 1)
            self.assertEqual(checked['recovery_proof_delta']['errors'], 0)
            rows = [json.loads(line) for line in (root / 'recovery.jsonl').read_text().splitlines()]
            rows[-1]['response']['sequence'] += 1
            write_events(root / 'recovery.jsonl', rows)
            with self.assertRaises(ValueError):
                analyze_baseline(root)


class Contracts(unittest.TestCase):
    def test_long_pause_is_explicit_and_never_weakens_existing_scenario(self):
        with tempfile.TemporaryDirectory() as directory:
            report, _, _, _ = fixture(Path(directory))
            validate_scenario('s3-recovery', report['fault_profile'], report['profile'])
            for scenario in ('healthy', 's3-outage'):
                with self.subTest(scenario=scenario), self.assertRaisesRegex(ValueError, 'exceeds request budget'):
                    validate_store_scenario(scenario, report['fault_profile'], report['profile'])
            for changes in ({'durability': 'quorum'}, {'timing': {**report['profile']['timing'], 'retry_window_ms': 30000}}):
                with self.assertRaises(ValueError):
                    validate_store_scenario('s3-recovery', report['fault_profile'], {**report['profile'], **changes})

    def test_failed_barriers_are_visible_only_in_explicit_recovery_measurement(self):
        before = [{'node': n, 'sample': {'output_proofs': {k: 0 for k in ('started', 'bucket', 'fleet', 'errors', 'elapsed_us')}}} for n in (1, 2, 3)]
        after = copy.deepcopy(before)
        after[0]['sample']['output_proofs'].update(started=1, errors=1)
        samples = {'before': before, 'after': after}
        with self.assertRaisesRegex(ValueError, 'proof failed'):
            proof_delta(samples, 'bucket')
        self.assertEqual(proof_delta(samples, 'bucket', allow_failed_proofs=True)['errors'], 1)


if __name__ == '__main__':
    unittest.main()
