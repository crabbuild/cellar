"""The independent verifier must reject plausible but inconsistent run evidence."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import contextlib
import io

from analyze_raft import analyze


class EvidenceTests(unittest.TestCase):
    def fixture(self):
        raw = {'samples_ms': [10.0], 'receipts': [{'cell': 0, 'version_token': 'opaque', 'index': 3}],
               'elapsed_seconds': 2.0, 'offered_seconds': 1, 'errors': []}
        summary = {'repeat': 1, 'concurrency': 1, 'successful_writes': 1, 'tps': .5,
                   'latency_ms': {'p50': 10.0, 'p95': 10.0, 'p99': 10.0, 'max': 10.0},
                   'errors': [], 'error_free': True, 'error_rate': 0}
        return {
            'seed-journal.jsonl': [{'event': 'outcome', 'status': 200, 'cell': c,
                'command_id': str(c), 'response': {'value': 1, 'index': 3}} for c in range(3)],
            'report.json': {'completed': True, 'cells': 3, 'measurements': [summary],
                            'limits': [{'NanoCpus': 1_000_000_000, 'Memory': 3*1024**3,
                                        'MemorySwap': 3*1024**3}] * 3},
            'write-r1-c1.json': raw,
            'replica-verification.json': [{'node': n, 'cell': c, 'value': 2 if c == 0 else 1,
                                           'index': 3} for n in range(1, 4) for c in range(3)],
            'archive-verification.json': [[200, {'cell': c, 'ready': True, 'leader': c+1,
                                                'archived': 3}] for c in range(3)],
        }

    def run_evidence(self, evidence):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, value in evidence.items():
                (root / name).write_text('\n'.join(map(json.dumps, value)) if name.endswith('.jsonl') else json.dumps(value))
            return analyze(root)

    def test_consistent_evidence(self):
        self.assertEqual(self.run_evidence(self.fixture())['acknowledged_writes_including_seed'], 4)
        self.assertEqual(self.run_evidence(self.fixture())['best_observed_below_limit']['concurrency'], 1)

    def test_two_windows_cannot_reuse_one_receipt_file(self):
        evidence = self.fixture()
        evidence['report.json']['measurements'].append(
            copy.deepcopy(evidence['report.json']['measurements'][0]))
        for replica in evidence['replica-verification.json']:
            if replica['cell'] == 0:
                replica['value'] = 3
        with self.assertRaisesRegex(AssertionError, 'reused measurement evidence'):
            self.run_evidence(evidence)

    def test_latency_limit_is_strict(self):
        evidence = self.fixture()
        evidence['write-r1-c1.json']['samples_ms'] = [200.0]
        evidence['report.json']['measurements'][0]['latency_ms'] = dict.fromkeys(('p50', 'p95', 'p99', 'max'), 200.0)
        self.assertIsNone(self.run_evidence(evidence)['best_observed_below_limit'])

    def test_one_primary_and_two_followers(self):
        evidence = self.fixture()
        evidence['report.json']['primary_only'] = True
        for _, status in evidence['archive-verification.json']:
            status['leader'] = 1
        self.assertTrue(self.run_evidence(evidence)['verified'])

    def test_scratch_tmpfs_cannot_replace_the_durable_volume(self):
        evidence = self.fixture()
        evidence['report.json']['archive_tmpfs_mib'] = 512
        for limits in evidence['report.json']['limits']:
            limits['Tmpfs'] = {'/data/node/raft-cells/archive-verifier': 'rw,size=512m'}
        self.assertTrue(self.run_evidence(evidence)['verified'])
        evidence['report.json']['limits'][0]['Tmpfs'] = {'/data': 'rw,size=512m'}
        with self.assertRaises(AssertionError):
            self.run_evidence(evidence)

    def test_corrupted_evidence_is_rejected(self):
        corruptions = {
            'inflated_tps': lambda e: e['report.json']['measurements'][0].update(tps=100),
            'wrong_percentile': lambda e: e['report.json']['measurements'][0]['latency_ms'].update(p99=1),
            'unacknowledged_write': lambda e: e['replica-verification.json'][0].update(value=3),
            'missing_replica': lambda e: e['replica-verification.json'].pop(),
            'duplicate_replica': lambda e: e['replica-verification.json'].__setitem__(1, e['replica-verification.json'][0]),
            'archive_behind': lambda e: e['archive-verification.json'][0][1].update(archived=2),
            'unmeasured_seed_behind': lambda e: e['archive-verification.json'][2][1].update(archived=2),
            'missing_seed': lambda e: e['seed-journal.jsonl'].pop(),
            'wrong_leader': lambda e: e['archive-verification.json'][0][1].update(leader=2),
            'wrong_cpu': lambda e: e['report.json']['limits'][0].update(NanoCpus=2_000_000_000),
            'unfinished_run': lambda e: e['report.json'].update(completed=False),
        }
        for name, corrupt in corruptions.items():
            with self.subTest(name=name):
                evidence = copy.deepcopy(self.fixture())
                corrupt(evidence)
                with self.assertRaises((AssertionError, ValueError)):
                    self.run_evidence(evidence)

    def test_sql_configuration_requires_matching_physical_owners(self):
        evidence = self.fixture()
        report = evidence['report.json']
        report.update(sql_workers=4, sql_worker_queue=16)
        for phase in ('initial_resources', 'final_resources'):
            report[phase] = [{'sql': {'running_workers': 4, 'worker_capacity': 4,
                'work_reservations': 0, 'work_capacity': 64, 'queue_closed': False}}
                for _ in range(3)]
        self.assertTrue(self.run_evidence(evidence)['verified'])
        corruptions = {
            'wrong_workers': lambda r: r['initial_resources'][0]['sql'].update(running_workers=2),
            'larger_queue': lambda r: r['final_resources'][1]['sql'].update(work_capacity=128),
            'closed_queue': lambda r: r['final_resources'][2]['sql'].update(queue_closed=True),
            'missing_observer': lambda r: r['initial_resources'][0].update(sql=None),
            'missing_node': lambda r: r['final_resources'].pop(),
        }
        for name, corrupt in corruptions.items():
            with self.subTest(name=name):
                changed = copy.deepcopy(evidence)
                corrupt(changed['report.json'])
                with self.assertRaises(AssertionError):
                    self.run_evidence(changed)


class PlacementTests(unittest.TestCase):
    def test_cached_readiness_requires_a_fresh_complete_scan(self):
        from raft import wait_for_placement
        calls = []
        replies = {0: iter([True, False, True, True]),
                   1: iter([False, True, True, True]),
                   2: iter([True, True, True])}
        def balance(cell):
            result = next(replies[cell])
            calls.append((cell, result))
            return result
        wait_for_placement(3, balance, 1, lambda _: None,
            scan=lambda fn, items: list(map(fn, items)), sleep=lambda _: None, now=lambda: 0)
        self.assertEqual(calls[-3:], [(0, True), (1, True), (2, True)])
        self.assertIn((0, False), calls)

    def test_expired_incomplete_placement_refuses_traffic(self):
        from raft import wait_for_placement
        with self.assertRaisesRegex(RuntimeError, 'leader placement deadline'):
            wait_for_placement(3, lambda _: False, 1, lambda _: None,
                scan=lambda fn, items: list(map(fn, items)), now=lambda: 2)


class LoadTests(unittest.TestCase):
    def test_repeated_settings_preserve_every_measurement(self):
        from raft import save_measurement
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = {'repeat': 1, 'concurrency': 12, 'offered_rate': 0,
                     'receipts': [{'command_id': 'first'}]}
            last = {**first, 'receipts': [{'command_id': 'last'}]}
            save_measurement(root, first, 1)
            save_measurement(root, last, 3)
            self.assertNotEqual(first['evidence_file'], last['evidence_file'])
            self.assertEqual(json.loads((root / first['evidence_file']).read_text())['receipts'],
                             [{'command_id': 'first'}])
            with self.assertRaises(FileExistsError):
                save_measurement(root, last, 3)
            self.assertEqual(json.loads((root / last['evidence_file']).read_text())['receipts'],
                             [{'command_id': 'last'}])

    def test_paced_load_keeps_the_full_measurement_interval_and_command_ids(self):
        from raft import load
        class Connection:
            status = 200
            def __init__(self, *args, **kwargs): pass
            def request(self, *args, **kwargs): pass
            def getresponse(self): return self
            def read(self): return b'{"index":3,"value":1,"version_token":"opaque"}'
            def close(self): pass
        output = io.StringIO()
        with patch('raft.http.client.HTTPConnection', Connection), contextlib.redirect_stdout(output):
            load({'cells': 3, 'token': 'test'}, .12, 3, 20)
        result = json.loads(output.getvalue())
        self.assertGreaterEqual(result['elapsed_seconds'], .12)
        self.assertGreaterEqual(result['successful_writes'], 1)
        self.assertLessEqual(result['successful_writes'], 3)
        self.assertEqual(result['offered_rate'], 20)
        self.assertFalse(result['errors'])
        self.assertTrue(all(r['command_id'] and r['retry_until_ms'] for r in result['receipts']))
