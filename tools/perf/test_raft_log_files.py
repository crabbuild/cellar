# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import unittest
from unittest.mock import Mock
from raft_log_files import analyze, capture, parse


def rows(stamp, name='0000000000000001.raftlog', size=100):
    return [{'node': f'run-node{n}', 'started_ns': stamp * 10**9 + n, 'finished_ns': stamp * 10**9 + n + 1,
             'identity': {'id': str(n) * 64, 'owner': 'mine', 'started': 'boot', 'running': True, 'paused': False},
             'raw': f'{name}\t{n}\t{size}\t8\n'} for n in range(1, 4)]


def fixture():
    report = {'run_id': 'mine', 'prefix': 'run', 'profile': {'topology': {'nodes': 3}, 'timing': {'warmup_seconds': 0}},
              'measurement': {'started_monotonic_seconds': 1.5, 'elapsed_seconds': 1}}
    after = lambda stamp: rows(stamp, '0000000000000002.rewrite', 60)
    return report, {'measurement': {'before': rows(1), 'after': after(3)}, 'settled': after(5)}, [rows(2), after(4)]


class LogFiles(unittest.TestCase):
    def test_physical_queues_and_removed_file_lower_bound(self):
        report, raw, periodic = fixture()
        result = analyze(raw, periodic, report)
        self.assertEqual(len(result['settled']), 3)
        for node in result['settled']:
            self.assertEqual((node['bytes'], node['allocated_bytes'], node['observed_removed_files'],
                              node['observed_removed_bytes_lower_bound']), (60, 4096, 1, 100))
        self.assertEqual(len(parse('0000000000000001.raftlog\t1\t200\t1\n0000000000000001.rewrite\t2\t1\t8\n')), 2)

    def test_invalid_partial_missing_and_duplicate_stats_refuse(self):
        for value in ('', 'private\t1\t2\t3', '0000000000000001.raftlog\t0\t2\t3',
                      '0000000000000001.raftlog\t1\t-2\t3', '0000000000000001.raftlog\t1\t02\t3'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse(value)
        line = rows(1)[0]['raw']
        with self.assertRaises(ValueError):
            parse(line + line)

    def test_missing_nodes_changed_process_or_unbracketed_workload_refuse(self):
        edits = [lambda r, p: r['settled'].pop(),
                 lambda r, p: r['settled'][0]['identity'].update(started='new'),
                 lambda r, p: r['measurement']['after'][0].update(finished_ns=1),
                 lambda r, p: r['measurement']['before'][0]['identity'].update(owner='foreign'),
                 lambda r, p: p['measurement'].update(elapsed_seconds=4)]
        for change in edits:
            report, raw, periodic = fixture(); change(raw, report)
            with self.assertRaises(ValueError): analyze(raw, periodic, report)
        report, raw, periodic = fixture(); periodic[0][0]['identity']['id'] = 'f' * 64
        with self.assertRaises(ValueError): analyze(raw, periodic[:1], report)

    def test_growing_live_files_do_not_count_as_removal(self):
        report, raw, _ = fixture()
        raw['measurement']['after'] = rows(3, size=200); raw['settled'] = rows(5, size=300)
        result = analyze(raw, [rows(2), rows(4, size=250)], report)
        self.assertTrue(all(n['observed_removed_files'] == 0 for n in result['settled']))

    def test_capture_checks_exact_owner_and_process_before_and_after(self):
        import json
        identity = rows(1)[0]['identity']; wrong = {**identity, 'started': 'other'}
        docker = Mock(side_effect=[json.dumps(identity), rows(1)[0]['raw'], json.dumps(wrong)])
        with self.assertRaises(ValueError): capture(docker, ['run-node1'], 'mine')
