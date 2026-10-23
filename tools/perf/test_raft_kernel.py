"""Kernel evidence must retain identities, limits, units and monotone counters."""
import json
import unittest
from unittest.mock import Mock

from raft_kernel import analyze, capture, parse


def raw_sample(stamp=1, store=False):
    fields = {
        'cpu': f'usage_usec {stamp * 100}\nuser_usec {stamp * 70}\nsystem_usec {stamp * 30}\nnr_periods {stamp}\nnr_throttled 0\nthrottled_usec 0',
        'quota': '200000 100000' if store else '100000 100000',
        'memory': '1024', 'peak': '2048', 'limit': str((4 if store else 3) * 1024**3),
        'events': 'low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0',
        'swap': '0', 'swap_limit': '0', 'io': f'252:0 rbytes={stamp * 512} wbytes={stamp * 1024} rios={stamp} wios={stamp} dbytes=0 dios=0',
        'status': 'Name:\tapp\nVmRSS:\t1 kB\nVmHWM:\t2 kB\nThreads:\t4',
        'process': '1 (app (worker)) S ' + ' '.join(['0'] * 18 + ['42']), 'fds': '12',
    }
    return ''.join('FILE ' + key + '\n' + value + '\n' for key, value in fields.items())


def rows(report, stamp):
    count = report['profile']['topology']['nodes']
    names = [report['prefix'] + '-node' + str(n) for n in range(1, count + 1)] + [report['prefix'] + '-store']
    return [{'node': name, 'started_ns': stamp * 10**9 + n * 1000, 'finished_ns': stamp * 10**9 + n * 1000 + 500,
             'identity': {'id': str(n + 1) * 64, 'owner': report['run_id'], 'running': True, 'paused': False, 'started': '2026-10-22T00:00:00Z'},
             'raw': raw_sample(stamp, name.endswith('-store'))} for n, name in enumerate(names)]


def fixture():
    report = {'run_id': 'owned', 'prefix': 'fixture', 'profile': {'topology': {'nodes': 3}, 'timing': {'warmup_seconds': 1}, 'slo': {'memory_headroom_fraction': .1}}}
    report.update(warmup={'started_monotonic_seconds': 1.1, 'elapsed_seconds': .7}, measurement={'started_monotonic_seconds': 3.1, 'elapsed_seconds': .7})
    raw = {'warmup': {'before': rows(report, 1), 'after': rows(report, 2)},
           'measurement': {'before': rows(report, 3), 'after': rows(report, 4)}}
    return report, raw


class Evidence(unittest.TestCase):
    def test_process_identity_units_and_phase_deltas(self):
        parsed = parse(raw_sample())
        self.assertEqual(parsed['process_start_ticks'], 42)
        self.assertEqual(parsed['process']['VmRSS'], 1024)
        report, raw = fixture()
        result = analyze(raw, report)['measurement']
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0]['cpu_delta']['usage_usec'], 100)
        self.assertEqual(result[0]['cgroup_io_delta']['252:0']['wbytes'], 1024)
        self.assertTrue(result[0]['average_cpu_cores']['minimum'] < .0001 < result[0]['average_cpu_cores']['maximum'])
        self.assertTrue(all(row['memory_objective_met'] for row in result))

    def test_incomplete_duplicate_invalid_units_and_negative_counters_refuse(self):
        for before, after in [('FILE memory', 'FILE unknown'), ('FILE memory', 'FILE cpu'), ('usage_usec 100', 'usage_usec -1'),
                              ('1 kB', '1 bytes'), ('peak\n2048', 'peak\n1'), ('quota\n100000 100000', 'quota\nmax 100000'),
                              ('wbytes=1024', 'wbytes=1024 wbytes=1024'), ('oom_group_kill 0\n', ''), ('1 (app (worker))', '2 (app)')]:
            with self.subTest(before=before), self.assertRaises(ValueError):
                parse(raw_sample().replace(before, after))

    def test_source_changes_missing_nodes_limits_reset_and_phase_overlap_refuse(self):
        changes = {
            'missing_store': lambda r, p: r['measurement']['after'].pop(),
            'duplicate_node': lambda r, p: r['measurement']['after'][1].update(node='fixture-node1'),
            'restart': lambda r, p: r['measurement']['after'][0]['identity'].update(started='new'),
            'foreign_owner': lambda r, p: r['measurement']['before'][0]['identity'].update(owner='foreign'),
            'paused': lambda r, p: r['measurement']['before'][0]['identity'].update(paused=True),
            'clock': lambda r, p: r['measurement']['after'][0].update(started_ns=True),
            'overlap': lambda r, p: r['measurement'].update(after=rows(p, 3)),
            'reset': lambda r, p: r['measurement']['after'][0].update(raw=raw_sample(1)),
            'pid_reuse': lambda r, p: r['measurement']['after'][0].update(raw=raw_sample(4).replace('0 42\n', '0 43\n')),
            'quota': lambda r, p: r['measurement']['after'][0].update(raw=raw_sample(4, True)),
            'missing_phase': lambda r, p: r.pop('warmup'),
            'outside_workload': lambda r, p: p['measurement'].update(elapsed_seconds=2),
            'io_disappeared': lambda r, p: r['measurement']['after'][0].update(raw=raw_sample(4).replace('252:0 ', '252:1 ')),
        }
        for name, change in changes.items():
            report, raw = fixture();change(raw, report)
            with self.subTest(name=name), self.assertRaises(ValueError):
                analyze(raw, report)

    def test_oom_or_low_headroom_is_reported_without_losing_raw_evidence(self):
        for old, new in [('oom 0', 'oom 1'), ('peak\n2048', 'peak\n3000000000')]:
            report, raw = fixture()
            raw['measurement']['after'][0]['raw'] = raw['measurement']['after'][0]['raw'].replace(old, new)
            self.assertFalse(analyze(raw, report)['measurement'][0]['memory_objective_met'])

    def test_capture_refuses_foreign_target_and_changed_process_during_read(self):
        report, raw = fixture();identity = raw['warmup']['before'][0]['identity']
        for changed in ({**identity, 'started': 'new'}, {**identity, 'id': 'a' * 64}):
            docker = Mock(side_effect=[json.dumps(identity), raw_sample(), json.dumps(changed)])
            with self.assertRaises(ValueError):
                capture(docker, ['fixture-node1'], 'owned')
        docker = Mock(return_value=json.dumps({**identity, 'owner': 'foreign'}))
        with self.assertRaises(ValueError):
            capture(docker, ['fixture-node1'], 'owned')
        self.assertEqual(docker.call_count, 1)


if __name__ == '__main__':
    unittest.main()
