#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Independently check native Raft run receipts, replica counts and TPS/quantiles."""
import argparse
import collections
import json
import math
from pathlib import Path


def analyze(root, p99_limit_ms=200):
    report = json.loads((root / 'report.json').read_text())
    if not report['completed']:
        raise ValueError('run did not complete its replica/archive verification')
    expected = collections.Counter({c: 1 for c in range(report['cells'])})
    latest = {}
    seeds = {}
    for line in (root / 'seed-journal.jsonl').read_text().splitlines():
        item = json.loads(line)
        if item['event'] == 'outcome' and item['status'] == 200:
            c = item['cell']
            assert item['response']['value'] == 1
            if c in seeds:
                assert seeds[c] == item['command_id']
            seeds[c] = item['command_id']
            latest[c] = item['response']['index']
    assert set(seeds) == set(range(report['cells']))
    windows = []
    measured_files = set()
    for summary in report['measurements']:
        path = (root / summary.get('evidence_file', f"write-r{summary['repeat']}-c{summary['concurrency']}.json")).resolve()
        assert path not in measured_files, 'reused measurement evidence'
        measured_files.add(path)
        raw = json.loads(path.read_text())
        samples = sorted(raw['samples_ms'])
        assert len(samples) == len(raw['receipts']) == summary['successful_writes'] > 0
        assert summary['tps'] == len(samples) / raw['elapsed_seconds']
        assert raw['elapsed_seconds'] >= raw['offered_seconds']
        for name, p in [('p50', .5), ('p95', .95), ('p99', .99)]:
            assert summary['latency_ms'][name] == samples[math.ceil(p * len(samples)) - 1]
        assert summary['latency_ms']['max'] == samples[-1]
        assert summary['errors'] == raw['errors']
        assert all(e.get('status') == 503 and e.get('error') == 'Overloaded' for e in raw['errors'])
        assert summary['error_free'] == (not raw['errors'])
        assert summary['error_rate'] == len(raw['errors']) / (len(samples) + len(raw['errors']))
        for receipt in raw['receipts']:
            c = receipt['cell']
            assert 0 <= c < report['cells'] and receipt['version_token']
            expected[c] += 1
            latest[c] = max(latest.get(c, 0), receipt['index'])
        windows.append({**{k: summary[k] for k in ('repeat', 'concurrency', 'successful_writes', 'tps', 'latency_ms', 'error_rate')}, 'offered_rate': summary.get('offered_rate', 0)})
    replicas = json.loads((root / 'replica-verification.json').read_text())
    assert len(replicas) == 3 * report['cells']
    identities = set()
    for replica in replicas:
        key = (replica['node'], replica['cell'])
        assert key not in identities and 1 <= key[0] <= 3 and 0 <= key[1] < report['cells']
        identities.add(key)
        assert replica['value'] == expected[key[1]]
        assert replica['index'] >= latest.get(key[1], 0)
    archives = json.loads((root / 'archive-verification.json').read_text())
    assert len(archives) == report['cells']
    leaders = collections.Counter()
    for c, (code, status) in enumerate(archives):
        assert code == 200 and status['cell'] == c and status['ready']
        assert status['leader'] == (1 if report.get('primary_only') else c % 3 + 1)
        assert status['archived'] >= latest.get(c, 0)
        leaders[status['leader']] += 1
    if report.get('primary_only'):
        assert dict(leaders) == {1: report['cells']}
    else:
        assert list(sorted(leaders.values())) == [report['cells'] // 3] * 3
    for limits in report['limits']:
        assert limits['NanoCpus'] == 1_000_000_000
        assert limits['Memory'] == limits['MemorySwap'] == 3 * 1024**3
        if report.get('archive_tmpfs_mib'):
            assert report['archive_tmpfs_mib'] == 512
            scratch = limits['Tmpfs']
            assert set(scratch) == {'/data/node/raft-cells/archive-verifier'}
            assert 'size=512m' in next(iter(scratch.values()))
    if 'archive_batch_groups' in report:
        assert report['archive_batch_groups'] in (1, 2)
        for phase in ('initial_resources', 'final_resources'):
            assert len(report[phase]) == 3
            for usage in report[phase]:
                assert usage['archive_batch_groups'] == report['archive_batch_groups']
    if 'archive_max_unarchived_mib' in report:
        assert 1 <= report['archive_max_unarchived_mib'] <= 16384
        for phase in ('initial_resources', 'final_resources'):
            assert len(report[phase]) == 3
            for usage in report[phase]:
                assert usage['archive_max_unarchived_bytes'] == report['archive_max_unarchived_mib'] * 1024**2
    if 'sql_workers' in report or 'sql_worker_queue' in report:
        assert 1 <= report['sql_workers'] <= 16 and 1 <= report['sql_worker_queue'] <= 256
        for phase in ('initial_resources', 'final_resources'):
            assert len(report[phase]) == 3
            for usage in report[phase]:
                sql = usage['sql']
                assert sql is not None and not sql['queue_closed']
                assert sql['running_workers'] == sql['worker_capacity'] == report['sql_workers']
                assert sql['work_capacity'] == report['sql_workers'] * report['sql_worker_queue']
                assert 0 <= sql['work_reservations'] <= sql['work_capacity']
    candidates = []
    for concurrency,rate in sorted({(w['concurrency'],w['offered_rate']) for w in windows}):
        group = [w for w in windows if (w['concurrency'],w['offered_rate']) == (concurrency,rate)]
        if all(w['error_rate'] == 0 and w['latency_ms']['p99'] < p99_limit_ms for w in group):
            candidates.append({'concurrency': concurrency, 'offered_rate': rate,
                'mean_tps': sum(w['tps'] for w in group) / len(group),
                'min_tps': min(w['tps'] for w in group),
                'worst_p99_ms': max(w['latency_ms']['p99'] for w in group),
                'windows': len(group)})
    return {'verified': True, 'cells': report['cells'], 'replica_values': len(replicas),
            'acknowledged_writes_including_seed': sum(expected.values()), 'windows': windows,
            'p99_limit_ms': p99_limit_ms,
            'best_observed_below_limit': max(candidates, key=lambda c: c['mean_tps'], default=None)}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run', type=Path)
    parser.add_argument('--p99-limit-ms', type=float, default=200)
    args = parser.parse_args()
    print(json.dumps(analyze(args.run, args.p99_limit_ms), indent=2))
