#!/usr/bin/env python3
"""Recompute the serialized workload result from raw ledger and replica evidence."""
import argparse
import json
from pathlib import Path
from raft_run import decode_json, input_digest, read_json, require, summarize_ledger, validate_profile
from raft_workload import reconcile
from raft_metrics import phases, periodic


def analyze(root):
    root = Path(root)
    report = read_json(root / 'report.json')
    require(report.get('completed') is True and report.get('reconciled') is True, 'unfinished run')
    profile = validate_profile(report['profile'])
    require(report['profile_sha256'] == input_digest(profile), 'changed workload profile')
    performance = phases(read_json(root / 'measurements.json'), bool(profile['timing']['warmup_seconds']))
    require(performance == report.get('performance'), 'performance differs from raw owner samples')
    with (root / 'node-samples.jsonl').open() as stream:
        periodic((decode_json(line) for line in stream), {row['node']: row['source'] for row in performance['measurement']})
    history = reconcile(root / 'operations.jsonl')
    require(history['cells'] == profile['cells']['total'], 'missing seeded cells')
    summary = summarize_ledger(root / 'operations.jsonl', 'measurement', profile['timing']['measurement_seconds'])
    require(summary == report['summary'], 'summary differs from raw operation ledger')
    require(summary['counts']['offered'] == report['measurement']['offered'], 'offered count differs')
    expected_offered = __import__('math').ceil(profile['arrival']['rate_per_second'] * profile['timing']['measurement_seconds'])
    require(summary['counts']['offered'] == expected_offered, 'arrival schedule incomplete')
    replicas = read_json(root / 'replicas.json')
    seen = set()
    for observed in replicas:
        cell, member = observed['cell'], observed['node']
        key = (cell, member)
        require(key not in seen and member in (1, 2, 3) and cell in history['final'], 'invalid or duplicate replica')
        seen.add(key)
        expected, reply = history['final'][cell], observed['response']
        require(reply['state'] == expected['state'] and reply['index'] >= expected['index'] and reply['served_by'] == member,
                'replica result differs from acknowledged history')
        require(reply['group'] == expected['group'] and reply['incarnation'] == expected['incarnation'], 'replica scope mismatch')
    require(len(seen) == 3 * profile['cells']['total'], 'missing replica')
    archives = report['final_archive']
    require(len(archives) == profile['cells']['total'], 'missing archive observations')
    for cell, (code, archive) in enumerate(archives):
        require(code == 200 and archive['cell'] == cell and archive['archived'] >= history['final'][cell]['index'], 'archive does not cover last acknowledgement')
    require(len(report['cgroups']) == 3 and len({row['node'] for row in report['cgroups']}) == 3, 'missing physical resource evidence')
    for row in report['cgroups']:
        require(row['cpu_max'] == '100000 100000' and row['memory_max'] == 3 * 1024**3 and row['swap_max'] == 0, 'physical cgroup differs')
    return {'verified': True, 'scope': 'healthy serialized workload, three local voters and archive coverage',
            'commands_including_seed': history['commands_including_seed'], 'queries': history['queries'],
            'replicas': len(seen), 'summary': summary, 'performance': performance, 'limitations': report['limitations']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_directory', type=Path)
    print(json.dumps(analyze(parser.parse_args().run_directory), indent=2))
