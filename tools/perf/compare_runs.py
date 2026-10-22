#!/usr/bin/env python3
"""Compare only verified runs; expose contract differences before any ratio."""
import argparse
import json
from pathlib import Path
from raft_run import matched_comparison, read_json, require


def contract(report):
    mode = report.get('adapter', 'raft')
    durability = report['profile']['durability']
    if mode == 'raft':
        require(durability in ('quorum', 's3'), 'unknown Raft durability')
        return {'authority': 'raft-majority', 'local_durable_copies_at_ack': 'majority-of-3',
                'object_store_at_ack': durability == 's3', 'receipts': 'raft-command',
                'reads': report['profile']['read_consistency']}
    require(mode in ('fleet', 'bucket'), 'unknown adapter')
    return {'authority': 'bucket-ownership-and-fenced-node-log' if mode == 'fleet' else 'bucket-ownership',
            'local_durable_copies_at_ack': 'current-ensemble-or-bucket' if mode == 'fleet' else 'not-required',
            'object_store_at_ack': mode == 'bucket', 'receipts': 'application-transaction', 'reads': 'owner'}


def normalized(report, verified):
    require(verified['verified'] is True, 'unverified input')
    profile = report['profile']
    workload = {key: value for key, value in profile.items() if key not in ('durability', 'qualification', 'slo', 'topology', 'resources', 'tick_ms')}
    # A matching compiler profile alone is insufficient: sources and the shared
    # reference workload must also be exactly the same build input snapshot.
    workload['tick_ms'] = profile['tick_ms'] if report.get('adapter', 'raft') == 'raft' else None
    workload['scenario'] = report['scenario']
    workload['fault_profile'] = report['fault_profile'] if report.get('adapter', 'raft') == 'raft' else None
    return {'durability_contract': contract(report),
            'hardware': {'resources': profile['resources'], 'vm': report['vm'], 'nodes': profile['topology']['nodes'],
                         'failure_domains': profile['topology']['failure_domains'], 'limits': report['limits']},
            'workload_contract': workload,
            'build_profile': {'profile': report['build']['build_profile'], 'source_manifest': report['build']['source_manifest_sha256'], 'binary_sha256': report['build']['binary_sha256']},
            'reconciled': True, 'completed_per_second': verified['summary']['completed_per_second']}


def compare(left, right):
    def load(path):
        report = read_json(Path(path) / 'report.json')
        if report.get('adapter', 'raft') == 'raft':
            from raft_analyze import analyze
        else:
            from baseline_analyze import analyze
        return normalized(report, analyze(path))
    return compare_verified(load(left), load(right))


def compare_verified(left, right):
    keys = ('durability_contract', 'hardware', 'workload_contract', 'build_profile')
    differences = {key: {'left': left[key], 'right': right[key]} for key in keys if left[key] != right[key]}
    if differences:
        return {'matched_contract': False, 'differences': differences,
                'limitation': 'These observations are not an equivalent-guarantee speedup.'}
    return {**matched_comparison(left, right),
            'limitation': 'Matching recorded inputs does not establish steady state, instrumentation overhead or causal performance on a shared host.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('left', type=Path)
    parser.add_argument('right', type=Path)
    args = parser.parse_args()
    print(json.dumps(compare(args.left, args.right), indent=2))
