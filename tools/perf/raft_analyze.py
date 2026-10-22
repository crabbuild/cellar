#!/usr/bin/env python3
"""Recompute the serialized workload result from raw ledger and replica evidence."""
import argparse
import json
from pathlib import Path
from raft_run import decode_json, input_digest, read_json, require, summarize_ledger, validate_profile
from raft_workload import reconcile
from raft_metrics import phases, periodic
from raft_faults import fault_evidence, validate_scenario
from workload_recovery import recovery_evidence
from raft_kernel import analyze as analyze_kernel
from raft_topology import local_placement, members, verify_topology, verify_resources, verify_populations, verify_traffic


def analyze(root):
    root = Path(root)
    report = read_json(root / 'report.json')
    require(report.get('completed') is True and report.get('reconciled') is True, 'unfinished run')
    profile = validate_profile(report['profile'])
    validate_scenario(report['scenario'], report['fault_profile'], profile)
    require(report['profile_sha256'] == input_digest(profile), 'changed workload profile')
    node_count = profile['topology']['nodes']
    planned = local_placement(profile)
    require(report['placement'] == planned, 'recorded placement differs from profile')
    verify_resources(report, node_count)
    topology = read_json(root / 'topology.json')
    require(set(topology) == {'before', 'after'}, 'missing topology boundary')
    for rows in topology.values():
        verify_topology(rows, profile['cells']['total'], node_count)
    raw_phases = read_json(root / 'measurements.json')
    performance = phases(raw_phases, bool(profile['timing']['warmup_seconds']), node_count)
    for phase in raw_phases.values():
        for rows in phase.values():
            verify_populations(rows, planned)
    require(performance == report.get('performance'), 'performance differs from raw owner samples')
    def observations(stream):
        for line in stream:
            rows = decode_json(line)
            verify_populations(rows, planned)
            yield rows
    with (root / 'node-samples.jsonl').open() as stream:
        periodic(observations(stream), {row['node']: row['source'] for row in performance['measurement']})
    recovery, resolutions = recovery_evidence(root, report)
    require(recovery == report.get('recovery'), 'recovery differs from raw ledger')
    history = reconcile(root / 'operations.jsonl', resolutions=resolutions)
    with (root / 'operations.jsonl').open() as stream:
        verify_traffic((decode_json(line) for line in stream), profile)
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
        require(key not in seen and cell in history['final'] and member in members(cell, node_count), 'invalid or duplicate replica')
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
    require(len(report['cgroups']) == node_count and {row['node'] for row in report['cgroups']} == {f"{report['prefix']}-node{n}" for n in range(1, node_count + 1)}, 'missing physical resource evidence')
    for row in report['cgroups']:
        require(row['cpu_max'] == '100000 100000' and row['memory_max'] == 3 * 1024**3 and row['swap_max'] == 0, 'physical cgroup differs')
    fault = fault_evidence(root, report, summarize_ledger, decode_json)
    require(fault == report['fault_result'], 'fault result differs from raw observations')
    kernel = analyze_kernel(read_json(root / 'kernel-measurements.json'), report)
    require(kernel == report['kernel'], 'kernel result differs from raw observations')
    require(report['memory_objectives_met'] is all(row['memory_objective_met'] for row in kernel['measurement']), 'memory objective differs from kernel evidence')
    return {'verified': True, 'scope': report['scenario'] + ' serialized workload, assigned three-voter replicas and archive coverage', 'placement': planned,
            'fault': fault, 'kernel': kernel, **({'recovery': recovery} if recovery is not None else {}),
            'commands_including_seed': history['commands_including_seed'], 'queries': history['queries'],
            'replicas': len(seen), 'summary': summary, 'performance': performance, 'limitations': report['limitations']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_directory', type=Path)
    print(json.dumps(analyze(parser.parse_args().run_directory), indent=2))
