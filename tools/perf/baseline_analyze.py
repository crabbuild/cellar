#!/usr/bin/env python3
"""Verify a Fleet/bucket reference run from its raw ledger and owner-route reads."""
import argparse
import math
from pathlib import Path
import json
from baseline_history import reconcile
from baseline_restart import verify_restart
from baseline_resume import continuation_events, verify_audit
from baseline_run import effective_profile
from raft_run import decode_json, input_digest, read_json, require, summarize_ledger
from raft_faults import fault_evidence
from store_faults import validate_store_scenario
from workload_recovery import recovery_evidence


def proof_delta(samples, mode, *, allow_failed_proofs=False):
    require(set(samples) == {'before', 'after'}, 'missing proof boundary')
    result = {key: 0 for key in ('started', 'bucket', 'fleet', 'errors', 'elapsed_us')}
    for edge in samples.values():
        require(len(edge) == 3 and {row['node'] for row in edge} == {1, 2, 3}, 'missing native node sample')
    before = {row['node']: row['sample']['output_proofs'] for row in samples['before']}
    for row in samples['after']:
        for key in result:
            left, right = before[row['node']][key], row['sample']['output_proofs'][key]
            require(type(left) is int and type(right) is int and 0 <= left <= right, 'invalid or reset proof counter')
            result[key] += right - left
    require(allow_failed_proofs or result['errors'] == 0, 'native output proof failed during measurement')
    if mode == 'bucket':
        require(result['fleet'] == 0, 'bucket baseline unexpectedly used Fleet proof')
    return result


def analyze(root):
    root = Path(root)
    report = read_json(root / 'report.json')
    require(report.get('completed') is True and report.get('reconciled') is True, 'unfinished baseline run')
    profile = effective_profile(report['source_profile'], report['adapter'])
    validate_store_scenario(report['scenario'], report['fault_profile'], profile)
    require(profile == report['profile'] and input_digest(profile) == report['profile_sha256'], 'altered baseline profile')
    recovery, resolutions = recovery_evidence(root, report)
    require(recovery == report.get('recovery'), 'recovery differs from raw ledger')
    restart = verify_restart(root, report)
    continuation = continuation_events(root, report, resolutions)
    audit = verify_audit(root, report) if restart is not None else None
    require(restart == report.get('restart') and audit == report.get('before_recovery'), 'restart or pre-retry audit differs from raw observations')
    history = reconcile(root / 'operations.jsonl', report['run_id'], profile['durability'], resolutions=resolutions, continuation=continuation)
    require(history['cells'] == profile['cells']['total'], 'missing seeded cells')
    summary = summarize_ledger(root / 'operations.jsonl', 'measurement', profile['timing']['measurement_seconds'])
    require(summary == report['summary'], 'summary differs from raw ledger')
    require(summary['counts']['offered'] == math.ceil(profile['timing']['measurement_seconds'] * profile['arrival']['rate_per_second']) == report['measurement']['offered'], 'incomplete offered schedule')
    seen = set()
    for row in read_json(root / 'owner-reads.json'):
        cell, ingress, reply = row['cell'], row['ingress'], row['response']
        require(cell in history['final'] and ingress in (1, 2, 3) and (cell, ingress) not in seen, 'invalid or duplicate owner-route read')
        seen.add((cell, ingress))
        current = history['final'][cell]
        require(reply['scope'] == current['scope'] and reply['sequence'] == current['sequence'] and reply['state'] == current['state'] and reply['read_contract'] == 'owner', 'owner read does not match acknowledged history')
        require(isinstance(reply['owner'], str) and bool(reply['owner']), 'owner identity missing')
    require(len(seen) == 3 * profile['cells']['total'], 'missing owner-route read')
    require(len(report['cgroups']) == 3 and len({row['node'] for row in report['cgroups']}) == 3, 'missing physical resource evidence')
    for row in report['cgroups']:
        require(row['cpu_max'] == '100000 100000' and row['memory_max'] == 3 * 1024**3 and row['swap_max'] == 0, 'physical cgroup differs')
    if restart is not None:
        require(report['proof_samples']['measurement']['after'] is None
                and report.get('measurement_proofs_complete') is False
                and report['traffic_objectives_met'] is False, 'missing old-process counters were presented as a benchmark')
        # Validate the retained starting samples without inventing final values.
        before = report['proof_samples']['measurement']['before']
        proof_delta({'before': before, 'after': before}, report['adapter'])
        proofs = None
        resumed_proofs = proof_delta(report['proof_samples']['resumed'], report['adapter'])
        require(resumed_proofs['bucket'] >= profile['cells']['total'], 'resumed writes did not exercise bucket proofs')
    else:
        proofs = proof_delta(report['proof_samples']['measurement'], report['adapter'], allow_failed_proofs=recovery is not None)
        resumed_proofs = None
    recovery_proofs = proof_delta(report['proof_samples']['recovery'], report['adapter']) if recovery is not None else None
    # No per-operation source is inferred from aggregate counters. Require the
    # configured Fleet fast path to have actually participated in this run.
    if report['adapter'] == 'fleet':
        require(proofs['fleet'] > 0, 'Fleet baseline never exercised a Fleet output proof')
    fault = fault_evidence(root, report, summarize_ledger, decode_json)
    require(fault == report['fault_result'], 'store fault result differs from raw observations')
    return {'verified': True, 'scope': report['scenario'] + ' serialized workload through native output gates and owner routes',
            'measurement_proofs_complete': restart is None,
            **({'restart': restart, 'before_recovery': audit, 'resumed_proof_delta': resumed_proofs} if restart is not None else {}),
            'fault': fault, **({'recovery': recovery, 'recovery_proof_delta': recovery_proofs} if recovery is not None else {}),
            'commands_including_seed': history['commands_including_seed'], 'queries': history['queries'],
            'owner_route_reads': len(seen), 'summary': summary, 'proof_delta': proofs, 'limitations': report['limitations']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_directory', type=Path)
    print(json.dumps(analyze(parser.parse_args().run_directory), indent=2))
