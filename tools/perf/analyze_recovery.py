#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Analyze exact cold-recovery trials; missing/failed samples remain target misses.

Controller-clock intervals are separate from owner-clock operation histograms.
A pilot can meet its observed target but never qualifies a recovery guarantee.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import sys

PHASES = (
    'fault', 'owner_joined', 'local_state_erased', 'admission_started', 'admitted',
    'archive_wait_started', 'archive_ready', 'seed_started', 'seed_installed', 'hydrated',
    'first_read', 'promoted', 'first_write_verified', 'receipt_verified',
    'payloads_verified', 'cleanup_joined',
)
CHECKS = (
    'payloads_verified', 'version_verified', 'first_write_verified', 'receipt_verified',
    'local_state_erased', 'replacement_started_cold', 'served_locally', 'callbacks_once', 'cleanup_joined',
)
FAULT_CASES = frozenset((
    'process_loss', 'corruption', 'stale_authority', 'disk_full',
    'duplicate_commands', 'concurrent_recovery',
))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, minimum=0):
    return type(value) is int and minimum <= value < 2**64


def digest(value, length=64):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{' + str(length) + '}', value) is not None


def trial_intervals(trial, campaign):
    require(type(trial) is dict, 'invalid trial')
    require(type(trial.get('returncode')) is int and trial['returncode'] == 0, 'trial process failed')
    require(trial.get('timed_out', False) is False, 'trial exceeded its controller deadline')
    require(trial.get('selected_tests') == 1 and type(trial.get('selected_tests')) is int,
            'trial must select exactly one test')
    require(trial.get('passed_tests') == 1 and type(trial.get('passed_tests')) is int,
            'trial did not pass its selected test')
    require(trial.get('source_unchanged') is True, 'source changed during trial')
    evidence = trial.get('evidence')
    require(type(evidence) is dict and type(evidence.get('format')) is int and evidence['format'] == 1, 'missing trial evidence')
    require(evidence.get('complete') is True, 'incomplete trial')
    for field in ('backend', 'storage', 'image_bytes'):
        require(evidence.get(field) == campaign[field], f'trial {field} differs')
    require(evidence.get('failure_mode') in ('joined_domain_loss', 'process_kill'), 'unknown failure mode')
    require(evidence['failure_mode'] == campaign['profile']['failure_mode'], 'trial failure mode differs from profile')
    checks = evidence.get('checks')
    require(type(checks) is dict and set(checks) == set(CHECKS), 'missing correctness checks')
    require(all(value is True for value in checks.values()), 'trial correctness check failed')
    events = evidence.get('events')
    require(type(events) is list and len(events) == len(PHASES), 'incomplete phase ledger')
    stamps = {}
    previous = 0
    for name, event in zip(PHASES, events):
        require(type(event) is dict and set(event) == {'name', 'elapsed_us'}, 'invalid phase event')
        require(event['name'] == name, 'phase sequence differs')
        stamp = event['elapsed_us']
        require(integer(stamp) and stamp >= previous, 'invalid or regressed phase clock')
        stamps[name] = stamp
        previous = stamp
    require(stamps['seed_installed'] <= stamps['first_read'] <= stamps['first_write_verified'],
            'first use precedes durable installation')
    require(stamps['hydrated'] <= stamps['first_read'], 'materialized first use precedes hydration')
    return {
        'cold_failure_to_ready_seconds': (stamps['first_write_verified'] - stamps['fault']) / 1_000_000,
        'cold_ready_seconds': (stamps['first_write_verified'] - stamps['admission_started']) / 1_000_000,
        'archive_wait_seconds': (stamps['archive_ready'] - stamps['archive_wait_started']) / 1_000_000,
        'full_hydration_seconds': (stamps['hydrated'] - stamps['admission_started']) / 1_000_000,
        'seed_seconds': (stamps['seed_installed'] - stamps['seed_started']) / 1_000_000,
        'promotion_seconds': (stamps['promoted'] - stamps['first_read']) / 1_000_000,
        'first_write_seconds': (stamps['first_write_verified'] - stamps['promoted']) / 1_000_000,
    }


def quantiles(samples):
    # A failed/unobserved completion is +infinity, rendered as null, not removed.
    ordered = sorted(samples, key=lambda value: math.inf if value is None else value)
    return {name: ordered[math.ceil(len(ordered) * fraction) - 1] if ordered else None
            for name, fraction in (('p50', .50), ('p95', .95), ('p99', .99))}


def analyze(raw, *, mode='pilot', target_seconds=120, minimum_samples=3):
    require(mode in ('pilot', 'qualify'), 'unknown analysis mode')
    require(type(target_seconds) in (int, float) and math.isfinite(target_seconds) and target_seconds > 0,
            'invalid recovery target')
    require(integer(minimum_samples, 1), 'invalid minimum sample count')
    require(type(raw) is dict and type(raw.get('format')) is int and raw['format'] == 1, 'invalid campaign format')
    require(raw.get('backend') in ('peer', 'rustfs') and raw.get('storage') == 'materialized',
            'unsupported recovery path')
    require(integer(raw.get('image_bytes'), 4096) and raw['image_bytes'] % 4096 == 0, 'invalid image size')
    count = raw.get('requested_repetitions')
    require(integer(count, 1), 'invalid requested repetitions')
    require(type(raw.get('trials')) is list and len(raw['trials']) == count, 'missing trials')
    source = raw.get('source')
    require(type(source) is dict and digest(source.get('commit'), 40)
            and digest(source.get('inputs_sha256')) and digest(source.get('binary_sha256'))
            and source.get('build_profile') == 'release',
            'invalid source or binary identity')
    profile = raw.get('profile')
    require(type(profile) is dict and type(profile.get('backends')) is list
            and profile.get('image_bytes') == raw['image_bytes']
            and raw['backend'] in profile.get('backends', [])
            and profile.get('failure_mode') in ('joined_domain_loss', 'process_kill'), 'invalid profile')
    expected_profile = hashlib.sha256(json.dumps(profile, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    require(digest(raw.get('profile_sha256')) and raw['profile_sha256'] == expected_profile, 'profile identity differs')
    intervals, failures, durations = [], [], []
    for number, trial in enumerate(raw['trials'], 1):
        try:
            timing = trial_intervals(trial, raw)
            intervals.append(timing)
            durations.append(timing['cold_failure_to_ready_seconds'])
        except ValueError as error:
            failures.append({'trial': number, 'reason': str(error)})
            durations.append(None)
    latency = quantiles(durations)
    target_met = not failures and count >= minimum_samples and latency['p95'] is not None and latency['p95'] <= target_seconds
    blockers = []
    if count < max(100, minimum_samples):
        blockers.append('at least 100 complete trials are required')
    if raw['image_bytes'] != 9_999_998_976:
        blockers.append('not the page-rounded decimal 10 GB fixture')
    if raw.get('reference_profile_verified') is not True:
        blockers.append('reference hardware/workload profile is unverified')
    if any(not isinstance(t.get('evidence'), dict) or t['evidence'].get('failure_mode') != 'process_kill'
           for t in raw['trials']):
        blockers.append('joined domain shutdown does not qualify hard process loss')
    faults = raw.get('fault_suite', {})
    if (not isinstance(faults, dict) or faults.get('complete') is not True
            or faults.get('source_inputs_sha256') != source['inputs_sha256']
            or not isinstance(faults.get('cases'), list)
            or not all(isinstance(case, str) for case in faults['cases']) or set(faults['cases']) != FAULT_CASES):
        blockers.append('matching hard-failure and contention suite is incomplete')
    if not target_met:
        blockers.append('insufficient, failed or over-target recovery observations')
    report = {
        'format': 1, 'mode': mode, 'backend': raw['backend'], 'storage': raw['storage'],
        'image_bytes': raw['image_bytes'], 'source': source, 'profile_sha256': raw['profile_sha256'],
        'requested_trials': count, 'successful_trials': len(intervals), 'failed_trials': len(failures),
        'target_seconds': target_seconds,
        'target_misses': sum(value is None or value > target_seconds for value in durations),
        'target_met': target_met, 'qualified': mode == 'qualify' and not blockers,
        'qualification_blockers': blockers, 'failures': failures,
        'cold_failure_to_ready_seconds': latency,
        'observed_successful_intervals': intervals,
    }
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--mode', choices=('pilot', 'qualify'), default='pilot')
    parser.add_argument('--target-seconds', type=float, default=120)
    parser.add_argument('--minimum-samples', type=int, default=3)
    args = parser.parse_args()
    try:
        raw = json.loads((args.directory / 'campaign.json').read_text())
        report = analyze(raw, mode=args.mode, target_seconds=args.target_seconds, minimum_samples=args.minimum_samples)
        (args.directory / 'analysis.json').write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
        print(json.dumps(report, indent=2, allow_nan=False))
        accepted = report['qualified'] if args.mode == 'qualify' else report['target_met']
        return 0 if accepted else 1
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f'ERROR: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
