#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Run one source-pinned RustFS/SIGKILL/empty-process recovery qualification.

Worker processes use real TLS Raft lanes. Test control files and source preparation
are outside the worker data directories. Failed workers and bucket objects remain.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import sys

import recovery
import full_quorum_recovery


def validate(evidence, image_bytes):
    recovery.require(evidence.get('complete') is True, 'native process drill incomplete')
    recovery.require(evidence.get('failure_mode') == 'sigkill' and evidence.get('backend') == 'rustfs'
                     and evidence.get('storage') == 'extents', 'wrong recovery mode')
    recovery.require(evidence.get('image_bytes') == image_bytes, 'wrong physical image size')
    events = evidence.get('events', [])
    expected = ['request_before_loss', 'before_fault', 'fault', 'owner_killed', 'local_state_erased',
                'admitted', 'archive_ready', 'replacement_spawn_started', 'replacement_process_ready',
                'seed_installed', 'first_read', 'promoted', 'request_after_recovery', 'first_write_verified',
                'receipts_and_quorum_reads_verified', 'payloads_verified', 'worker_joined', 'worker_joined',
                'worker_joined', 'cleanup_joined']
    recovery.require([event.get('name') for event in events] == expected, 'incomplete or reordered phases')
    times = [event.get('elapsed_us') for event in events]
    recovery.require(all(type(value) is int and value >= 0 for value in times)
                     and times == sorted(times), 'invalid controller clock')
    by_name = {event['name']: event for event in events}
    detail = lambda name: by_name[name]['detail']
    pids = detail('before_fault')['pids']
    recovery.require(len(pids) == len(set(pids)) == 3 and all(type(pid) is int and pid > 0 for pid in pids), 'workers are not distinct processes')
    killed = detail('owner_killed')['pid']
    recovery.require(killed in pids and detail('owner_killed')['signal'] == 9
                     and detail('fault')['pid'] == killed and detail('fault')['signal'] == 9, 'owner did not exit by SIGKILL')
    replacement = detail('replacement_process_ready')
    recovery.require(type(replacement['pid']) is int and replacement['pid'] > 0
                     and replacement['pid'] not in pids and replacement['initially_empty'] is True
                     and detail('replacement_spawn_started')['absent'] is True, 'replacement was not fresh')
    seed = detail('seed_installed')
    recovery.require(seed.get('snapshot_timeout_ms') == 3600000, 'full-capacity snapshot deadline differs')
    counters = {item['name']: int(item['value']) for item in seed['performance']['counters']}
    recovery.require(seed['pid'] == replacement['pid'] and seed['from_s3'] is True and counters['object_get_body_bytes'] >= image_bytes
                     and counters['recovery_peer_received_bytes'] == 0, 'seed data did not come from object storage')
    recovery.require(detail('first_read')['served_by'] == 4 and detail('first_read')['value'] == 8
                     and detail('first_read')['verified'] is True, 'replacement did not serve restored data')
    recovery.require(detail('promoted')['pid'] == replacement['pid'] and detail('promoted')['member'] == 4,
                     'replacement was not promoted')
    write = detail('first_write_verified')
    recovery.require(write['value'] == 9 and write['durability'] == 'quorum', 'replacement write was not durable')
    recovery.require(detail('receipts_and_quorum_reads_verified') == {'callbacks_before_kill': 1, 'replacement_callbacks': 1}, 'receipt verification incomplete')
    joined = [event['detail'] for event in events if event['name'] == 'worker_joined']
    recovery.require({item['pid'] for item in joined} == set(pids + [replacement['pid']]) - {killed}
                     and all(item['report']['owners_closed'] is True for item in joined), 'child owners did not join')
    recovery.require(detail('cleanup_joined') == {'workers': 4, 'hard_killed': 1, 'gracefully_closed': 3}, 'cleanup incomplete')
    return {
        'fault_to_verified_write_seconds': (by_name['first_write_verified']['elapsed_us'] - by_name['fault']['elapsed_us']) / 1e6,
        'fresh_process_start_through_seed_seconds': (by_name['seed_installed']['elapsed_us'] - by_name['replacement_spawn_started']['elapsed_us']) / 1e6,
        'object_get_body_bytes': counters['object_get_body_bytes'], 'peer_recovery_bytes': 0,
        'snapshot_timeout_ms': seed['snapshot_timeout_ms'],
        'original_worker_pids': pids, 'killed_pid': killed, 'replacement_pid': replacement['pid'],
    }


def run(args):
    image_bytes = 9999998976 if args.profile == '10gb' else 272 * 1024**2
    quota = 10000000000 if args.profile == '10gb' else image_bytes
    args.output.mkdir(mode=0o700, parents=False, exist_ok=False)
    source, inputs = recovery.source_identity()
    binary = recovery.binary_for(args, args.output, source)
    for name in ['CELLAR_LARGE_IMAGE_ENDPOINT', 'CELLAR_LARGE_IMAGE_BUCKET', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']:
        recovery.require(os.environ.get(name), f'missing {name}')
    workers, store = args.work_directory.resolve(), args.store_data_directory.resolve()
    quorum_loss = args.scenario == "quorum-loss"
    required_workers = (6 if quorum_loss else 4) * image_bytes + 8 * 1024**3
    required_store = required_workers if workers.stat().st_dev == store.stat().st_dev else (2 if quorum_loss else 1) * image_bytes + 8 * 1024**3
    admission = {'worker_free_bytes': shutil.disk_usage(workers).free, 'worker_required_bytes': required_workers,
                 'store_free_bytes': shutil.disk_usage(store).free, 'store_required_bytes': required_store}
    report = {'format': 1, 'source': source, 'binary_sha256': recovery.sha256(binary), 'image_bytes': image_bytes,
              'quota': quota, 'scenario': args.scenario, 'admission': admission, 'complete': False, 'production_qualified': False}
    path = args.output / 'result.json'
    def save():
        path.write_text(json.dumps(report, indent=2) + '\n')
    save()
    recovery.require(admission['worker_free_bytes'] >= required_workers and admission['store_free_bytes'] >= required_store, 'insufficient worker or object-store disk headroom')
    env = dict(os.environ)
    env.pop('CELLAR_PROCESS_RECOVERY_CHILD', None)
    env['CELLAR_LARGE_IMAGE_WORKDIR'] = str(workers)
    if args.profile == '10gb':
        recovery.require(args.fixture is not None and args.fixture.is_file() and not args.fixture.is_symlink(), '10 GB immutable fixture required')
        recovery.require(args.fixture.stat().st_size == image_bytes and args.fixture.stat().st_mode & 0o222 == 0, 'fixture size/permissions differ')
        env['CELLAR_LARGE_IMAGE_FIXTURE'] = str(args.fixture.resolve())
        report['fixture_sha256'] = recovery.sha256(args.fixture)
    test = 'process_recovery::rustfs_' + ('decimal_ten_gb' if args.profile == '10gb' else '272_mib') + ('_after_full_quorum_loss' if quorum_loss else '_after_sigkill_restores_empty_process')
    code, timed = recovery.run_process([str(binary), '--exact', test, '--list'], args.output / 'selection.log', timeout=30, env=env)
    recovery.require(code == 0 and not timed and (args.output / 'selection.log').read_text().splitlines().count(test + ': test') == 1, 'exact test absent')
    command = [str(binary), '--exact', test, '--ignored', '--nocapture', '--test-threads=1']
    report['command'] = command
    save()
    code, timed = recovery.run_process(command, args.output / 'native.log', timeout=args.trial_timeout_seconds, env=env)
    log = (args.output / 'native.log').read_text(errors='replace')
    selected, passed = recovery.selected_results(log)
    report.update(exit_code=code, timed_out=timed, selected_tests=selected, passed_tests=passed,
                  source_unchanged=recovery.source_identity() == (source, inputs) and recovery.sha256(binary) == report['binary_sha256'])
    roots = re.findall(r'process-recovery directory=(.+) object_prefix=(\S+)$', log, re.M)
    if len(roots) == 1:
        directory = Path(roots[0][0])
        recovery.require(directory.parent == workers and directory.name.startswith('process-recovery-') and directory.resolve() == directory, 'worker evidence escaped its parent')
        evidence_path = directory / 'evidence.json'
        if evidence_path.exists():
            report.update(evidence=json.loads(evidence_path.read_text()), worker_directory=str(directory), object_prefix=roots[0][1])
    save()
    recovery.require(code == 0 and not timed and selected == passed == 1 and report['source_unchanged'], 'native process qualification failed; inspect retained evidence')
    report['measurements'] = (full_quorum_recovery.validate if quorum_loss else validate)(report['evidence'], image_bytes)
    if args.fixture is not None:
        recovery.require(recovery.sha256(args.fixture) == report['fixture_sha256'], 'golden fixture changed')
    report['complete'] = True
    save()
    print(json.dumps({'complete': True, 'result': str(path), **report['measurements']}, indent=2))
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', choices=['owner-loss', 'quorum-loss'], default='owner-loss')
    parser.add_argument('--profile', choices=['272mib', '10gb'], required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--work-directory', type=Path, required=True)
    parser.add_argument('--store-data-directory', type=Path, required=True)
    parser.add_argument('--fixture', type=Path)
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--binary-manifest', type=Path)
    parser.add_argument('--toolchain', default='1.98.1')
    parser.add_argument('--build-timeout-seconds', type=int, default=1800)
    parser.add_argument('--trial-timeout-seconds', type=int, default=3600)
    args = parser.parse_args()
    with recovery.termination_signals():
        return run(args)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, OSError, recovery.InterruptedRun) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
