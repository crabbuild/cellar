#!/usr/bin/env python3
"""Run the reference SQL workload through the native Fleet or bucket output gate.

Only a fresh, disposable three-node Colima/RustFS deployment is supported.
Owner-route reads are not follower reads. No Raft proof is fabricated.
"""
import argparse
import base64
import copy
import hashlib
import json
import math
from pathlib import Path
import secrets
import subprocess
import threading
import time

from baseline_history import position, reconcile
from raft import captured_node_limits, port_base
from raft_run import (LABEL, ROOT, RUSTFS, input_digest, read_json, request, require,
                      summarize_ledger, validate_profile, verify_image)
from raft_workload import Ledger, model, run_load
from raft_faults import fault_evidence, preflight_pause
from raft_docker import BoundContainer, discover as discover_fault_engine
from store_faults import StorePause, validate_store_scenario
from workload_recovery import apply_resolutions, recover_commands
from run import create_bucket


def effective_profile(source, mode):
    validate_profile(source)
    require(mode in ('fleet', 'bucket'), 'unsupported baseline mode')
    require(source['topology'] == {'nodes': 3, 'voters': 3, 'failure_domains': 1}, 'baseline supports three local nodes only')
    require(source['cells']['total'] <= 4096, 'baseline cell bound')
    require(source['timing']['soak_seconds'] == 0, 'baseline soak is not implemented')
    require(source['timing']['retry_window_ms'] <= 300000, 'baseline retry window bound')
    # Even if every offered operation targets one cell, no receipt capacity can
    # be crossed. Rejected arrivals still count in this conservative bound.
    offered = sum(math.ceil(source['timing'][key] * source['arrival']['rate_per_second'])
                  for key in ('warmup_seconds', 'measurement_seconds'))
    require(offered + 1 <= 4096, 'baseline retained receipt bound')
    profile = copy.deepcopy(source)
    profile['read_consistency'] = 'owner'
    profile['durability'] = 'fleet-or-bucket' if mode == 'fleet' else 'bucket'
    profile['topology']['voters'] = 0
    return profile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', required=True, choices=['fleet', 'bucket'])
    parser.add_argument('--profile', required=True, type=Path)
    parser.add_argument('--profile-name', default='local-q0')
    parser.add_argument('--scenario', choices=['healthy', 's3-outage', 's3-recovery'], default='healthy')
    parser.add_argument('--fault-profile', type=Path)
    parser.add_argument('--image', required=True)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--context', default='colima-cellar-multi-raft')
    parser.add_argument('--port-base', type=port_base, default=20100)
    args = parser.parse_args()
    catalog = read_json(args.profile)
    require(catalog.get('format') == 1, 'unsupported profile format')
    source = catalog['profiles'][args.profile_name]
    profile = effective_profile(source, args.mode)
    args.fault_config = read_json(args.fault_profile) if args.fault_profile else None
    validate_store_scenario(args.scenario, args.fault_config, profile)
    require(args.context.startswith('colima-cellar-multi-raft'), 'use the separate Cellar Colima profile')
    require(args.port_base <= 65522, 'baseline reserves ports through base+13')
    require(not args.output.exists(), 'provide a new output directory')
    def docker(*command):
        result = subprocess.run(['docker', '--context', args.context, *command], text=True, capture_output=True,
                                timeout=15 if command[0] in ('pause', 'unpause', 'inspect', 'exec') else None)
        require(result.returncode == 0, f'Docker {command[0]} failed (arguments and environment omitted)')
        return (result.stdout + (result.stderr if command[0] == 'logs' else '')).strip()
    build = verify_image(args.image, docker, binary_name='cellar-perf-baseline')
    build['source_revision'] = subprocess.check_output(['git', '-C', str(ROOT), 'rev-parse', 'HEAD'], text=True).strip()
    build['working_tree_dirty'] = bool(subprocess.check_output(['git', '-C', str(ROOT), 'status', '--porcelain'], text=True).strip())
    cpus = int(docker('info', '--format', '{{.NCPU}}'))
    memory = int(docker('info', '--format', '{{.MemTotal}}'))
    require(cpus >= 6 and memory >= 14 * 1024**3, 'insufficient dedicated VM resources')
    # Refuse to add three more Cellar nodes to an active experiment in this VM.
    require(not docker('ps', '-q', '--filter', f'label={LABEL}'), 'stop the previous owned experiment before starting another')
    args.fault_engine = discover_fault_engine(docker, args.context) if args.fault_config is not None else None
    args.fault_capability = preflight_pause(docker, args.image, lambda node, owner: BoundContainer(**args.fault_engine, node=node, owner=owner)) if args.fault_config is not None else None
    run(args, source, profile, build, docker, cpus, memory)


def run(args, source, profile, build, docker, cpus, memory):
    root = args.output.resolve()
    root.mkdir(parents=True, mode=0o700, exist_ok=False)
    config = root / 'config'
    config.mkdir(mode=0o700)
    run_id = secrets.token_hex(12)
    prefix = f'cellar-mr14-{run_id}'
    owner = f'{LABEL}={run_id}'
    token, access, secret = [secrets.token_hex(32) for _ in range(3)]
    peer_key = base64.b64encode(secrets.token_bytes(32)).decode()
    for name, value in [('token', token), ('access', access), ('secret', secret), ('peer-key', peer_key)]:
        path = config / name
        path.write_text(value)
        path.chmod(0o600)
    nodes = [f'{prefix}-node{i}' for i in range(1, 4)]
    bases = [f'http://127.0.0.1:{args.port_base+i}' for i in range(1, 4)]
    internal = [f'http://127.0.0.1:{args.port_base+10+i}' for i in range(1, 4)]
    report = {'format': 1, 'adapter': args.mode, 'run_id': run_id, 'prefix': prefix, 'context': args.context,
              'build': build, 'source_profile': source, 'profile': profile, 'profile_sha256': input_digest(profile),
              'vm': {'cpus': cpus, 'memory_bytes': memory}, 'scenario': args.scenario,
              'fault_profile': args.fault_config, 'fault_profile_sha256': input_digest(args.fault_config),
              'fault_engine': args.fault_engine, 'fault_capability': args.fault_capability,
              'runner_sha256': {name: hashlib.sha256((ROOT / 'tools/perf' / name).read_bytes()).hexdigest() for name in ('baseline_run.py', 'baseline_analyze.py', 'baseline_history.py', 'raft_workload.py', 'raft_faults.py', 'store_faults.py', 'workload_recovery.py', 'raft_docker.py', 'raft_run.py')},
              'completed': False, 'reconciled': False,
              'limitations': ['One local VM and RustFS, without independent failure domains or Amazon S3 validation.',
                              'Fleet races all current follower fsyncs against bucket publication; the response names that policy, not a per-write proof source.',
                              'Application transaction receipts, owner reads and native ownership differ from Raft receipts, follower reads and quorum authority.',
                              'Only the recorded scenario is covered; no crash, packet-loss, disk-loss recovery, steady-state, instrumentation overhead or capacity qualification.']}
    def save():
        (root / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    def capture():
        rows = []
        for node, base in enumerate(internal, 1):
            code, sample = request(base, '/state', token)
            require(code == 200 and isinstance(sample.get('output_proofs'), dict), 'native output proof observations unavailable')
            rows.append({'node': node, 'sample': sample})
        return rows
    save()
    ledger = Ledger(root / 'operations.jsonl')
    stop = threading.Event()
    sampler = None
    fault = None
    try:
        docker('network', 'create', '--label', owner, prefix)
        docker('volume', 'create', '--label', owner, prefix + '-store')
        docker('run', '--rm', '--network', 'none', '--label', owner, '-v', prefix + '-store:/data', 'alpine:3.22', 'chown', '10001:10001', '/data')
        docker('run', '-d', '--name', prefix + '-store', '--label', owner, '--network', prefix, '--network-alias', 'rustfs',
               '--cpus', '2', '--cpuset-cpus', '3,4', '--memory', '4g', '--memory-swap', '4g',
               '-p', f'127.0.0.1:{args.port_base}:9000', '-v', prefix + '-store:/data',
               '-e', 'RUSTFS_ACCESS_KEY=' + access, '-e', 'RUSTFS_SECRET_KEY=' + secret,
               '-e', 'RUSTFS_CONSOLE_ENABLE=false', RUSTFS, '/data')
        report['object_store'] = {'image_reference': RUSTFS, 'image_id': docker('inspect', '--format', '{{.Image}}', prefix + '-store')}
        deadline = time.monotonic() + 120
        while True:
            try:
                create_bucket(f'http://127.0.0.1:{args.port_base}', 'baseline-perf', access, secret)
                break
            except Exception:
                require(time.monotonic() < deadline, 'disposable RustFS startup failed')
                time.sleep(1)
        for index, node in enumerate(nodes, 1):
            docker('volume', 'create', '--label', owner, node)
            docker('run', '-d', '--name', node, '--label', owner, '--network', prefix, '--network-alias', f'node{index}',
                   '--cpus', '1', '--cpuset-cpus', str(index-1), '--memory', '3g', '--memory-swap', '3g',
                   '--ulimit', 'nofile=65536:65536', '-p', f'127.0.0.1:{args.port_base+index}:8080',
                   '-p', f'127.0.0.1:{args.port_base+10+index}:8081', '-v', node + ':/data',
                   '-e', f'CELLAR_PERF_NODE=node{index}', '-e', 'CELLAR_PERF_DATA_DIR=/data/state',
                   '-e', 'CELLAR_PERF_THREADS=1', '-e', 'CELLAR_PERF_MAX_RESIDENT=4096',
                   '-e', 'CELLAR_PERF_MAX_CACHED_ROUTES=8192', '-e', 'CELLAR_PERF_MAX_REQUESTS=512',
                   '-e', 'CELLAR_PERF_PEER_KEY=' + peer_key, '-e', 'CELLAR_PERF_DURABILITY=' + args.mode,
                   '-e', 'CELLAR_PERF_TOKEN=' + token, '-e', 'CELLAR_PERF_RUN_ID=' + run_id,
                   '-e', 'AWS_ACCESS_KEY_ID=' + access, '-e', 'AWS_SECRET_ACCESS_KEY=' + secret,
                   '--entrypoint', '/usr/local/bin/cellar-perf-baseline', args.image,
                   '--bucket', 'baseline-perf', '--endpoint', 'http://rustfs:9000', '--region', 'us-east-1',
                   '--listen', '0.0.0.0:8080', '--internal-listen', '0.0.0.0:8081', '--advertise', f'node{index}:8081')
        deadline = time.monotonic() + profile['timing']['startup_seconds']
        for base in bases:
            while True:
                try:
                    code, _ = request(base, '/.well-known/cellar/health', token)
                    if code == 200:
                        break
                except (OSError, ValueError):
                    pass
                require(time.monotonic() < deadline, 'native node startup timed out')
                time.sleep(.5)
        report['limits'] = [captured_node_limits(json.loads(docker('inspect', '--format', '{{json .HostConfig}}', node))) for node in nodes]
        for node, limits in zip(nodes, report['limits']):
            require(limits['NanoCpus'] == 10**9 and limits['Memory'] == 3 * 1024**3 and limits['MemorySwap'] == 3 * 1024**3 and not limits['Tmpfs'], 'resource limit mismatch')
            require(docker('exec', node, 'sha256sum', '/proc/1/exe').split()[0] == build['binary_sha256'], 'deployed executable differs from verified image')
        def send(cell, operation, body, headers, member=None):
            node = cell % 3 if operation == 'command' else (cell + 1) % 3
            if member is not None:
                node = member
            extra = {'x-command-id': headers['command_id'], 'x-retry-until-ms': str(headers['retry_until_ms'])} if operation == 'command' else {'x-read-consistency': 'owner'}
            code, reply = request(bases[node], f'/cells/{cell}', token, method='POST' if operation == 'command' else 'GET', body=body if operation == 'command' else None, headers=extra)
            if code != 200 and operation == 'command':
                reply['ambiguous'] = True  # An HTTP error alone cannot prove non-commit.
            return code, reply
        state = {}
        for cell in range(profile['cells']['total']):
            change = {'kind': 'initialize', 'database_bytes': profile['database_bytes'][cell % len(profile['database_bytes'])]}
            headers = {'command_id': hashlib.sha256(f'{run_id}:{cell}'.encode()).hexdigest()[:32] + ':1',
                       'retry_until_ms': int(time.time()*1000) + profile['timing']['retry_window_ms']}
            ledger.append({'event': 'seed_invoke', 'cell': cell, 'input': change, 'input_sha256': input_digest(change), **headers})
            code, reply = send(cell, 'command', change, headers)
            ledger.append({'event': 'seed_complete', 'cell': cell, 'status': code, 'response': reply})
            expected, changed = model(None, change)
            require(code == 200 and reply['state'] == expected and reply['changed'] == changed and reply['durability'] == profile['durability'], 'seed failed; retain its original identity and ledger')
            state[cell] = {'state': expected, 'next_sequence': 2, **position(reply)}
        def sample():
            try:
                with (root / 'resources.jsonl').open('x') as stream:
                    while not stop.is_set():
                        stats = docker('stats', '--no-stream', '--format', '{{json .}}', *nodes, prefix + '-store')
                        for row in stats.splitlines():
                            stream.write(json.dumps({'monotonic': time.monotonic(), 'stats': json.loads(row)}) + '\n')
                        stream.flush()
                        stop.wait(1)
            except Exception as error:
                report['sampler_failure'] = str(error)
        sampler = threading.Thread(target=sample)
        sampler.start()
        if args.fault_config is not None:
            control = BoundContainer(**args.fault_engine, node=prefix + '-store', owner=run_id)
            fault = StorePause(root, args.scenario, args.fault_config, profile, prefix, run_id, control)
        report['proof_samples'] = {}
        for phase, seconds in [('warmup', profile['timing']['warmup_seconds']), ('measurement', profile['timing']['measurement_seconds'])]:
            if seconds:
                print(f'{args.mode} {phase}: {seconds}s at {profile["arrival"]["rate_per_second"]} offered operations/sec', flush=True)
                report['proof_samples'][phase] = {'before': capture()}
                report[phase] = run_load(profile, phase, seconds, state, ledger, send, run_id, position=position,
                                        on_start=fault.start if fault and phase == 'measurement' else None)
                if fault and phase == 'measurement':
                    fault.finish()
                report['proof_samples'][phase]['after'] = capture()
                if not (args.scenario == 's3-recovery' and phase == 'measurement'):
                    require(not report[phase]['ambiguous'] and not report[phase]['blocked_cells'], 'ambiguous history requires recovery before continuing')
        resolutions = []
        if args.scenario == 's3-recovery':
            report['proof_samples']['recovery'] = {'before': capture()}
            report['recovery'], resolutions = recover_commands(root, report, send)
            report['proof_samples']['recovery']['after'] = capture()
            reconcile(root / 'operations.jsonl', run_id, profile['durability'], resolutions=resolutions)
            apply_resolutions(state, resolutions, position)
        reads = []
        for cell, expected in state.items():
            for member in range(3):
                code, reply = send(cell, 'query', None, {}, member=member)
                require(code == 200 and reply['state'] == expected['state'] and reply['sequence'] == expected['sequence'], 'owner-route reconciliation failed')
                reads.append({'cell': cell, 'ingress': member+1, 'response': reply})
        (root / 'owner-reads.json').write_text(json.dumps(reads) + '\n')
        report['store_recovery_verified_monotonic_seconds'] = time.monotonic()
        report['summary'] = summarize_ledger(root / 'operations.jsonl', 'measurement', profile['timing']['measurement_seconds'])
        report['history'] = reconcile(root / 'operations.jsonl', run_id, profile['durability'], resolutions=resolutions)
        report['fault_result'] = fault_evidence(root, report, summarize_ledger)
        report['cgroups'] = [{'node': node, 'cpu_max': docker('exec', node, 'cat', '/sys/fs/cgroup/cpu.max'),
            'memory_max': int(docker('exec', node, 'cat', '/sys/fs/cgroup/memory.max')),
            'swap_max': int(docker('exec', node, 'cat', '/sys/fs/cgroup/memory.swap.max')),
            'memory_peak': int(docker('exec', node, 'cat', '/sys/fs/cgroup/memory.peak')),
            'cpu_stat': docker('exec', node, 'cat', '/sys/fs/cgroup/cpu.stat'),
            'memory_events': docker('exec', node, 'cat', '/sys/fs/cgroup/memory.events')} for node in nodes]
        require(all(row['cpu_max'] == '100000 100000' and row['memory_max'] == 3 * 1024**3 and row['swap_max'] == 0 for row in report['cgroups']), 'actual cgroup mismatch')
        summary, slo = report['summary'], profile['slo']
        report['traffic_objectives_met'] = bool(summary['latency_ms']['p99'] is not None and summary['latency_ms']['p99'] <= slo['p99_ms'] and summary['error_fraction'] <= slo['max_error_fraction'] and summary['completed_per_second'] >= slo['min_completed_per_second'])
        report.update(completed=True, reconciled=True)
    except Exception as error:
        report['failure'] = str(error)
        raise
    finally:
        if fault:
            fault.close()
        stop.set()
        if sampler:
            sampler.join()
        ledger.close()
        if report.get('sampler_failure'):
            report['completed'] = False
        for node in nodes:
            try:
                (root / f'{node}.log').write_text(docker('logs', node))
            except ValueError:
                pass
        save()
        print(f'Evidence retained at {root}; containers and volumes retained under {prefix}.', flush=True)
    require(report['completed'], 'run incomplete; inspect the retained observations')
    from baseline_analyze import analyze
    try:
        report['verification'] = analyze(root)
    except Exception as error:
        report['completed'] = False
        report['failure'] = str(error)
        save()
        raise
    save()


if __name__ == '__main__':
    main()
