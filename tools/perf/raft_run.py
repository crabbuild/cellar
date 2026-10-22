#!/usr/bin/env python3
"""MR-14 isolated local qualification runner; unavailable fault modes fail closed."""
import argparse
import collections
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import threading
import time
import urllib.error
import urllib.request

from raft import captured_node_limits, port_base, wait_for_placement
from raft_workload import Ledger, input_digest, model, run_load, reconcile
from raft_metrics import phases, samples
from run import create_bucket

ROOT = Path(__file__).resolve().parents[2]
RUSTFS = 'ghcr.io/rustfs/rustfs@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858'
LABEL = 'io.cellar.mr14.run'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def decode_json(text):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f'duplicate JSON key: {key}')
            result[key] = value
        return result
    return json.loads(text, object_pairs_hook=unique,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f'invalid number: {value}')))


def read_json(path):
    return decode_json(Path(path).read_text())


def validate_profile(profile):
    required = {'topology', 'cells', 'database_bytes', 'write_bytes', 'read_fraction', 'read_consistency',
                'skew', 'arrival', 'timing', 'durability', 'resources', 'tick_ms', 'seed', 'slo', 'qualification'}
    require(set(profile) == required, 'missing or unknown workload fields')
    def integer(value, minimum, maximum, name):
        require(type(value) is int and minimum <= value <= maximum, f'invalid {name}')
    def number(value, minimum, maximum, name):
        require(type(value) in (int, float) and math.isfinite(value) and minimum <= value <= maximum, f'invalid {name}')
    def fields(value, expected, name):
        require(isinstance(value, dict) and set(value) == set(expected.split()), f'invalid {name} fields')
    fields(profile['topology'], 'nodes voters failure_domains', 'topology')
    for key, value in profile['topology'].items():
        integer(value, 1, 1000, key)
    require(profile['topology']['voters'] <= profile['topology']['nodes'], 'voters exceed nodes')
    fields(profile['cells'], 'total active', 'cells')
    integer(profile['cells']['total'], 1, 1000000, 'total cells')
    integer(profile['cells']['active'], 1, profile['cells']['total'], 'active cells')
    for name, maximum in [('database_bytes', 8 * 1024**2), ('write_bytes', 64 * 1024)]:
        require(isinstance(profile[name], list) and 1 <= len(profile[name]) <= 32, f'invalid {name} distribution')
        for value in profile[name]:
            integer(value, 0, maximum, name)
    number(profile['read_fraction'], 0, 1, 'read fraction')
    require(profile['read_consistency'] in ('stale', 'at-least', 'linearizable'), 'invalid read consistency')
    require(profile['durability'] in ('quorum', 's3'), 'invalid durability')
    fields(profile['skew'], 'hot_fraction hot_probability', 'skew')
    for value in profile['skew'].values():
        number(value, 0, 1, 'skew')
    fields(profile['arrival'], 'rate_per_second max_inflight max_lateness_ms', 'arrival')
    number(profile['arrival']['rate_per_second'], .01, 100000, 'offered rate')
    integer(profile['arrival']['max_inflight'], 1, 256, 'inflight limit')
    integer(profile['arrival']['max_lateness_ms'], 1, 60000, 'lateness')
    fields(profile['timing'], 'warmup_seconds measurement_seconds soak_seconds request_timeout_ms retry_window_ms startup_seconds', 'timing')
    for name, value in profile['timing'].items():
        integer(value, 0 if name in ('warmup_seconds', 'soak_seconds') else 1, 86400000, name)
    require(profile['timing']['request_timeout_ms'] == 5000, 'native workload timeout is 5000ms')
    require(profile['timing']['retry_window_ms'] > profile['timing']['request_timeout_ms'], 'retry deadline must cover request timeout')
    fields(profile['resources'], 'node_cpus node_memory_bytes node_swap_bytes store_cpus store_memory_bytes', 'resources')
    require(profile['resources'] == {'node_cpus': 1, 'node_memory_bytes': 3 * 1024**3, 'node_swap_bytes': 0,
                                    'store_cpus': 2, 'store_memory_bytes': 4 * 1024**3}, 'unsupported local resource profile')
    integer(profile['tick_ms'], 1, 60000, 'tick interval')
    integer(profile['seed'], 0, 2**32-1, 'seed')
    fields(profile['slo'], 'p99_ms max_error_fraction min_completed_per_second max_archive_lag_seconds max_recovery_seconds memory_headroom_fraction', 'SLO')
    for name, value in profile['slo'].items():
        number(value, 0, 1 if name.endswith('_fraction') else 86400000, name)
    return profile


def capacity(profile):
    total = profile['cells']['total'] * profile['topology']['voters']
    per_node = math.ceil(total / profile['topology']['nodes'])
    return {'total_voter_replicas': total, 'average_replicas_per_node': total / profile['topology']['nodes'],
            'minimum_peak_replicas_per_node': per_node, 'host_group_limit': 4096,
            'minimum_peak_with_20_percent_repair_reserve': math.ceil(per_node * 1.2),
            'host_bound_admits_balanced_placement': per_node <= 4096, 'density_qualified': False}


def source_manifest(root=ROOT):
    files = [root / 'Cargo.toml', root / 'Cargo.lock']
    for folder in ('crates', 'vendor', 'examples', 'tools/perf', 'tools/xtask'):
        files.extend(p for p in (root / folder).rglob('*') if p.is_file()
                     and not set(p.relative_to(root).parts) & {'.git', '.cellar', 'node_modules', '__pycache__'}
                     and p.name != '.DS_Store' and p.suffix != '.pyc')
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(files)}


def parse_manifest(text):
    result = {}
    for line in text.splitlines():
        digest, path = line.split('  ', 1)
        require(re.fullmatch('[0-9a-f]{64}', digest) and path not in result, 'invalid image source manifest')
        result[path] = digest
    require(bool(result), 'empty image source manifest')
    return result


def verify_image(image, docker, expected=None):
    require(re.fullmatch(r'(sha256:[0-9a-f]{64}|[^\s]+@sha256:[0-9a-f]{64})', image), 'provide an immutable image digest, not a tag')
    identifier = docker('image', 'inspect', image, '--format', '{{.Id}}')
    actual = parse_manifest(docker('run', '--rm', '--network', 'none', '--entrypoint', 'cat', image, '/usr/local/bin/raft-source.sha256'))
    require(actual == (source_manifest() if expected is None else expected), 'stale image: compiled source manifest differs from this checkout')
    binary = docker('run', '--rm', '--network', 'none', '--entrypoint', 'sha256sum', image, '/usr/local/bin/cellar-perf-raft').split()[0]
    require(re.fullmatch('[0-9a-f]{64}', binary), 'invalid executable digest')
    return {'image_id': identifier, 'binary_sha256': binary,
            'source_manifest_sha256': input_digest(actual), 'build_profile': 'lab'}


def matched_comparison(left, right):
    for key in ('durability_contract', 'hardware', 'workload_contract', 'build_profile'):
        require(key in left and key in right and left[key] == right[key], f'unmatched benchmark {key}')
    require(left.get('reconciled') is True and right.get('reconciled') is True, 'unreconciled benchmark')
    require(left['completed_per_second'] > 0 and right['completed_per_second'] > 0, 'empty benchmark')
    return {'matched_contract': True, 'throughput_ratio': right['completed_per_second'] / left['completed_per_second']}


def request(base, path, token, *, method='GET', body=None, headers=None):
    wire = None if body is None else json.dumps(body, sort_keys=True, separators=(',', ':')).encode()
    req = urllib.request.Request(base + path, data=wire, method=method,
        headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=6) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        raw = error.read().decode()
        try:
            raw = json.loads(raw)
        except ValueError:
            raw = {'error': raw, 'ambiguous': method == 'POST'}
        return error.code, raw


def summarize_ledger(path, phase, seconds):
    invocations, samples, violations = {}, [], []
    seen = set()
    by_operation = {'command': [], 'query': []}
    counts = collections.Counter()
    last_order = 0
    with Path(path).open() as stream:
        for line in stream:
            event = json.loads(line)
            require(event['order'] == last_order + 1, 'ledger event order gap')
            last_order = event['order']
            if event.get('phase') != phase:
                continue
            key = event['id']
            if event['event'] in ('invoke', 'reject'):
                require(key not in seen, 'duplicate offered identity')
                seen.add(key)
            if event['event'] == 'invoke':
                require(input_digest(event['input']) == event['input_sha256'], 'changed invocation payload')
                invocations[key] = event
                counts['submitted'] += 1
            elif event['event'] == 'reject':
                counts['generator_rejected'] += 1
            elif event['event'] == 'complete':
                require(key in invocations, 'completion without invocation')
                invocation = invocations.pop(key)
                require(event['operation'] == invocation['operation'] and event['cell'] == invocation['cell'], 'completion identity mismatch')
                counts[event['outcome']] += 1
                if event.get('timed_out'):
                    counts['timed_out'] += 1
                if event.get('violation'):
                    violations.append(event['violation'])
                if event['outcome'] == 'completed':
                    require(event['status'] == 200, 'success without response')
                    samples.append(event['latency_ms'])
                    by_operation[event['operation']].append(event['latency_ms'])
    require(not invocations, 'unfinished invocation ledger')
    require(not violations, 'reference-model violation')
    counts['offered'] = counts['submitted'] + counts['generator_rejected']
    require(counts['submitted'] == counts['completed'] + counts['rejected'] + counts['ambiguous'] + counts['failed'], 'outcome count mismatch')
    def quantiles(values):
        values.sort()
        return {q: values[min(len(values)-1, math.ceil(len(values)*fraction)-1)] if values else None
                for q, fraction in [('p50', .5), ('p95', .95), ('p99', .99), ('p99_9', .999)]}
    return {'counts': dict(counts), 'latency_ms': quantiles(samples), 'latency_by_operation_ms': {key: quantiles(values) for key, values in by_operation.items()}, 'completed_per_second': counts['completed'] / seconds,
            'offered_per_second': counts['offered'] / seconds,
            'error_fraction': (counts['offered'] - counts['completed']) / counts['offered'] if counts['offered'] else 1}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image')
    parser.add_argument('--profile', type=Path, required=True)
    parser.add_argument('--profile-name', default='local-q0')
    parser.add_argument('--scenario', choices=['healthy', 'leader-loss', 'minority-partition', 'disk-stall', 's3-outage', 'snapshot-crash'], default='healthy')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--context', default='colima-cellar-multi-raft')
    parser.add_argument('--port-base', type=port_base, default=20000)
    parser.add_argument('--capacity-only', action='store_true')
    args = parser.parse_args()
    catalog = read_json(args.profile)
    require(catalog.get('format') == 1, 'unsupported profile format')
    profile = validate_profile(catalog['profiles'][args.profile_name])
    if args.capacity_only:
        print(json.dumps(capacity(profile), indent=2))
        return
    require(args.scenario == 'healthy', 'fault capability not implemented: refuse before creating resources')
    require(profile['topology'] == {'nodes': 3, 'voters': 3, 'failure_domains': 1}, 'local launcher supports three voters in one Colima VM; Q1 needs a separate qualified topology')
    require(capacity(profile)['host_bound_admits_balanced_placement'], 'profile exceeds current host group bound')
    require(sum(profile['timing'][key] for key in ('warmup_seconds', 'measurement_seconds')) * profile['arrival']['rate_per_second'] <= 1000000, 'local ledger analysis bound is one million arrivals')
    require(profile['timing']['soak_seconds'] == 0, 'soak requires fault scheduling and bounded offline history checking')
    require(args.context.startswith('colima-cellar-multi-raft'), 'use the separate Cellar Colima profile')
    require(args.output is not None and not args.output.exists(), 'provide a new output directory')
    require(args.image is not None, 'provide --image')
    def docker(*command):
        # Never print arguments: credentials may appear in container environment.
        result = subprocess.run(['docker', '--context', args.context, *command], text=True, capture_output=True)
        require(result.returncode == 0, f'Docker {command[0]} failed (arguments and environment omitted)')
        return result.stdout.strip()
    build = verify_image(args.image, docker)
    build['source_revision'] = subprocess.check_output(['git', '-C', str(ROOT), 'rev-parse', 'HEAD'], text=True).strip()
    build['working_tree_dirty'] = bool(subprocess.check_output(['git', '-C', str(ROOT), 'status', '--porcelain'], text=True).strip())
    cpus = int(docker('info', '--format', '{{.NCPU}}'))
    memory = int(docker('info', '--format', '{{.MemTotal}}'))
    require(cpus >= 6 and memory >= 14 * 1024**3, 'insufficient dedicated VM resources')
    run(args, profile, build, docker, cpus, memory)


def run(args, profile, build, docker, cpus, memory):
    root = args.output.resolve()
    root.mkdir(parents=True, mode=0o700, exist_ok=False)
    config = root / 'config'
    config.mkdir(mode=0o700)
    run_id = secrets.token_hex(12)
    prefix = f'cellar-mr14-{run_id}'
    owner = f'{LABEL}={run_id}'
    token, access, secret = [secrets.token_hex(32) for _ in range(3)]
    (config / 'token').write_text(token)
    for index in range(1, 4):
        (config / f'process{index}').write_bytes(secrets.token_bytes(16))
    for name in ('ca.der', 'server.der', 'key.der'):
        shutil.copyfile(ROOT / 'crates/cellar/transport/tcp/fixtures' / name, config / name)
    for path in config.iterdir():
        path.chmod(0o600)
    nodes = [f'{prefix}-node{i}' for i in range(1, 4)]
    bases = [f'http://127.0.0.1:{args.port_base+i}' for i in range(1, 4)]
    report = {'format': 1, 'run_id': run_id, 'prefix': prefix, 'context': args.context, 'build': build,
              'profile': profile, 'profile_sha256': input_digest(profile), 'scenario': args.scenario,
              'runner_sha256': {name: hashlib.sha256((ROOT / 'tools/perf' / name).read_bytes()).hexdigest() for name in ('raft_run.py', 'raft_workload.py', 'raft_analyze.py', 'raft_metrics.py', 'raft.py', 'run.py')},
              'capacity': capacity(profile), 'vm': {'cpus': cpus, 'memory_bytes': memory},
              'completed': False, 'reconciled': False,
              'limitations': ['One local VM and RustFS; no independent failure domains or Amazon S3 qualification.',
                              'No fault/recovery or all-disk-loss restore qualification.',
                              'No matched Fleet/bucket baseline measurement yet.',
                              'Engine/SQL API timings and bytes are measured; S3/network bytes, controller timings and whole-device I/O attribution remain open.']}
    def save():
        (root / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    save()
    ledger = Ledger(root / 'operations.jsonl')
    stop = threading.Event()
    sampler = None
    try:
        # Let Docker allocate a private subnet; avoid overlapping a retained run.
        network = json.loads(docker('network', 'inspect', docker('network', 'create', '--label', owner, prefix)))
        subnet = network[0]['IPAM']['Config'][0]['Subnet']
        import ipaddress
        net = ipaddress.ip_network(subnet)
        require(net.version == 4 and net.prefixlen <= 24, 'unsupported Docker subnet')
        network_prefix = '.'.join(str(net.network_address).split('.')[:3])
        report['network'] = subnet
        docker('volume', 'create', '--label', owner, prefix + '-store')
        docker('run', '--rm', '--network', 'none', '--label', owner, '-v', prefix + '-store:/data', 'alpine:3.22', 'chown', '10001:10001', '/data')
        docker('run', '-d', '--name', prefix + '-store', '--label', owner, '--network', prefix, '--network-alias', 'rustfs',
               '--cpus', '2', '--cpuset-cpus', '3,4', '--memory', '4g', '--memory-swap', '4g',
               '-p', f'127.0.0.1:{args.port_base}:9000', '-v', prefix + '-store:/data',
               '-e', 'RUSTFS_ACCESS_KEY=' + access, '-e', 'RUSTFS_SECRET_KEY=' + secret,
               '-e', 'RUSTFS_CONSOLE_ENABLE=false', RUSTFS, '/data')
        deadline = time.monotonic() + 120
        while True:
            try:
                create_bucket(f'http://127.0.0.1:{args.port_base}', 'raft-perf', access, secret)
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise RuntimeError('disposable RustFS startup failed') from None
                time.sleep(1)
        for index, node in enumerate(nodes, 1):
            docker('volume', 'create', '--label', owner, node)
            docker('run', '-d', '--name', node, '--label', owner, '--network', prefix, '--ip', f'{network_prefix}.{10+index}',
                   '--cpus', '1', '--cpuset-cpus', str(index-1), '--memory', '3g', '--memory-swap', '3g',
                   '--ulimit', 'nofile=65536:65536', '-p', f'127.0.0.1:{args.port_base+index}:8082',
                   '-v', node + ':/data', '-v', str(config) + ':/config:ro',
                   '-e', f'RAFT_NODE={index}', '-e', f"RAFT_CELLS={profile['cells']['total']}",
                   '-e', f"RAFT_TICK_MS={profile['tick_ms']}", '-e', 'RAFT_QUEUE_ITEMS=4096',
                   '-e', 'RAFT_WORKLOAD=reference', '-e', f"RAFT_DURABILITY={profile['durability']}",
                   '-e', f'RAFT_NETWORK_PREFIX={network_prefix}',
                   '-e', 'AWS_ACCESS_KEY_ID=' + access, '-e', 'AWS_SECRET_ACCESS_KEY=' + secret, args.image)
        print(f'Created {prefix} in {args.context}; waiting for actual group leadership.', flush=True)
        def balance(cell):
            try:
                code, state = request(bases[cell % 3], f'/admin/{cell}', token)
                if code != 200:
                    return False
                if state['leader'] and state['leader'] != cell % 3 + 1:
                    request(bases[state['leader']-1], f'/admin/{cell}', token, method='POST')
                return state['ready'] and state['leader'] == cell % 3 + 1
            except (OSError, ValueError):
                return False
        wait_for_placement(profile['cells']['total'], balance, time.monotonic() + profile['timing']['startup_seconds'], lambda text: print(text, flush=True))
        report['limits'] = [captured_node_limits(json.loads(docker('inspect', '--format', '{{json .HostConfig}}', node))) for node in nodes]
        for node, limits in zip(nodes, report['limits']):
            require(limits['NanoCpus'] == 10**9 and limits['Memory'] == 3 * 1024**3 and limits['MemorySwap'] == 3 * 1024**3 and not limits['Tmpfs'], 'resource limit mismatch')
            require(docker('exec', node, 'sha256sum', '/proc/1/exe').split()[0] == build['binary_sha256'], 'deployed executable differs from verified image')
        def send(cell, operation, body, headers, member=None):
            node = cell % 3 if operation == 'command' else (cell + 1) % 3
            if member is not None:
                node = member
            extra = {'x-command-id': headers['command_id'], 'x-retry-until-ms': str(headers['retry_until_ms'])} if operation == 'command' else {
                'x-read-consistency': body['consistency'], 'x-min-version': headers.get('min_version', '')}
            return request(bases[node], f'/cells/{cell}', token, method='POST' if operation == 'command' else 'GET', body=body if operation == 'command' else None, headers=extra)
        state = {}
        for cell in range(profile['cells']['total']):
            change = {'kind': 'initialize', 'database_bytes': profile['database_bytes'][cell % len(profile['database_bytes'])]}
            command_id = hashlib.sha256(f'{run_id}:{cell}'.encode()).hexdigest()[:32] + ':1'
            headers = {'command_id': command_id, 'retry_until_ms': int(time.time()*1000) + profile['timing']['retry_window_ms']}
            ledger.append({'event': 'seed_invoke', 'cell': cell, 'input': change, 'input_sha256': input_digest(change), **headers})
            code, reply = send(cell, 'command', change, headers)
            ledger.append({'event': 'seed_complete', 'cell': cell, 'status': code, 'response': reply})
            expected, changed = model(None, change)
            require(code == 200 and reply['state'] == expected and reply['changed'] == changed and reply['durability'] == profile['durability'], 'seed failed; retain original command identity and ledger')
            state[cell] = {'state': expected, 'next_sequence': 2, 'version_token': reply['version_token'], 'index': reply['index']}
        def wait_archive():
            began = time.monotonic()
            while True:
                observations = [request(bases[cell % 3], f'/admin/{cell}', token) for cell in state]
                if all(code == 200 and item['archived'] >= state[cell]['index'] for cell, (code, item) in enumerate(observations)):
                    return observations, time.monotonic() - began
                require(time.monotonic() - began < profile['slo']['max_archive_lag_seconds'], 'archive drain exceeded declared SLO')
                time.sleep(.5)
        report['initial_archive'], report['initial_archive_seconds'] = wait_archive()
        def capture():
            rows = []
            for node, base in enumerate(bases, 1):
                began = time.monotonic_ns()
                code, sample = request(base, '/metrics', token)
                ended = time.monotonic_ns()
                require(code == 200, 'node metrics unavailable')
                rows.append({'node': node, 'started_ns': began, 'finished_ns': ended, 'sample': sample})
            return rows
        measurements = {}
        def boundary(phase, edge):
            measurements.setdefault(phase, {})[edge] = capture()
            (root / 'measurements.json').write_text(json.dumps(measurements) + '\n')
            samples(measurements[phase][edge])

        def sample():
            try:
                with (root / 'resources.jsonl').open('x') as stream, (root / 'node-samples.jsonl').open('x') as internal:
                    while not stop.is_set():
                        stats = docker('stats', '--no-stream', '--format', '{{json .}}', *nodes, prefix + '-store')
                        for row in stats.splitlines():
                            stream.write(json.dumps({'monotonic': time.monotonic(), 'stats': json.loads(row)}) + '\n')
                        stream.flush()
                        observations = capture()
                        internal.write(json.dumps(observations) + '\n')
                        internal.flush()
                        samples(observations)
                        stop.wait(1)
            except Exception as error:
                report['sampler_failure'] = str(error)
        sampler = threading.Thread(target=sample)
        sampler.start()
        for phase, seconds in [('warmup', profile['timing']['warmup_seconds']), ('measurement', profile['timing']['measurement_seconds'])]:
            if seconds:
                print(f'{phase}: {seconds}s, {profile["arrival"]["rate_per_second"]} offered operations/sec', flush=True)
                boundary(phase, 'before')
                report[phase] = run_load(profile, phase, seconds, state, ledger, send, run_id)
                boundary(phase, 'after')
                require(not report[phase]['ambiguous'] and not report[phase]['blocked_cells'], 'ambiguous history requires reconciliation before continuing')
        report['performance'] = phases(measurements, bool(profile['timing']['warmup_seconds']))
        replicas = []
        for cell, expected in state.items():
            for member in range(3):
                code, reply = send(cell, 'query', {'consistency': 'at-least'}, {'min_version': expected['version_token']}, member=member)
                require(code == 200 and reply['state'] == expected['state'] and reply['index'] >= expected['index'] and reply['served_by'] == member+1, 'replica state or version mismatch')
                replicas.append({'cell': cell, 'node': member+1, 'response': reply})
        (root / 'replicas.json').write_text(json.dumps(replicas) + '\n')
        report['final_archive'], report['final_archive_seconds'] = wait_archive()
        report['summary'] = summarize_ledger(root / 'operations.jsonl', 'measurement', profile['timing']['measurement_seconds'])
        require(report['summary']['counts']['offered'] == report['measurement']['offered'], 'offered ledger mismatch')
        report['history'] = reconcile(root / 'operations.jsonl')
        report['cgroups'] = [{'node': node, 'cpu_max': docker('exec', node, 'cat', '/sys/fs/cgroup/cpu.max'),
            'memory_max': int(docker('exec', node, 'cat', '/sys/fs/cgroup/memory.max')),
            'swap_max': int(docker('exec', node, 'cat', '/sys/fs/cgroup/memory.swap.max')),
            'memory_peak': int(docker('exec', node, 'cat', '/sys/fs/cgroup/memory.peak')),
            'cpu_stat': docker('exec', node, 'cat', '/sys/fs/cgroup/cpu.stat'),
            'memory_events': docker('exec', node, 'cat', '/sys/fs/cgroup/memory.events')} for node in nodes]
        require(all(row['cpu_max'] == '100000 100000' and row['memory_max'] == 3 * 1024**3 and row['swap_max'] == 0 for row in report['cgroups']), 'actual cgroup mismatch')
        report['reconciled'] = True
        summary, slo = report['summary'], profile['slo']
        report['traffic_objectives_met'] = bool(summary['latency_ms']['p99'] is not None and summary['latency_ms']['p99'] <= slo['p99_ms'] and summary['error_fraction'] <= slo['max_error_fraction'] and summary['completed_per_second'] >= slo['min_completed_per_second'])
        report['completed'] = True
    except Exception as error:
        report['failure'] = str(error)
        raise
    finally:
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
    require(report['completed'], 'run incomplete; inspect retained report and raw observations')


if __name__ == '__main__':
    main()
