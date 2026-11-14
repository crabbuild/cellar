#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Owned Colima comparison: three 1-vCPU/2-GiB nodes, RustFS and a capped client."""
import argparse
import base64
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

from primitive_load import latency_summary
from raft_run import ROOT, RUSTFS, input_digest, parse_manifest, require, source_manifest
sys.path.insert(0, str(ROOT / 'tools/multi-raft'))
from store_probe import probe

LABEL = 'io.cellar.primitive-comparison.run'
GIB = 1024**3
FAMILIES = ['sql', 'kv', 'queue']
MODES = ['raft-quorum', 'raft-s3', 'fleet', 'bucket']
INSPECT = '''{"id":{{json .Id}},"image":{{json .Image}},"owner":{{json (index .Config.Labels "io.cellar.primitive-comparison.run")}},"cpus":{{.HostConfig.NanoCpus}},"memory":{{.HostConfig.Memory}},"swap":{{.HostConfig.MemorySwap}},"cpuset":{{json .HostConfig.CpusetCpus}},"state":{{json .State}},"restarts":{{.RestartCount}},"ports":{{json .NetworkSettings.Ports}}}'''
CGROUP = 'for f in cpu.max memory.max memory.swap.max cpu.stat memory.current memory.peak memory.events memory.stat; do echo "$f"; cat "/sys/fs/cgroup/$f"; done'


def save(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def parse_cgroup(value):
    result, name = {}, None
    for line in value.splitlines():
        if '.' in line and ' ' not in line:
            name = line
            result[name] = {}
        elif name in ('cpu.max', 'memory.max', 'memory.swap.max', 'memory.current', 'memory.peak'):
            result[name] = line
        else:
            key, number = line.split()
            result[name][key] = int(number)
    quota, period = result['cpu.max'].split()
    require(quota != 'max' and int(quota) == int(period), 'cgroup CPU ceiling differs from one core')
    require(int(result['memory.max']) == 2 * GIB and int(result['memory.swap.max']) == 0,
            'cgroup memory/swap ceiling differs from 2 GiB/no swap')
    return result


def validate_rows(rows, family, args):
    expected = [(repeat, concurrency, phase)
                for repeat in range(args.repetitions)
                for concurrency in (args.concurrency if repeat % 2 == 0 else list(reversed(args.concurrency)))
                for phase in (['warmup', 'enqueue'] if family == 'queue' else ['warmup', 'write', 'read'])]
    require([(r['repeat'], r['concurrency'], r['phase']) for r in rows] == expected, 'incomplete/reordered workload matrix')
    for row in rows:
        duration = args.warmup if row['phase'] == 'warmup' else args.seconds
        require(row['family'] == family and row['verified'] and not row['errors'], 'failed/unverified window')
        require(row['requested_seconds'] == duration and row['elapsed_seconds'] >= duration, 'short measurement window')
        require(row['successful_requests'] == row['attempts'] == len(row['attempt_latency_us']) == len(row['success_latency_us']) > 0,
                'request accounting mismatch')
        require(row['attempt_latency'] == latency_summary(row['attempt_latency_us'])
                and row['success_latency'] == latency_summary(row['success_latency_us']), 'latencies differ from raw samples')
        require(math.isclose(row['requests_per_second'], row['successful_requests'] / row['elapsed_seconds'], rel_tol=1e-10), 'rate accounting mismatch')
        if family == 'queue':
            require(row['acknowledged_messages'] == row['successful_requests'] * 8 == row['delivered_messages']
                    and not row['missing_messages'] and not row['extra_messages']
                    and all(m['backlog'] == m['dead'] == 0 for m in row['queue_metrics']), 'Queue delivery/settlement accounting mismatch')
            require(row['enqueue_and_drain_seconds'] >= row['elapsed_seconds']
                    and math.isclose(row['enqueue_messages_per_second'], row['acknowledged_messages'] / row['elapsed_seconds'], rel_tol=1e-10)
                    and math.isclose(row['verified_messages_per_second_including_drain'], row['acknowledged_messages'] / row['enqueue_and_drain_seconds'], rel_tol=1e-10), 'Queue rate accounting mismatch')


class Engine:
    def __init__(self, context, env):
        self.context, self.env = context, env

    def call(self, *args):
        process = subprocess.run(['docker', '--context', self.context, *args], env=self.env,
                                 capture_output=True, text=True, timeout=240)
        require(process.returncode == 0, f'Docker {args[0]} failed; command/environment omitted')
        return (process.stdout + (process.stderr if args[0] == 'logs' else '')).strip()

    def inspect(self, container):
        return json.loads(self.call('inspect', '--format', INSPECT, container))

    def owned(self, container, run_id, cpu, image=None):
        state = self.inspect(container)
        require(state['id'] == container and state['owner'] == run_id, 'container identity/ownership changed')
        require((state['cpus'], state['memory'], state['swap'], state['cpuset']) == (10**9, 2 * GIB, 2 * GIB, str(cpu)), 'container limits changed')
        require(image is None or state['image'] == image, 'container image changed')
        return state


def bounds(cpu):
    return ['--cpus', '1', '--cpuset-cpus', str(cpu), '--memory', '2g', '--memory-swap', '2g', '--pids-limit', '512', '--ulimit', 'nofile=65536:65536']


def sample_container(engine, container, run_id, cpu, image, load):
    """A successful load may exit between inspect and exec; retain its terminal state."""
    def terminal(state):
        require(container == load and not state['state']['Running']
                and state['state']['ExitCode'] == 0 and not state['state']['OOMKilled']
                and state['restarts'] == 0, 'node/store exited or load failed during sampling')
        return None, state

    state = engine.owned(container, run_id, cpu, image)
    require(state['restarts'] == 0 and not state['state']['OOMKilled'], 'container restarted or OOM during sampling')
    if not state['state']['Running']:
        return terminal(state)
    try:
        raw = engine.call('exec', container, 'sh', '-c', CGROUP)
    except ValueError:
        # Recheck ownership and limits as well as process status. An exec failure
        # on a running container remains a failure, including for the load.
        state = engine.owned(container, run_id, cpu, image)
        if state['state']['Running']:
            raise
        return terminal(state)
    return parse_cgroup(raw), None


def port(state, container_port):
    bindings = state['ports'][str(container_port) + '/tcp']
    require(len(bindings) == 1 and bindings[0]['HostIp'] == '127.0.0.1', 'public port is not loopback-only')
    return int(bindings[0]['HostPort'])


def request(url, token, method='GET'):
    query = urllib.request.Request(url, method=method, headers={'authorization': 'Bearer ' + token})
    try:
        with urllib.request.urlopen(query, timeout=20) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        with error:
            detail = error.read(4096).decode(errors='replace')
        raise ValueError(f'admin HTTP {error.code}: {detail}') from None


def rows_from(path):
    return [json.loads(line.removeprefix('PRIMITIVE_COMPARE ')) for line in path.read_text().splitlines() if line.startswith('PRIMITIVE_COMPARE ')]


def capture_raft_failure(bases, token):
    """Observe each live owner before shutdown; missing nodes remain explicit."""
    evidence = {'scope':'after failure, before shutdown','observations':[],'errors':[]}
    for node, base in enumerate(bases, 1):
        try:
            evidence['observations'].append(request(base + '/state', token))
        except Exception as error:
            evidence['errors'].append({'node':node,'error':str(error)})
    return evidence


def record_load_evidence(case_dir, record):
    """Index emitted windows even when a controller failure bypassed normal collection."""
    log_path = case_dir / 'load.log'
    rows = rows_from(log_path)
    index = case_dir / 'windows.json'
    if index.exists():
        require(json.loads(index.read_text()) == rows, 'window index differs from retained load log')
    else:
        save(index, rows)
    record['windows'] = [{key:value for key,value in row.items() if key not in ('attempt_latency_us','success_latency_us','attempted_batches')} for row in rows]
    record['load_log_sha256'] = hashlib.sha256(log_path.read_bytes()).hexdigest()
    return rows


def wait_store_ready(endpoint):
    deadline = time.monotonic() + 60
    while True:
        try:
            with urllib.request.urlopen(endpoint + '/health/ready', timeout=2) as response:
                require(response.status == 200, 'RustFS readiness failed')
            return
        except OSError as error:
            if isinstance(error, urllib.error.HTTPError):
                error.close()
            if time.monotonic() > deadline:
                raise RuntimeError('RustFS readiness failed') from None
            time.sleep(.5)


def serving_leaders(observations, family):
    """Ready grants serving leadership; healthy followers do not report ready."""
    total = 48 if family == 'queue' else 24
    per_group = total // 3
    require(len(observations) == 3, 'three node observations required')
    expected_counts = {'durable_groups':3, 'attached_groups':3, 'unattached_groups':0,
                       'dormant_groups':0, 'pending_groups':0}
    for index, state in enumerate(observations):
        require(state.get('node') == index + 1, 'observation does not match its node endpoint')
        profile = state.get('profile', {})
        require(profile.get('family') == family and profile.get('logicalCells') == total
                and profile.get('groups') == 3 and profile.get('votersPerGroup') == 3
                and profile.get('cellsPerGroup') == per_group, 'wrong declared Raft population')
        require(profile == observations[0]['profile'], 'node profiles differ')
        groups = state.get('groups', [])
        require(len(groups) == 3 and all(g.get('cells') == per_group for g in groups), 'incomplete enrolled replica population')
        counts = state.get('resources', {}).get('physical_counts', {})
        require(all(counts.get(key) == value for key, value in expected_counts.items()), 'incomplete physical group inventory')
    result = []
    for ordinal in range(3):
        groups = [state['groups'][ordinal] for state in observations]
        leaders = {group.get('leader') for group in groups}
        terms = {group.get('term') for group in groups}
        require(len(leaders) == 1 and all(type(value) is int and 1 <= value <= 3 for value in leaders), 'no coherent group leader')
        require(len(terms) == 1 and all(type(value) is int and value > 0 for value in terms), 'replica terms differ')
        leader = next(iter(leaders))
        require(all(group.get('ready') is (index + 1 == leader) for index, group in enumerate(groups)),
                'serving leader not ready or another replica reports serving readiness')
        result.append(leader)
    return result


def case(args, engine, report, family, mode, config, token):
    run_id = report['run_id']
    stem = f'{family}-{mode}'
    prefix = f'cellar-pc-{run_id}-{stem}'
    owner = f'{LABEL}={run_id}'
    case_dir = args.output / stem
    case_dir.mkdir(mode=0o700)
    record = {'family': family, 'mode': mode, 'status': 'STARTING', 'resources': [], 'nodes': [], 'containers': [],
              'acknowledgement': {'raft-quorum':'RF3 Raft quorum', 'raft-s3':'RF3 Raft quorum and shared-store publication',
                                  'fleet':'Fleet peer replication or bucket proof', 'bucket':'shared-store bucket proof'}[mode]}
    report['cases'].append(record)
    save(args.output / 'run.json', report)
    def checkpoint():
        save(case_dir / 'record.json', record)
        save(args.output / 'run.json', report)
    owned = []
    store = None
    bases = []
    try:
        network = engine.call('network', 'create', '--label', owner, '--subnet', args.subnet, prefix)
        record['network'] = network
        network_prefix = args.subnet.rsplit('.', 1)[0]
        config_volume = prefix + '-config'
        engine.call('volume', 'create', '--label', owner, config_volume)
        helper = engine.call('create', '--name', prefix + '-config-copy', '--label', owner, '--network', 'none', '-v', config_volume + ':/config', '--entrypoint', '/bin/true', args.image)
        record['config_helper'] = helper
        engine.call('cp', str(config) + '/.', helper + ':/config')
        store_volume = prefix + '-store'
        engine.call('volume', 'create', '--label', owner, store_volume)
        engine.call('run', '--rm', '--network', 'none', '--label', owner, *bounds(3), '-v', store_volume + ':/data', '--entrypoint', 'chown', args.image, '10001:10001', '/data')
        store = engine.call('run', '-d', '--name', prefix + '-store', '--label', owner, '--network', network, '--network-alias', 'rustfs', *bounds(3),
                            '-p', '127.0.0.1::9000', '-v', store_volume + ':/data', '-e', 'RUSTFS_ACCESS_KEY', '-e', 'RUSTFS_SECRET_KEY', '-e', 'RUSTFS_CONSOLE_ENABLE=false', RUSTFS, '/data')
        owned.append((store, 3, None))
        record['store'] = store
        store_state = engine.owned(store, run_id, 3)
        store_image = store_state['image']
        owned[-1] = (store, 3, store_image)
        endpoint = f'http://127.0.0.1:{port(store_state, 9000)}'
        wait_store_ready(endpoint)
        bucket = 'pc-' + run_id + '-' + stem
        record['store_probe'] = probe(endpoint, bucket, engine.env['RUSTFS_ACCESS_KEY'], engine.env['RUSTFS_SECRET_KEY'])
        common = ['-e', 'AWS_ACCESS_KEY_ID', '-e', 'AWS_SECRET_ACCESS_KEY', '-e', 'CELLAR_PERF_TOKEN',
                  '-e', f'CELLAR_PERF_PRIMITIVE={family}', '-e', f'CELLAR_PERF_DURABILITY={mode}', '-e', f'CELLAR_PERF_BUCKET={bucket}',
                  '-e', f'RAFT_NETWORK_PREFIX={network_prefix}', '-v', config_volume + ':/config:ro']
        if args.lease_trace:
            common += ['-e', 'RUST_LOG=warn,cellar::actor::effects=info,store=debug']
        for i in range(1, 4):
            engine.call('volume', 'create', '--label', owner, prefix + f'-node{i}')
        if mode.startswith('raft-'):
            init = engine.call('create', '--name', prefix + '-init', '--label', owner, '--network', network, *bounds(0),
                               '-v', prefix + '-node1:/data', *common, '-e', 'RAFT_INITIALIZE=1', '-e', 'RAFT_NODE=1', args.image)
            record['initialization'] = init
            engine.call('start', init)
            exit_code = int(engine.call('wait', init))
            init_log = engine.call('logs', init)
            (case_dir / 'initialization.log').write_text(init_log + '\n')
            record['initialization_exit_code'] = exit_code
            require(exit_code == 0, 'Raft namespace initialization failed')
        for i in range(1, 4):
            command = ['run', '-d', '--name', prefix + f'-node{i}', '--label', owner, '--network', network, '--ip', f'{network_prefix}.{i+10}',
                       *bounds(i-1), '-p', '127.0.0.1::8080', '-v', prefix + f'-node{i}:/data', *common]
            if mode.startswith('raft-'):
                command += ['-p', '127.0.0.1::8082', '-e', f'RAFT_NODE={i}', args.image]
            else:
                command += ['--entrypoint', '/usr/local/bin/cellar-perf-baseline', '-e', f'CELLAR_PERF_NODE=raft-node{i}',
                            '-e', 'CELLAR_PERF_PEER_KEY', '-e', 'CELLAR_PERF_DATA_DIR=/data', '-e', 'CELLAR_PERF_THREADS=1',
                            '-e', 'CELLAR_PERF_MAX_RESIDENT=128', '-e', 'CELLAR_PERF_MAX_CACHED_ROUTES=256', '-e', 'CELLAR_PERF_MAX_REQUESTS=128',
                            args.image, '--bucket', bucket, '--endpoint', 'http://rustfs:9000', '--region', 'us-east-1', '--listen', '0.0.0.0:8080',
                            '--internal-listen', '0.0.0.0:8081', '--advertise', f'{network_prefix}.{i+10}:8081']
            node = engine.call(*command)
            owned.append((node, i-1, args.image))
            record['nodes'].append(node)
        states = [engine.owned(node, run_id, i, args.image) for i, node in enumerate(record['nodes'])]
        require(len({state['state']['Pid'] for state in states}) == 3 and all(state['state']['Running'] for state in states), 'three distinct running node processes required')
        record['topology_start'] = states
        bases = [f'http://127.0.0.1:{port(state, 8082)}' for state in states] if mode.startswith('raft-') else []
        deadline = time.monotonic() + 240
        while True:
            try:
                for node, cpu, image in owned:
                    state = engine.owned(node, run_id, cpu, image)
                    require(state['state']['Running'] and state['restarts'] == 0, 'node/store stopped during startup')
                if bases:
                    observations = [request(base + '/state', token) for base in bases]
                    leaders = serving_leaders(observations, family)
                    balanced = True
                    for ordinal, leader in enumerate(leaders):
                        if leader != ordinal + 1:
                            balanced = False
                            request(bases[leader-1] + f'/balance/{ordinal}', token, 'POST')
                    if not balanced:
                        time.sleep(.25)
                        continue
                    record['raft_start'] = observations
                else:
                    for state in states:
                        query = urllib.request.Request(f'http://127.0.0.1:{port(state,8080)}/ready', data=b'{}', headers={'authorization':'Bearer '+token,'content-type':'application/json'})
                        with urllib.request.urlopen(query, timeout=7) as response:
                            require(json.load(response) == {'family': family, 'entities':24}, 'readiness mismatch')
                break
            except Exception as error:
                if time.monotonic() > deadline:
                    raise RuntimeError(f'bounded topology readiness failed: {error}') from None
                time.sleep(1)
        record['status'] = 'RUNNING'
        checkpoint()
        print(json.dumps({'case':stem,'event':'ready','nodes':3,'node_cpu':1,'node_memory_gib':2}), flush=True)
        load = engine.call('run', '-d', '--name', prefix + '-load', '--label', owner, '--network', network, *bounds(4),
                           '-v', config_volume + ':/config:ro', '--entrypoint', 'python3', args.image, '/usr/local/bin/primitive_load.py',
                           '--family', family, '--case', run_id, '--hosts', *[f'{network_prefix}.{i+10}:8080' for i in range(1,4)],
                           '--seconds', str(args.seconds), '--warmup', str(args.warmup), '--repetitions', str(args.repetitions),
                           '--concurrency', *map(str, args.concurrency))
        owned.append((load, 4, args.image))
        record['load'] = load
        started = time.monotonic()
        max_seconds = 300 + args.repetitions * len(args.concurrency) * ((2 if family != 'queue' else 1) * args.seconds + args.warmup + 300)
        while engine.owned(load, run_id, 4, args.image)['state']['Running']:
            require(time.monotonic() - started < max_seconds, 'bounded load controller expired')
            sample = {'elapsed_seconds':time.monotonic()-started,'containers':{},'terminal':{}}
            for container, cpu, image in owned:
                cgroup, terminal = sample_container(engine, container, run_id, cpu, image, load)
                if cgroup is not None:
                    sample['containers'][container] = cgroup
                if terminal is not None:
                    sample['terminal'][container] = terminal
            record['resources'].append(sample)
            with (case_dir / 'resource-samples.jsonl').open('a') as samples:
                samples.write(json.dumps(sample,separators=(',',':'))+'\n')
            time.sleep(2)
        record['load_state'] = engine.owned(load, run_id, 4, args.image)
        record['load_exit_code'] = int(engine.call('wait', load))
        log_path = case_dir / 'load.log'
        log_path.write_text(engine.call('logs', load) + '\n')
        rows = record_load_evidence(case_dir, record)
        record['elapsed_seconds'] = time.monotonic() - started
        if bases:
            record['raft_end'] = [request(base + '/state', token) for base in bases]
        require(record['load_exit_code'] == 0 and not record['load_state']['state']['OOMKilled'], 'load process failed')
        validate_rows(rows, family, args)
        require(record['resources'] and all(cgroup['memory.events'].get('oom',0) == cgroup['memory.events'].get('oom_kill',0) == 0
                                          for sample in record['resources'] for cgroup in sample['containers'].values()), 'missing samples or cgroup OOM')
        record['status'] = 'PASSED'
    except BaseException as error:
        record.update(status='FAILED', error=str(error))
        if isinstance(error, Exception) and bases and 'raft_end' not in record:
            record['raft_failure'] = capture_raft_failure(bases, token)
        if not isinstance(error, Exception):
            raise
    finally:
        # Stop and retain the exact owned containers/volumes, including failures.
        # Never delete another experiment or treat a failed run as a zero rate.
        record['containers'] = []
        for container, cpu, image in reversed(owned):
            try:
                state = engine.owned(container, run_id, cpu, image)
                if state['state']['Running']:
                    engine.call('stop', '--time', '45', container)
                final = engine.owned(container, run_id, cpu, image)
                record['containers'].append(final)
                log_path = (case_dir / (('node' + str(cpu+1)) if cpu < 3 else ('store' if cpu == 3 else 'load'))).with_suffix('.log')
                log_path.write_text(engine.call('logs', container) + '\n')
                if container == record.get('load'):
                    record['load_state'] = final
                    record['load_exit_code'] = final['state']['ExitCode']
                    recovered = not (case_dir / 'windows.json').exists()
                    record_load_evidence(case_dir, record)
                    if recovered:
                        record['window_index_recovered_after_failure'] = True
                if final['state']['OOMKilled'] or final['restarts'] != 0 or final['state']['ExitCode'] != 0:
                    record.update(status='FAILED', shutdown_error='nonzero container exit, restart or OOM retained')
            except Exception as error:
                record.update(status='FAILED', cleanup_error=str(error))
        if record.get('network'):
            # Disconnect stopped members before releasing this case's subnet.
            # Retained node volumes and container identities remain inspectable.
            try:
                engine.call('network', 'rm', record['network'])
            except Exception:
                record['network_retained'] = True
        checkpoint()
        print(json.dumps({'case':stem,'status':record['status'],'windows':len(record.get('windows',[])), 'error':record.get('error')}), flush=True)


def run(args):
    require(args.context.startswith('colima-cellar-multi-raft'), 'use the owned Cellar Colima profile')
    require(args.image.startswith('sha256:') and len(args.image) == 71, 'immutable image ID required')
    require(not args.output.exists(), 'provide a new output directory')
    require(1 <= args.seconds <= 120 and 1 <= args.warmup <= 30 and 1 <= args.repetitions <= 5, 'invalid timing')
    require(args.concurrency and len(set(args.concurrency)) == len(args.concurrency) and all(1 <= c <= 32 for c in args.concurrency), 'invalid concurrency')
    require(not args.lease_trace or all(mode.startswith('raft-') for mode in args.modes),
            'lease tracing requires only Raft modes')
    args.output = args.output.resolve()
    args.output.mkdir(mode=0o700)
    token, access, secret = secrets.token_hex(32), secrets.token_hex(10), secrets.token_hex(32)
    env = dict(os.environ, AWS_ACCESS_KEY_ID=access, AWS_SECRET_ACCESS_KEY=secret, RUSTFS_ACCESS_KEY=access,
               RUSTFS_SECRET_KEY=secret, CELLAR_PERF_TOKEN=token, CELLAR_PERF_PEER_KEY=base64.b64encode(bytes([19])*32).decode())
    engine = Engine(args.context, env)
    inputs = source_manifest()
    actual = parse_manifest(engine.call('run', '--rm', '--network', 'none', '--entrypoint', 'cat', args.image, '/usr/local/bin/raft-source.sha256'))
    require(actual == inputs, 'image compiler inputs differ from current checkout')
    info = json.loads(engine.call('info','--format','{"id":{{json .ID}},"cpus":{{.NCPU}},"memory":{{.MemTotal}}}'))
    require(info['cpus'] >= 5 and info['memory'] >= 11*GIB, 'VM cannot fit the explicit node/store/client resource envelope')
    require(not engine.call('ps', '-q', '--filter', 'label='+LABEL), 'another owned comparison is running')
    config = args.output / 'config'
    config.mkdir(mode=0o700)
    (config / 'token').write_text(token)
    for i in range(1,4):
        (config / f'process{i}').write_bytes(secrets.token_bytes(16))
    for name in ['ca.der','server.der','key.der']:
        shutil.copyfile(ROOT / 'crates/cellar/transport/tcp/fixtures' / name, config / name)
    for path in config.iterdir():
        path.chmod(0o600)
    (args.output / 'source.patch').write_bytes(subprocess.check_output(['git','diff','HEAD','--binary'],cwd=ROOT))
    save(args.output / 'source-inputs.json', inputs)
    report = {'format':1, 'run_id':secrets.token_hex(6),'status':'RUNNING','context':args.context,'engine':info,
              'image':args.image,'source_revision':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
              'source_inputs_sha256':input_digest(inputs),'cases':[],
              'purpose':'measurement' if args.seconds>=30 and args.repetitions>=2 else 'smoke',
              'profile':{'node_count':3,'voters_per_raft_group':3,'resources':{'node_cpu':1,'node_memory_bytes':2*GIB,'store_cpu':1,
                          'store_memory_bytes':2*GIB,'load_cpu':1,'load_memory_bytes':2*GIB,'extra_swap_bytes':0,
                          'cpu_sets':{'nodes':[0,1,2],'store':3,'load':4}},'entities':24,'raft_groups':3,
                         'payload_bytes':256,'queue_batch_messages':8,'actor_operation_ms':5000,'actor_lease_ms':5000,
                         'lease_trace':args.lease_trace,
                         'seconds':args.seconds,'warmup_seconds':args.warmup,'repetitions':args.repetitions,'concurrency':args.concurrency},
              'compiler':engine.call('run','--rm','--network','none','--entrypoint','cat',args.image,'/usr/local/bin/raft-rustc.txt'),
              'binary_digests':engine.call('run','--rm','--network','none','--entrypoint','cat',args.image,'/usr/local/bin/executables.sha256'),
              'limitations':['One Colima VM and one local RustFS per case; no independent failure-domain or cloud-S3 qualification.',
                             'Closed-loop completion latency; queue latency is batch enqueue acknowledgement, with separate verified drain throughput.',
                             'Fleet acknowledges peer replication or a bucket proof; it is not strict Raft quorum.',
                             'Read consistency follows each public binding implementation; this does not certify equivalent read algorithms.',
                             'The existing production harness manifest budget is distinct from the failing single-process 1,024-page fixture.']}
    save(args.output / 'run.json',report)
    try:
        for family in args.families:
            for mode in args.modes:
                require(source_manifest() == inputs, 'source changed during frozen comparison')
                case(args,engine,report,family,mode,config,token)
        report['status'] = 'PASSED' if all(c['status']=='PASSED' for c in report['cases']) else 'FAILED'
    except BaseException as error:
        report.update(status='FAILED',error=str(error))
        raise
    finally:
        report['source_inputs_unchanged'] = source_manifest() == inputs
        report['engine_unchanged'] = json.loads(engine.call('info','--format','{"id":{{json .ID}},"cpus":{{.NCPU}},"memory":{{.MemTotal}}}')) == info
        if not report['source_inputs_unchanged'] or not report['engine_unchanged']:
            report['status']='FAILED'
        save(args.output/'run.json',report)
    require(report['status']=='PASSED','failed comparison results retained')


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context',default='colima-cellar-multi-raft-6aff')
    parser.add_argument('--image',required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--subnet',default='172.29.182.0/24')
    parser.add_argument('--families',nargs='+',choices=FAMILIES,default=FAMILIES)
    parser.add_argument('--modes',nargs='+',choices=MODES,default=MODES)
    parser.add_argument('--seconds',type=int,default=30)
    parser.add_argument('--warmup',type=int,default=5)
    parser.add_argument('--repetitions',type=int,default=2)
    parser.add_argument('--concurrency',nargs='+',type=int,default=[1,4,16])
    parser.add_argument('--lease-trace',action='store_true',help='record existing Raft node-lease attempt and store-request traces')
    try:
        run(parser.parse_args())
    except Exception as error:
        print(json.dumps({'status':'FAILED','error':str(error)}),flush=True)
        raise SystemExit(1)
