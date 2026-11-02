#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Three-container Quorum/Fleet/bucket comparison on disposable RustFS.

The output and data directory must be on a writable Colima mount. Data and
containers are retained; only this invocation's containers are stopped on exit.
Small requests modify a hot row while dense, incompressible state stays present.
Use --cell-mib 0 to start with fresh cells without bulk state population.
"""
import argparse
import base64
import concurrent.futures
from contextlib import nullcontext
import hashlib
import http.client
import ipaddress
import json
import math
from pathlib import Path
import random
import secrets
import shutil
import subprocess
import threading
import time

from raft import captured_node_limits, wait_for_placement
from raft_run import ROOT, RUSTFS, request, require, verify_image
from raft_workload import Ledger, model
from run import create_bucket

MIB = 1024**2
LABEL = 'io.cellar.dense.run'


def expected_block(seed, block):
    prefix = seed.to_bytes(8, 'little') + block.to_bytes(8, 'little')
    digest = hashlib.sha256()
    for offset in range(32768):
        digest.update(hashlib.sha256(prefix + offset.to_bytes(8, 'little')).digest())
    return digest.hexdigest()


def quantiles(values):
    ordered = sorted(values)
    return {name: ordered[min(len(ordered)-1, math.ceil(p * len(ordered))-1)] if ordered else None
            for name, p in [('p50', .5), ('p95', .95), ('p99', .99), ('max', 1)]}


class Experiment:
    def __init__(self, args, mode):
        self.args, self.mode = args, mode
        self.root = args.output / mode
        self.root.mkdir(mode=0o700, parents=True, exist_ok=False)
        self.config = self.root / 'config'
        self.config.mkdir(mode=0o700)
        self.run_id = secrets.token_hex(12)
        self.prefix = 'cellar-dense-' + self.run_id
        self.nodes = [f'{self.prefix}-node{i}' for i in range(1, 4)]
        self.token, self.access, self.secret = [secrets.token_hex(32) for _ in range(3)]
        self.peer = base64.b64encode(secrets.token_bytes(32)).decode()
        (self.config / 'token').write_text(self.token)
        for i in range(1, 4):
            (self.config / f'process{i}').write_bytes(secrets.token_bytes(16))
        for name in ('ca.der', 'server.der', 'key.der'):
            shutil.copyfile(ROOT / 'crates/cellar/transport/tcp/fixtures' / name, self.config / name)
        for path in self.config.iterdir():
            path.chmod(0o600)
        self.bases = [f'http://127.0.0.1:{args.port_base+i}' for i in range(1, 4)]
        self.state = {}
        self.ledger = Ledger(self.root / 'operations.jsonl', mode=getattr(args, 'ledger_mode', 'group'))
        self.stop_sample = threading.Event()
        self.sample_thread = None
        self.report = {'format': 1, 'mode': mode, 'source_revision': subprocess.check_output(
            ['git', '-C', str(ROOT), 'rev-parse', 'HEAD'], text=True).strip(),
            'working_tree_dirty': bool(subprocess.check_output(['git', '-C', str(ROOT), 'status', '--porcelain'], text=True).strip()),
            'run_id': self.run_id, 'prefix': self.prefix, 'context': args.context,
            'cells': args.cells, 'logical_bytes_per_cell': args.cell_mib * MIB,
            'initial_state': 'no bulk padding' if args.cell_mib == 0 else 'dense bulk padding',
            'image_limit_mib': 1152, 'write_bytes': args.write_bytes,
            'quorum_population_timeout_ms': 120000, 'quorum_foreground_timeout_ms': 5000,
            'guest_data_root': str(args.guest_data_root) if args.guest_data_root else None,
            'preloaded_quorum': args.preload_quorum and mode == 'quorum',
            'seconds': args.seconds, 'repeats': args.repeats, 'concurrency': args.concurrency,
            'durability': 'fleet-or-bucket' if mode == 'fleet' else mode,
            'operations': getattr(args, 'operations', ['write', 'read']),
            'ledger_mode': getattr(args, 'ledger_mode', 'group'),
            'read_ledger_policy': getattr(args, 'read_ledger_policy', 'window'),
            'read_ledger_max_records': getattr(args, 'read_ledger_max_records', 200000),
            'read_ledger_max_bytes': getattr(args, 'read_ledger_max_mib', 128) * MIB,
            'completed': False, 'windows': [], 'audit': [], 'state_audit': [],
            'limitations': ['Containers share one physical host and VM; no failure injection or soak.',
                            'Quorum uses Raft RF3 and follower token reads; Fleet/bucket use owner reads.',
                            'Fleet may acknowledge either follower persistence or bucket persistence.',
                            'Foreground writes update a small hot row; any padding is immutable after setup.',
                            ('Storage uses an owned guest ext4 mount; its disk-image backing is recorded separately.' if args.guest_data_root
                             else 'Storage uses host virtiofs binds; measurements include that I/O layer.'),
                            'Response latency excludes client ledger sync. Throughput includes durable append waits and client bookkeeping; deferred read-evidence flushes are reported outside timing.',
                            ('Dense size and sampled block hashes are verified; no full payload scrub.' if args.cell_mib
                             else 'Fresh schemas and business rows only; acknowledged states are audited, without dense-block hashes.')]}
        self.save()

    def docker(self, *command, timeout=600):
        result = subprocess.run(['docker', '--context', self.args.context, *map(str, command)],
                                capture_output=True, text=True, timeout=timeout)
        # Never print arguments or arbitrary stderr: environments contain secrets.
        require(result.returncode == 0, f'Docker {command[0]} failed; exit={result.returncode}')
        return (result.stdout + (result.stderr if command[0] == 'logs' else '')).strip()

    def save(self):
        temp = self.root / 'report.tmp'
        temp.write_text(json.dumps(self.report, indent=2) + '\n')
        temp.replace(self.root / 'report.json')

    def call(self, cell, body=None, member=None, audit=False, block=None, setup=False):
        current = self.state.get(cell, {})
        target = cell % 3 if member is None else member
        headers = {}
        if body is not None:
            sequence = current.get('sequence', 0) + 1
            headers = {'x-command-id': f'{self.run_id}{cell:08x}:{sequence}',
                       'x-retry-until-ms': str(int(time.time()*1000) + 300000)}
            if setup:
                headers['x-cellar-setup'] = 'preloaded-warmup'
        else:
            headers = {'x-read-consistency': 'at-least' if self.mode == 'quorum' else 'owner',
                       'x-min-version': current.get('version_token', '')}
            if audit:
                headers['x-cellar-dense-audit'] = json.dumps(block)
        return request(self.bases[target], f'/cells/{cell}', self.token,
                       method='POST' if body is not None else 'GET', body=body, headers=headers, timeout=120)

    def mutate(self, cell, body, phase):
        sequence = self.state.get(cell, {}).get('sequence', 0) + 1
        self.ledger.append({'event': 'invoke', 'phase': phase, 'cell': cell,
                            'sequence': sequence, 'input': body})
        began = time.monotonic()
        code, reply = self.call(cell, body, setup=phase == 'preloaded_warmup')
        self.ledger.append({'event': 'complete', 'phase': phase, 'cell': cell,
                            'sequence': sequence, 'status': code, 'response': reply,
                            'wire_ms': (time.monotonic()-began)*1000})
        require(code == 200 and reply.get('durability') == self.report['durability'],
                f'{phase} cell {cell} sequence {sequence} failed: {code}; inspect ledger')
        if body['kind'] != 'populate':
            expected, changed = model(self.state.get(cell, {}).get('state'), body)
            require(reply['state'] == expected and reply['changed'] == changed, 'state/receipt mismatch')
        else:
            require(reply['state'] == self.state[cell]['state'] and reply['changed'], 'padding changed business state')
        self.state[cell] = {'state': reply['state'], 'sequence': sequence,
                            **{key: reply[key] for key in ('version_token', 'index') if key in reply}}

    def setup(self):
        self.report['build'] = verify_image(self.args.image, self.docker,
            binary_name='cellar-perf-raft' if self.mode == 'quorum' else 'cellar-perf-baseline')
        self.report['vm'] = json.loads(self.docker('info', '--format', '{"cpus":{{.NCPU}},"memory_bytes":{{.MemTotal}}}'))
        require(self.report['vm']['cpus'] >= 6 and self.report['vm']['memory_bytes'] >= 14*1024**3,
                'VM cannot provide the declared resources')
        require(not self.docker('ps', '-q', '--filter', f'label={LABEL}'), 'another dense run is active')
        network_id = self.docker('network', 'create', '--label', f'{LABEL}={self.run_id}', self.prefix)
        network = json.loads(self.docker('network', 'inspect', network_id))[0]['IPAM']['Config'][0]['Subnet']
        net = ipaddress.ip_network(network)
        require(net.version == 4 and net.prefixlen <= 24, 'unsupported private subnet')
        network_prefix = '.'.join(str(net.network_address).split('.')[:3])
        if self.args.guest_data_root:
            data = self.args.guest_data_root / self.run_id
            script = f'mkdir /owned/{self.run_id} && mkdir /owned/{self.run_id}/rustfs /owned/{self.run_id}/node1 /owned/{self.run_id}/node2 /owned/{self.run_id}/node3 && chmod 777 /owned/{self.run_id}/rustfs'
            self.docker('run', '--rm', '--network', 'none', '--entrypoint', 'sh',
                        '--mount', f'type=bind,src={self.args.guest_data_root},dst=/owned', self.args.image, '-c', script)
        else:
            data = self.root / 'data'
            data.mkdir(mode=0o755)
            (data / 'rustfs').mkdir(mode=0o777)
            (data / 'rustfs').chmod(0o777)
        store = data / 'rustfs'
        self.docker('run', '-d', '--name', self.prefix+'-store', '--label', f'{LABEL}={self.run_id}',
                    '--network', self.prefix, '--network-alias', 'rustfs', '--cpus', '2', '--cpuset-cpus', '3,4',
                    '--memory', '4g', '--memory-swap', '4g', '-p', f'127.0.0.1:{self.args.port_base}:9000',
                    '-v', f'{store}:/data', '-e', 'RUSTFS_ACCESS_KEY='+self.access,
                    '-e', 'RUSTFS_SECRET_KEY='+self.secret, '-e', 'RUSTFS_CONSOLE_ENABLE=false', RUSTFS, '/data')
        self.report['object_store'] = {'reference': RUSTFS,
            'image': self.docker('inspect', '--format', '{{.Image}}', self.prefix+'-store')}
        bucket = 'raft-perf' if self.mode == 'quorum' else 'baseline-perf'
        deadline = time.monotonic()+120
        while True:
            try:
                create_bucket(f'http://127.0.0.1:{self.args.port_base}', bucket, self.access, self.secret)
                break
            except Exception:
                require(time.monotonic() < deadline, 'RustFS startup failed')
                time.sleep(1)
        if self.args.preload_quorum and self.mode == 'quorum':
            require(self.args.guest_data_root is not None, 'preload requires native guest storage')
            command = ['run', '--name', self.prefix+'-preload', '--label', f'{LABEL}={self.run_id}',
                       '--network', self.prefix, '--cpus', '2', '--memory', '3g', '--memory-swap', '3g',
                       '-v', f'{data}:/replicas', '-v', f'{self.config}:/config:ro',
                       '-e', 'RAFT_NODE=1', '-e', f'RAFT_CELLS={self.args.cells}',
                       '-e', 'RAFT_WORKLOAD=reference', '-e', 'RAFT_DENSE_IMAGE_MIB=1152',
                       '-e', 'RAFT_DENSE_PRELOAD_ONLY=1', '-e', f'RAFT_DENSE_PRELOAD_MIB={self.args.cell_mib}',
                       '-e', 'AWS_ACCESS_KEY_ID='+self.access, '-e', 'AWS_SECRET_ACCESS_KEY='+self.secret, self.args.image]
            print('Building and independently verifying initial Quorum images on all voters.', flush=True)
            output = self.docker(*command, timeout=1800)
            (self.root / 'preload.log').write_text(output+'\n')
            self.report['preload_container'] = self.docker('inspect', '--format', '{{json .State}}', self.prefix+'-preload')
        for i, node in enumerate(self.nodes, 1):
            local = data / f'node{i}'
            if not self.args.guest_data_root:
                local.mkdir()
            common = ['run', '-d', '--name', node, '--label', f'{LABEL}={self.run_id}', '--network', self.prefix,
                      '--network-alias', f'node{i}', '--ip', f'{network_prefix}.{i+10}',
                      '--cpus', '1', '--cpuset-cpus', str(i-1), '--memory', '3g', '--memory-swap', '3g',
                      '--ulimit', 'nofile=65536:65536', '--log-opt', 'max-size=20m', '--log-opt', 'max-file=2',
                      '-v', f'{local}:/data', '-e', 'AWS_ACCESS_KEY_ID='+self.access,
                      '-e', 'AWS_SECRET_ACCESS_KEY='+self.secret]
            if self.mode == 'quorum':
                common += ['-p', f'127.0.0.1:{self.args.port_base+i}:8082', '-v', f'{self.config}:/config:ro',
                           '-e', f'RAFT_NODE={i}', '-e', f'RAFT_CELLS={self.args.cells}',
                           '-e', 'RAFT_TICK_MS=500', '-e', 'RAFT_QUEUE_ITEMS=4096',
                           '-e', 'RAFT_WORKLOAD=reference', '-e', 'RAFT_DURABILITY=quorum',
                           '-e', f'RAFT_NETWORK_PREFIX={network_prefix}', '-e', 'RAFT_DENSE_IMAGE_MIB=1152', self.args.image]
            else:
                common += ['-p', f'127.0.0.1:{self.args.port_base+i}:8080',
                           '-e', f'CELLAR_PERF_NODE=node{i}', '-e', 'CELLAR_PERF_DATA_DIR=/data/state',
                           '-e', 'CELLAR_PERF_THREADS=1', '-e', 'CELLAR_PERF_MAX_RESIDENT=32',
                           '-e', 'CELLAR_PERF_MAX_CACHED_ROUTES=32', '-e', 'CELLAR_PERF_MAX_REQUESTS=128',
                           '-e', 'CELLAR_PERF_PEER_KEY='+self.peer, '-e', 'CELLAR_PERF_DURABILITY='+self.mode,
                           '-e', 'CELLAR_PERF_TOKEN='+self.token, '-e', 'CELLAR_PERF_RUN_ID='+self.run_id,
                           '--entrypoint', '/usr/local/bin/cellar-perf-baseline', self.args.image,
                           '--bucket', bucket, '--endpoint', 'http://rustfs:9000', '--region', 'us-east-1',
                           '--listen', '0.0.0.0:8080', '--internal-listen', '0.0.0.0:8081', '--advertise', f'node{i}:8081']
            self.docker(*common)
        if self.mode == 'quorum':
            def balance(cell):
                try:
                    code, view = request(self.bases[cell % 3], f'/admin/{cell}', self.token)
                    if code != 200:
                        return False
                    if view['leader'] and view['leader'] != cell % 3 + 1:
                        request(self.bases[view['leader']-1], f'/admin/{cell}', self.token, method='POST')
                    return view['ready'] and view['leader'] == cell % 3 + 1
                except Exception:
                    return False
            wait_for_placement(self.args.cells, balance, time.monotonic()+600, print)
        else:
            for base in self.bases:
                deadline = time.monotonic()+120
                while True:
                    try:
                        code, _ = request(base, '/.well-known/cellar/health', self.token)
                        if code == 200:
                            break
                    except Exception:
                        pass
                    require(time.monotonic() < deadline, 'node startup failed')
                    time.sleep(.5)
        self.report['limits'] = [captured_node_limits(json.loads(self.docker('inspect', '--format', '{{json .HostConfig}}', node))) for node in self.nodes]
        for i, (node, limits) in enumerate(zip(self.nodes, self.report['limits'])):
            require(limits == {'NanoCpus': 10**9, 'CpusetCpus': str(i), 'Memory': 3*1024**3,
                               'MemorySwap': 3*1024**3, 'Tmpfs': {}}, 'resource limit mismatch')
            require(self.docker('exec', node, 'sha256sum', '/proc/1/exe').split()[0] == self.report['build']['binary_sha256'], 'wrong deployed executable')
        self.sample_thread = threading.Thread(target=self.sample, daemon=True)
        self.sample_thread.start()
        self.save()

    def sample(self):
        try:
            with (self.root / 'resources.jsonl').open('x') as stream:
                while not self.stop_sample.is_set():
                    rows = self.docker('stats', '--no-stream', '--format', '{{json .}}', *self.nodes, self.prefix+'-store')
                    for row in rows.splitlines():
                        stream.write(json.dumps({'unix': time.time(), 'stats': json.loads(row)})+'\n')
                    stream.flush()
                    self.stop_sample.wait(1)
        except Exception as error:
            self.report['sampling_error'] = str(error)

    def populate(self):
        began = time.monotonic()
        if self.args.preload_quorum and self.mode == 'quorum':
            expected = model(None, {'kind': 'initialize', 'database_bytes': 0})[0]
            for cell in range(self.args.cells):
                code, reply = request(self.bases[cell % 3], f'/cells/{cell}', self.token,
                                      headers={'x-read-consistency': 'linearizable'}, timeout=120)
                require(code == 200 and reply['state'] == expected, 'preloaded live-quorum state differs')
                self.state[cell] = {'state': expected.copy(), 'sequence': 0,
                                    'version_token': reply['version_token'], 'index': reply['index']}
                self.ledger.append({'event': 'preloaded_initial', 'cell': cell,
                                    'payload_mib': self.args.cell_mib, 'response': reply})
            self.archive()
            self.report['preloaded_base_archive'] = self.report['archive']
            for cell in range(self.args.cells):
                self.mutate(cell, {'kind': 'set', 'expected_revision': 0,
                                   'value': 1000000, 'payload': ''}, 'preloaded_warmup')
                print(f'Initialized Quorum writer {cell+1}/{self.args.cells} with a normal majority-proven command.', flush=True)
            self.report['population_seconds'] = None
            self.report['preloaded_linearizable_admission_seconds'] = time.monotonic()-began
            self.save()
            return
        for cell in range(self.args.cells):
            self.mutate(cell, {'kind': 'initialize', 'database_bytes': 0}, 'initialize')
        # One bounded writer per node, round-robin cells; never buffer a GiB.
        def worker(worker_id):
            for block in range(0, self.args.cell_mib, 4):
                for cell in range(worker_id, self.args.cells, 3):
                    self.mutate(cell, {'kind': 'populate', 'seed': cell, 'block': block,
                                       'blocks': min(4, self.args.cell_mib-block)}, 'populate')
                if worker_id == 0 and (block % 64 == 0 or block+4 >= self.args.cell_mib):
                    print(f'{self.mode}: {min(block+4,self.args.cell_mib)}/{self.args.cell_mib} MiB per cell, elapsed={time.monotonic()-began:.1f}s', flush=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            list(executor.map(worker, range(3)))
        self.report['population_seconds'] = time.monotonic()-began
        self.save()

    def audit(self, phase):
        for cell in range(self.args.cells):
            members = range(3) if self.mode == 'quorum' else [cell % 3]
            for member in members:
                blocks = sorted({0, self.args.cell_mib//2, self.args.cell_mib-1}) if self.args.cell_mib else []
                for block in blocks:
                    code, result = self.call(cell, member=member, audit=True, block=block)
                    require(code == 200 and result['blocks'] == self.args.cell_mib
                            and result['bytes'] == self.args.cell_mib*MIB
                            and result['sha256'] == expected_block(cell, block), 'dense state audit failed')
                    if self.mode == 'quorum':
                        require(result['served_by'] == member+1, 'audit served by a different replica')
                    self.report['audit'].append({'phase': phase, 'cell': cell, 'member': member+1, 'response': result})
                code, result = self.call(cell, member=member)
                require(code == 200 and result['state'] == self.state[cell]['state'], 'acknowledged state lost')
                if self.mode == 'quorum':
                    require(result['served_by'] == member+1, 'state read served by a different replica')
                self.report['state_audit'].append({'phase': phase, 'cell': cell,
                                                  'member': member+1, 'response': result})
        self.report.setdefault('disk_usage_kib', {})[phase] = {
            node: self.docker('exec', node, 'du', '-sk', '/data') for node in self.nodes}
        self.save()

    def window(self, kind, concurrency, repeat, seconds):
        require(1 <= concurrency <= (self.args.cells if kind == 'write' else 256), 'invalid operation concurrency')
        before = self.ledger.stats()
        buffered = kind == 'read' and getattr(self.args, 'read_ledger_policy', 'window') == 'window'
        context = (self.ledger.read_window(max_records=getattr(self.args, 'read_ledger_max_records', 200000),
                                          max_bytes=getattr(self.args, 'read_ledger_max_mib', 128) * MIB)
                   if buffered else nullcontext())
        with context:
            began = time.monotonic()
            stop = began+seconds
            samples, failures = [], []
            worker_successes = [0] * concurrency
            lock = threading.Lock()
            def worker(worker_id):
                rng = random.Random(worker_id+repeat*1000)
                connections = [http.client.HTTPConnection('127.0.0.1', self.args.port_base+i, timeout=15) for i in range(1, 4)]
                # Readers may share cells; writers retain disjoint cell sets.
                cells = range(worker_id % self.args.cells, self.args.cells, concurrency)
                try:
                    while time.monotonic() < stop:
                        cell = rng.choice(cells)
                        current = self.state[cell]
                        mutation = kind == 'write'
                        sequence = current['sequence']+1
                        headers = {'Authorization': 'Bearer '+self.token, 'Content-Type': 'application/json'}
                        body = None
                        if mutation:
                            body = {'kind': 'set', 'expected_revision': current['state']['revision'],
                                    'value': sequence % 1000000, 'payload': hashlib.shake_256(f'{cell}:{sequence}'.encode()).hexdigest(self.args.write_bytes//2)}
                            headers.update({'x-command-id': f'{self.run_id}{cell:08x}:{sequence}',
                                            'x-retry-until-ms': str(int(time.time()*1000)+300000)})
                        else:
                            headers.update({'x-read-consistency': 'at-least' if self.mode == 'quorum' else 'owner',
                                            'x-min-version': current.get('version_token', '')})
                        if mutation:
                            self.ledger.append({'event': 'invoke', 'phase': 'measurement', 'cell': cell, 'sequence': sequence, 'input': body})
                        target = (cell+(0 if mutation or self.mode != 'quorum' else 1)) % 3
                        conn = connections[target]
                        sent = time.monotonic()
                        status, reply = None, None
                        try:
                            conn.request('POST' if mutation else 'GET', f'/cells/{cell}',
                                         None if body is None else json.dumps(body).encode(), headers)
                            response = conn.getresponse()
                            status = response.status
                            raw = response.read()
                            latency = (time.monotonic()-sent)*1000
                            reply = json.loads(raw)
                            require(response.status == 200, f'HTTP {response.status}: {str(reply)[:500]}')
                            if mutation:
                                expected, changed = model(current['state'], body)
                                require(reply['state'] == expected and reply['changed'] == changed
                                        and reply['durability'] == self.report['durability'], 'write receipt mismatch')
                                self.state[cell] = {'state': expected, 'sequence': sequence,
                                    **{key: reply[key] for key in ('version_token', 'index') if key in reply}}
                            else:
                                require(reply['state'] == current['state'], 'read mismatch')
                                if self.mode == 'quorum':
                                    require(reply['served_by'] == target+1, 'read served by a different replica')
                            self.ledger.append({'event': 'complete', 'phase': 'measurement', 'kind': kind, 'repeat': repeat,
                                                'concurrency': concurrency, 'cell': cell, 'sequence': sequence if mutation else current['sequence'],
                                                'status': response.status, 'wire_ms': latency, 'response': reply})
                            with lock:
                                samples.append(latency)
                                worker_successes[worker_id] += 1
                        except Exception as error:
                            # No mutation retries; quarantine this worker's cell set.
                            refused = status is not None and status != 200 and isinstance(reply, dict) and reply.get('ambiguous') is False
                            failure = {'cell': cell, 'sequence': sequence, 'error': str(error),
                                       'status': status, 'response': reply,
                                       'mutation_ambiguous': mutation and not refused}
                            self.ledger.append({'event': 'failure', 'phase': 'measurement', 'kind': kind, **failure})
                            with lock:
                                failures.append(failure)
                            break
                finally:
                    for conn in connections:
                        conn.close()
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
                list(executor.map(worker, range(concurrency)))
            elapsed = time.monotonic()-began
            finished = time.time()
        after = self.ledger.stats()
        result = {'kind': kind, 'concurrency': concurrency, 'repeat': repeat, 'offered_seconds': seconds,
                  'elapsed_seconds': elapsed, 'successful': len(samples), 'ops_per_second': len(samples)/elapsed,
                  'latency_ms': quantiles(samples), 'failures': failures, 'samples_ms': samples,
                  'successful_by_worker': worker_successes,
                  'started_unix': finished-elapsed, 'finished_unix': finished,
                  'ledger': {'mode': after['mode'], 'read_policy': 'window' if buffered else 'durable',
                             'syncs': after['sync_completed']-before['sync_completed'],
                             'records': after['records']-before['records'],
                             'read_buffer_peak_bytes': after['last_read_buffer_bytes'] if buffered else 0,
                             'after_window_flush_ms': (after['read_flush_ns']-before['read_flush_ns'])/1e6,
                             'failed': after['failed']}}
        self.report['windows'].append(result)
        self.save()
        require(bool(samples), 'empty measurement; inspect retained failure records')
        print(f'{self.mode} {kind} c={concurrency} r={repeat}: {result["ops_per_second"]:.2f} ops/s, p99={result["latency_ms"]["p99"]:.2f}ms, errors={len(failures)}', flush=True)
        require(not failures, 'measurement failed; ambiguous mutations retained in ledger')

    def archive(self):
        if self.mode != 'quorum':
            return
        began = time.monotonic()
        while True:
            rows = [request(self.bases[cell % 3], f'/admin/{cell}', self.token) for cell in range(self.args.cells)]
            if all(code == 200 and result['archived'] >= self.state[cell]['index'] for cell, (code, result) in enumerate(rows)):
                self.report['archive'] = [result for _, result in rows]
                self.report['archive_drain_seconds'] = time.monotonic()-began
                self.save()
                return
            require(time.monotonic()-began < 1800, 'archive did not cover acknowledged state')
            time.sleep(3)

    def run(self):
        try:
            self.setup()
            self.populate()
            self.audit('before')
            self.archive()
            warmup_concurrency = 1 if self.args.preload_quorum and self.mode == 'quorum' else min(3, self.args.cells)
            operations = getattr(self.args, 'operations', ['write', 'read'])
            self.window('read' if operations == ['read'] else 'write', warmup_concurrency, -1, min(5, self.args.seconds))
            for repeat in range(self.args.repeats):
                cases = [(kind, c) for kind in operations for c in self.args.concurrency]
                if repeat % 2:
                    cases.reverse()
                for kind, concurrency in cases:
                    self.window(kind, concurrency, repeat, self.args.seconds)
            self.audit('after')
            self.archive()
            self.report['completed'] = True
        except Exception as error:
            self.report['failure'] = str(error)
            print(f'{self.mode} failed: {error}', flush=True)
        finally:
            self.stop_sample.set()
            if self.sample_thread:
                self.sample_thread.join(timeout=20)
            self.report['final_state'] = self.state
            try:
                self.ledger.close()
            except Exception as error:
                self.report['completed'] = False
                self.report['ledger_failure'] = type(error).__name__
            self.report['ledger'] = self.ledger.stats()
            owned = [*self.nodes, self.prefix+'-store']
            if self.args.preload_quorum and self.mode == 'quorum':
                owned.append(self.prefix+'-preload')
            for node in owned:
                try:
                    (self.root / (node.split(self.prefix+'-')[-1]+'.log')).write_text(self.docker('logs', node))
                    inspection = json.loads(self.docker('inspect', node))[0]
                    self.report.setdefault('container_outcomes', {})[node] = inspection['State']
                    if inspection['State']['Running']:
                        script = 'for f in memory.current memory.peak memory.events cpu.stat; do echo "$f"; cat "/sys/fs/cgroup/$f"; done'
                        self.report.setdefault('cgroup_final', {})[node] = self.docker('exec', node, 'sh', '-c', script)
                    # Exact invocation-owned names only; keep all data for audit.
                    self.docker('stop', '--time', '30', node)
                    self.report.setdefault('post_stop_outcomes', {})[node] = json.loads(
                        self.docker('inspect', '--format', '{{json .State}}', node))
                except Exception as error:
                    self.report.setdefault('cleanup_errors', []).append(str(error))
            self.report['post_stop_oom_nodes'] = [name for name, value in self.report.get('post_stop_outcomes', {}).items()
                                                  if value['OOMKilled']]
            self.save()
        return self.report['completed']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', required=True)
    parser.add_argument('--image', required=True)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--guest-data-root', type=Path, help='existing, owned ext4 mount inside the Docker VM')
    parser.add_argument('--preload-quorum', action='store_true', help='use verified initial-image lab setup before Raft runtimes start')
    parser.add_argument('--modes', nargs='+', choices=['quorum', 'fleet', 'bucket'], default=['quorum', 'fleet', 'bucket'])
    parser.add_argument('--cells', type=int, default=10)
    parser.add_argument('--cell-mib', type=int, default=1024, help='bulk payload MiB per cell; 0 starts without padding')
    parser.add_argument('--write-bytes', type=int, default=1024)
    parser.add_argument('--seconds', type=int, default=20)
    parser.add_argument('--repeats', type=int, default=2)
    parser.add_argument('--concurrency', nargs='+', type=int, default=[1, 3, 10])
    parser.add_argument('--operations', nargs='+', choices=['read', 'write'], default=['write', 'read'])
    parser.add_argument('--ledger-mode', choices=['sync', 'group'], default='group', help='durable evidence sync policy; both wait before send/reuse')
    parser.add_argument('--read-ledger-policy', choices=['window', 'durable'], default='window', help='sync read-only evidence after timing or retain durable per-response appends')
    parser.add_argument('--read-ledger-max-records', type=int, default=200000)
    parser.add_argument('--read-ledger-max-mib', type=int, default=128)
    parser.add_argument('--port-base', type=int, default=21300)
    args = parser.parse_args()
    args.output = args.output.resolve()
    require(not args.output.exists(), 'provide a new output directory')
    if args.guest_data_root:
        require(args.guest_data_root.is_absolute() and str(args.guest_data_root).startswith('/mnt/cellar-dense-')
                and '..' not in args.guest_data_root.parts, 'use an owned /mnt/cellar-dense-* guest mount')
    require(1 <= args.cells <= 32 and 0 <= args.cell_mib <= 1024 and args.seconds > 0 and args.repeats > 0,
            'invalid dense workload size')
    require(not args.preload_quorum or args.cell_mib > 0, 'preloaded images require nonempty bulk state')
    require(2 <= args.write_bytes <= 65536 and args.write_bytes % 2 == 0, 'write size must be even')
    require(all(1 <= c <= 256 for c in args.concurrency), 'invalid reader concurrency')
    require('write' not in args.operations or all(c <= args.cells for c in args.concurrency),
            'write concurrency cannot exceed the cell count; use --operations read for shared-cell readers')
    require(len(args.operations) == len(set(args.operations)), 'duplicate operation kind')
    require(1 <= args.read_ledger_max_records <= 1000000 and 1 <= args.read_ledger_max_mib <= 1024, 'invalid read evidence bounds')
    results = []
    for mode in args.modes:
        results.append(Experiment(args, mode).run())
    raise SystemExit(0 if all(results) else 1)


if __name__ == '__main__':
    main()
