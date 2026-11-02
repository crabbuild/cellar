#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Owned three-container shared-group/RustFS cold recovery, with permanent fencing.

The controller is the sole launcher. Its durable revocation ledger and private
Ed25519 key are never mounted in voters/operators. Source volumes alone are
removed after complete inventory checks; new volumes and sanitized evidence stay.
"""
import argparse
from contextlib import contextmanager
import fcntl
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/perf'))
from raft_disaster import LABEL, command, fence_and_delete, owned_container
from raft_run import input_digest, parse_manifest, request, require, source_manifest
from run import create_bucket
from group_configuration import copy_configuration

RUSTFS = 'ghcr.io/rustfs/rustfs@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858'
PURPOSE = b'cellar-docker-cell-group-fence-1\0'


def save(path, value):
    temporary = path.with_name(path.name + '.' + secrets.token_hex(8) + '.pending')
    with open(temporary, 'x', opener=lambda p, f: os.open(p, f, 0o600)) as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class Launcher:
    """Reloaded for each admission; revocation has no reverse operation.

    Holding the lock through launch resolution prevents a launch from passing
    admission before revocation and materializing after the fence inventory.
    """
    def __init__(self, root, run_id):
        self.root, self.run_id = Path(root), run_id
        self.path = self.root / 'launcher.json'
        with self.lock():
            if not self.path.exists():
                save(self.path, {'format': 1, 'run_id': run_id, 'revoked': [], 'nodes': []})
            self.read()

    @contextmanager
    def lock(self):
        with open(self.root / 'launcher.lock', 'a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def read(self):
        value = json.loads(self.path.read_text())
        require(value['format'] == 1 and value['run_id'] == self.run_id, 'foreign launcher ledger')
        return value

    def launch(self, generation, name, volume, invoke):
        with self.lock():
            value = self.read()
            require(generation not in value['revoked'], 'deployment permanently revoked')
            require(all(row['name'] != name for row in value['nodes']), 'duplicate launch identity')
            row = {'generation': generation, 'name': name, 'volume': volume, 'container_id': None}
            value['nodes'].append(row)
            save(self.path, value)
            identifier = invoke()
            require(re.fullmatch('[0-9a-f]{64}', identifier) is not None, 'unknown launch outcome')
            row['container_id'] = identifier
            save(self.path, value)
            return identifier

    def revoke(self, generation):
        with self.lock():
            value = self.read()
            nodes = [row for row in value['nodes'] if row['generation'] == generation]
            require(len(nodes) == 3 and len({row['container_id'] for row in nodes}) == 3
                    and all(row['container_id'] for row in nodes), 'incomplete writer inventory')
            if generation not in value['revoked']:
                value['revoked'].append(generation)
                save(self.path, value)
            return [(row['container_id'], row['volume']) for row in nodes]


def verify_image(image, docker):
    require(re.fullmatch('sha256:[0-9a-f]{64}', image), 'immutable Docker image ID required')
    actual = parse_manifest(docker('run', '--rm', '--network', 'none', '--entrypoint', 'cat', image, '/usr/local/bin/raft-source.sha256'))
    require(actual == source_manifest(), 'stale compiled image source manifest')
    binary = docker('run', '--rm', '--network', 'none', '--entrypoint', 'sha256sum', image, '/usr/local/bin/cellar-perf-cell-group').split()[0]
    return {'image': image, 'binary_sha256': binary, 'source_manifest_sha256': input_digest(actual), 'profile': 'dev-debug0', 'source_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()}


class Qualification:
    def __init__(self, args):
        self.args = args
        self.root = args.output.resolve()
        self.root.mkdir(mode=0o700, parents=True, exist_ok=False)
        self.config_dir = self.root / 'config'
        self.config_dir.mkdir(mode=0o700)
        self.run_id = secrets.token_hex(12)
        self.prefix = 'cellar-cg-dr-' + self.run_id
        self.owner = LABEL + '=' + self.run_id
        self.launcher = Launcher(self.root, self.run_id)
        self.containers, self.targets = [], []
        self.config = {'rooted': args.mode == 'extents', 'endpoint': 'http://rustfs:9000',
            'bucket': 'cellar-cg-dr', 'prefix': self.prefix, 'access': secrets.token_hex(10),
            'secret': secrets.token_hex(32), 'token': secrets.token_hex(32),
            'peers': [[{'host': '127.0.0.1', 'disk': list(secrets.token_bytes(16)), 'process': list(secrets.token_bytes(16))} for _ in range(3)] for _ in range(2)],
            'fence_public': [], 'certificate': [], 'recovery': []}
        self.report = {'format': 1, 'run_id': self.run_id, 'mode': args.mode, 'completed': False,
            'group_count': 1, 'cells': ['alpha', 'beta', 'gamma'], 'voters': 3, 'checks': {},
            'limitations': ['Single local VM, not independent failure domains or Amazon S3.',
                'Trusted sole-launcher Docker controller; production requires an equivalent fencing authority.',
                'No external effects, managed recovery, sustained-load or recovery-time SLA qualification.']}
        self.ledger = {'commands': [], 'observations': {}}

    def docker(self, *args):
        return command(['docker', '--context', self.args.context, *args]).stdout.strip()

    def save(self):
        save(self.root / 'report.json', self.report)
        save(self.config_dir / 'config.json', self.config)
        save(self.config_dir / 'ledger.json', self.ledger)

    def setup(self):
        require(self.args.context.startswith('colima-cellar-multi-raft'), 'dedicated qualification context required')
        self.report['build'] = verify_image(self.args.image, self.docker)
        info = json.loads(self.docker('info', '--format', '{{json .}}'))
        require(info['OSType'] == 'linux' and info['NCPU'] >= 5 and info['MemTotal'] >= 12 * 1024**3,
                'insufficient isolated Docker resources')
        self.docker('network', 'create', '--label', self.owner, self.prefix)
        network = json.loads(self.docker('network', 'inspect', self.prefix))[0]
        subnet = ipaddress.ip_network(network['IPAM']['Config'][0]['Subnet'])
        require(subnet.version == 4 and subnet.prefixlen <= 24, 'private IPv4 network required')
        prefix = '.'.join(str(subnet.network_address).split('.')[:3])
        for peers in self.config['peers']:
            for n, peer in enumerate(peers):
                peer['host'] = prefix + '.' + str(11+n)
        self.key = self.root / 'controller-private.pem'
        command(['openssl', 'genpkey', '-algorithm', 'ED25519', '-out', str(self.key)])
        self.key.chmod(0o600)
        public = self.root / 'controller-public.der'
        command(['openssl', 'pkey', '-in', str(self.key), '-pubout', '-outform', 'DER', '-out', str(public)])
        data = public.read_bytes()
        require(len(data) == 44 and data[:12].hex() == '302a300506032b6570032100', 'unexpected public key encoding')
        self.config['fence_public'] = list(data[12:])
        self.save()
        self.docker('pull', RUSTFS)
        volume = self.prefix + '-objects'
        self.docker('volume', 'create', '--label', self.owner, volume)
        # RustFS's pinned image runs as UID 10001. Use the measurement image's
        # root user for this one owned volume, without any backend credentials.
        self.docker('run', '--rm', '--network', 'none', '--label', self.owner, '--entrypoint', 'chown',
                    '-v', volume + ':/data', self.args.image, '10001:10001', '/data')
        identifier = self.docker('run', '-d', '--label', self.owner, '--name', self.prefix + '-rustfs', '--network', self.prefix, '--network-alias', 'rustfs',
            '--cpus', '1', '--memory', '2g', '--memory-swap', '2g', '-p', f'127.0.0.1:{self.args.port_base}:9000', '-v', volume + ':/data',
            '-e', 'RUSTFS_ACCESS_KEY=' + self.config['access'], '-e', 'RUSTFS_SECRET_KEY=' + self.config['secret'], '-e', 'RUSTFS_CONSOLE_ENABLE=false', RUSTFS, '/data')
        self.containers.append(identifier)
        store = owned_container(self.docker, identifier, self.run_id)
        self.capture_resources(store, 2)
        self.report['rustfs'] = {'container_id':identifier, 'image':store['Image'], 'pinned_image':RUSTFS}
        self.until(lambda: self.bucket(), 90)
        self.operator('init')

    def bucket(self):
        create_bucket(f'http://127.0.0.1:{self.args.port_base}', self.config['bucket'], self.config['access'], self.config['secret'])
        return True

    @staticmethod
    def until(operation, seconds=90):
        deadline = time.monotonic() + seconds
        last = None
        while time.monotonic() < deadline:
            try:
                value = operation()
                if value:
                    return value
            except (OSError, ValueError, RuntimeError) as error:
                last = type(error).__name__
            time.sleep(.2)
        raise RuntimeError('bounded qualification wait expired: ' + str(last))

    def stage_configuration(self, identifier):
        value = owned_container(self.docker, identifier, self.run_id)
        require(not value['State']['Running'], 'configuration cannot change under a live process')
        hashes = copy_configuration(self.args.context, identifier, self.config_dir)
        self.report.setdefault('configuration_copies', []).append({'container_id':identifier, 'sha256':hashes})
        self.save()

    def operator(self, action, node=0, *, volume=None, generation=0, check=True):
        args = ['create', '--label', self.owner, '--network', self.prefix,
            '--cpus', '1', '--memory', '3g', '--memory-swap', '3g',
            '-e', 'CELLAR_GROUP_ACTION=' + action,
            '-e', 'CELLAR_GROUP_NODE=' + str(node), '-e', 'CELLAR_GROUP_GENERATION=' + str(generation)]
        if volume:
            args += ['-v', volume + ':/data']
        identifier = self.docker(*args, self.args.image)
        self.capture_resources(owned_container(self.docker, identifier, self.run_id), 3)
        self.stage_configuration(identifier)
        result = command(['docker', '--context', self.args.context, 'start', '--attach', identifier], check=False)
        value = owned_container(self.docker, identifier, self.run_id)
        require(not value['State']['Running'], 'operator did not join')
        require(result.returncode == 0 or value['State']['ExitCode'] != 0, 'Docker attachment failed')
        result.returncode = value['State']['ExitCode']
        text = self.redact(result.stdout + result.stderr)
        with (self.root / 'operator.log').open('a') as stream:
            stream.write(action + ': exit ' + str(result.returncode) + '\n' + text + '\n')
        self.docker('rm', identifier)
        if check:
            require(result.returncode == 0, 'operator failed; inspect private sanitized operator log')
        return result, text

    def launch(self, node, generation, volume=None):
        name = f'{self.prefix}-g{generation}-node{node}'
        volume = volume or name
        if volume == name:
            self.docker('volume', 'create', '--label', self.owner, volume)
        def invoke():
            identifier = self.docker('create', '--label', self.owner, '--restart=no', '--name', name,
                '--network', self.prefix, '--ip', self.config['peers'][generation][node]['host'],
                '--cpus', '1', '--memory', '3g', '--memory-swap', '3g', '-p', f'127.0.0.1:{self.args.port_base+node+1}:{8080+node}',
                '-v', volume + ':/data',
                '-e', 'CELLAR_GROUP_NODE=' + str(node), '-e', 'CELLAR_GROUP_GENERATION=' + str(generation),
                '-e', 'CELLAR_GROUP_RETAINED=' + str(generation == 1).lower(), self.args.image)
            self.containers.append(identifier)
            self.stage_configuration(identifier)
            self.docker('start', identifier)
            captured = owned_container(self.docker, identifier, self.run_id)
            self.capture_resources(captured, 3)
            self.save()
            return identifier
        return self.launcher.launch(generation, name, volume, invoke), volume

    def capture_resources(self, container, gib):
        limits = container['HostConfig']
        require(limits['NanoCpus'] == 1_000_000_000 and limits['Memory'] == gib*1024**3
                and limits['MemorySwap'] == gib*1024**3, 'actual container resource limits differ')
        self.report.setdefault('resources', []).append({'container_id':container['Id'],
            'nano_cpus':limits['NanoCpus'], 'memory':limits['Memory'], 'memory_swap':limits['MemorySwap']})

    def call(self, node, action):
        result = request(f'http://127.0.0.1:{self.args.port_base+node+1}', '/control', self.config['token'], method='POST', body=action, timeout=25)
        with (self.root / 'http.jsonl').open('a') as stream:
            stream.write(json.dumps({'node': node, 'request': action, 'status': result[0], 'response': result[1]})+'\n')
        return result

    def leader(self, generation, ready=True):
        for node in range(3):
            try:
                status, value = self.call(node, {'action': 'status'})
                if status == 200 and value['leader'] == node + (1 if generation == 0 else 11) and (not ready or value['ready']):
                    return node + 1
            except (OSError, ValueError):
                pass
        return None

    def ready(self, generation):
        return self.until(lambda: self.leader(generation)) - 1

    def ok(self, action, generation):
        def attempt():
            leader = self.leader(generation)
            if leader is None:
                return None
            code, value = self.call(leader-1, action)
            return value if code == 200 else None
        return self.until(attempt)

    def command(self, cell, sequence, generation, *, retry=None, s3=True, input_value=None):
        body = {'action': 'command', 'cell': cell, 'sequence': sequence, 'input': input_value or [1],
                'retry_until_ms': retry or int(time.time()*1000)+300000, 's3': s3}
        row = {'generation': generation, 'request': body, 'acknowledged': None}
        self.ledger['commands'].append(row)
        self.save()
        response = self.ok(body, generation)
        require(response['s3'] and response['minimum_s3'], 'stored S3 minimum was downgraded')
        row['acknowledged'] = response
        self.save()
        return response

    def observations(self, generation, old_tokens=False):
        values = {}
        for cell in ('alpha', 'beta', 'gamma'):
            action = {'action': 'query', 'cell': cell, 'token': None}
            if old_tokens:
                action['token'] = self.ledger['observations'][cell]['token']
                node = self.ready(generation)
                code, reply = self.call(node, action)
                require(code == 503 and 'WrongHistory' in reply['error'], 'old token not refused by fresh serving path')
            else:
                values[cell] = self.ok(action, generation)
        return values

    def counts(self):
        result = [self.call(node, {'action': 'status'}) for node in range(3)]
        require(all(code == 200 for code, _ in result), 'all three voters must be live')
        return {key: sum(value[key] for _, value in result) for key in ('commands', 'initializations')}

    def sign(self, digest):
        message = self.root / 'fence-message'
        message.write_bytes(PURPOSE + bytes.fromhex(digest))
        signature = self.root / 'fence-signature'
        command(['openssl', 'pkeyutl', '-sign', '-inkey', str(self.key), '-rawin', '-in', str(message), '-out', str(signature)])
        data = signature.read_bytes()
        require(len(data) == 64, 'unexpected Ed25519 signature length')
        self.config['certificate'] = list(bytes.fromhex(digest) + data)
        self.save()

    def drill(self):
        old = [self.launch(n, 0) for n in range(3)]
        self.ready(0)
        self.ok({'action': 'archive'}, 0)
        for cell in ('alpha', 'beta', 'gamma'):
            require(self.ok({'action': 'enroll', 'cell': cell}, 0)['s3'], 'enrollment lacks S3 proof')
        expiry = int(time.time()*1000)+1000
        self.command('alpha', 1, 0, retry=expiry)
        source = self.command('alpha', 2, 0)
        time.sleep(max(0, (expiry-int(time.time()*1000))/1000)+.05)
        self.command('alpha', 3, 0)
        beta = self.command('beta', 1, 0)
        require(source['result']['value'] == 2 and beta['result']['value'] == 1, 'independent source counter oracle')
        self.ledger['observations'] = self.observations(0)
        require({k:v['value'] for k,v in self.ledger['observations'].items()} == {'alpha':3,'beta':1,'gamma':0}, 'source group state mismatch')
        require(self.counts() == {'commands':4,'initializations':3}, 'source callbacks not exact')
        self.report['checks']['source_s3_ledger_verified'] = True
        self.save()
        plan = self.ok({'action': 'plan'}, 0)
        self.config['recovery'] = plan['recovery']
        self.save()
        missing, text = self.operator('fence', check=False)
        require(missing.returncode != 0 and 'fence certificate' in text, 'missing infrastructure fence accepted')
        self.config['certificate'] = list(bytes.fromhex(plan['fence_digest']) + bytes(64))
        self.save()
        forged, text = self.operator('fence', check=False)
        require(forged.returncode != 0 and 'untrusted Docker controller fence' in text, 'forged certificate did not reach signature-verification refusal')
        inventory = self.launcher.revoke(0)
        require(inventory == old, 'durable launcher inventory differs')
        for identifier, _ in old:
            captured = command(['docker','--context',self.args.context,'logs',identifier],check=False)
            (self.root / (identifier[:12]+'-before-erasure.log')).write_text(self.redact(captured.stdout+captured.stderr))
        self.report['fence'] = fence_and_delete(self.docker, self.run_id, inventory)
        for identifier, _ in old:
            result = command(['docker', '--context', self.args.context, 'start', identifier], check=False)
            require(result.returncode != 0, 'old immutable container restarted')
        # A newly opened controller must refuse before any Docker mutation.
        reloaded = Launcher(self.root, self.run_id)
        for node in range(3):
            try:
                reloaded.launch(0, 'forbidden-'+str(node), 'forbidden', lambda: (_ for _ in ()).throw(RuntimeError('launcher reached Docker')))
            except ValueError as error:
                require(str(error) == 'deployment permanently revoked', 'wrong launch refusal')
            else:
                raise ValueError('revoked deployment launch accepted')
        self.report['checks']['old_launch_after_controller_reload_refused'] = True
        self.report['checks']['all_source_disks_deleted'] = True
        self.sign(plan['fence_digest'])
        self.operator('fence')
        bindings = []
        volumes = []
        for node in range(3):
            volume = f'{self.prefix}-target-{node}'
            self.docker('volume', 'create', '--label', self.owner, volume)
            _, text = self.operator('seed', node, volume=volume, generation=1)
            result = json.loads(text.strip().splitlines()[-1])
            require(result['seeded'] and result['cells'] == 3, 'incomplete physical seed')
            bindings.append(result['binding'])
            volumes.append(volume)
        require(len(set(bindings)) == 1, 'voters seeded different roots')
        self.operator('publish')
        for node in range(3):
            self.operator('probe-old', node)
        self.report['checks']['recreated_old_disks_refused'] = True
        for peer in self.config['peers'][1]:
            peer['process'] = list(secrets.token_bytes(16))
        self.save()
        self.targets = [self.launch(node, 1, volumes[node]) for node in range(3)]
        self.until(lambda: self.leader(1, ready=False))
        for node in range(3):
            code, _ = self.call(node, {'action':'command','cell':'alpha','sequence':4,'input':[1], 'retry_until_ms':int(time.time()*1000)+300000,'s3':False})
            require(code == 503, 'pending group admitted application work')
            code, _ = self.call(node, {'action':'query','cell':'alpha','token':None})
            require(code == 503, 'pending group admitted a strong read')
            code, _ = self.call(node, {'action':'query','cell':'alpha','token':None,'stale':True})
            require(code == 503, 'pending group admitted a stale read')
        require(self.counts() == {'commands':0,'initializations':0}, 'pending group invoked callback')
        def activate():
            node = self.leader(1, ready=False)
            if not node: return False
            code, value = self.call(node-1, {'action':'activate'})
            return code == 200 and value.get('activated')
        self.until(activate)
        self.ready(1)
        self.report['checks']['fresh_quorum_activation'] = True
        self.operator('target-archive')
        self.ok({'action':'archive'}, 1)
        self.operator('verify-old-tokens')
        self.observations(1, old_tokens=True)
        restored = self.observations(1)
        require({k:v['value'] for k,v in restored.items()} == {'alpha':3,'beta':1,'gamma':0}, 'cold group state mismatch')
        require(all(v['version']==1 and v['incarnation']=='47'*16 for v in restored.values()), 'fresh cell histories not reset')
        for cell, sequence, original in [('alpha',2,source),('beta',1,beta)]:
            reply = self.command(cell, sequence, 1, s3=False)
            require(reply['result'] == original['result'] and reply['version']==1, 'original receipt result was re-executed or retagged')
        node = self.ready(1)
        for sequence, input_value, expected in [(1,[1],'CommandExpired'),(2,[2],'CommandConflict')]:
            code, value = self.call(node, {'action':'command','cell':'alpha','sequence':sequence,'input':input_value,'retry_until_ms':int(time.time()*1000)+300000,'s3':False})
            require(code == 503 and expected in value['error'], 'restored receipt floor/identity lost')
        require(self.counts() == {'commands':0,'initializations':0}, 'recovery replayed callbacks')
        self.report['checks'].update(source_cut_restored=True, exact_receipts_no_replay=True, s3_minimums=True, client_floors=True, old_tokens_refused=True)
        fresh = self.command('alpha', 4, 1)
        require(fresh['result']['value']==4 and fresh['result']['incarnation']=='47'*16 and fresh['version']==2, 'fresh write not in new history')
        require(self.counts() == {'commands':1,'initializations':0}, 'fresh callback count mismatch')
        # Retain disks but stop every process before giving the next processes
        # new generation IDs. Removing containers is not needed for this restart.
        self.docker('stop', '--time','60', *(identifier for identifier,_ in self.targets))
        for peer in self.config['peers'][1]: peer['process'] = list(secrets.token_bytes(16))
        self.save()
        for identifier, _ in self.targets:
            self.stage_configuration(identifier)
        self.docker('start', *(identifier for identifier,_ in self.targets))
        self.ready(1)
        self.ok({'action':'archive'},1)
        for cell, sequence, original in [('alpha',2,source),('beta',1,beta)]:
            restored_retry = self.command(cell,sequence,1,s3=False)
            # The original opaque result stays fixed. Its proof observes the
            # current cell image: alpha advanced once after genesis; beta did not.
            expected_version = 2 if cell == 'alpha' else 1
            require(restored_retry['result']==original['result'] and restored_retry['version']==expected_version,
                    'imported receipt or current observation lost on retained restart')
        retry = self.command('alpha',4,1,s3=False)
        require(retry['result']==fresh['result'] and retry['version']==2, 'retained receipt changed')
        require(self.counts()=={'commands':0,'initializations':0}, 'retained restart replayed callback')
        after = self.command('alpha',5,1)
        require(after['result']['value']==5 and after['version']==3, 'post-restart write failed')
        self.verify_replicas({'alpha':5,'beta':1,'gamma':0}, {'alpha':3,'beta':1,'gamma':1})
        self.report['checks'].update(native_shared_group=True, all_source_disks_deleted=True, source_cut_restored=True,
            fresh_quorum_activation=True, exact_receipts_no_replay=True, s3_minimums=True, client_floors=True,
            old_tokens_refused=True, fresh_writes=True, retained_restart=True)
        self.report['completed'] = True
        self.save()

    def verify_replicas(self, expected_values, expected_versions):
        leader = self.ready(1)
        code, selected = self.call(leader, {'action':'status'})
        require(code == 200, 'selected leader unavailable')
        for node in range(3):
            self.until(lambda: self.call(node, {'action':'status'})[1]['applied'] >= selected['applied'])
            for cell, expected in expected_values.items():
                code, reply = self.call(node, {'action':'query','cell':cell,'token':None,'stale':True})
                require(code == 200 and reply['value']==expected and reply['version']==expected_versions[cell]
                        and reply['incarnation']=='47'*16, 'replica state/history/version mismatch')
        self.report['checks']['all_three_replica_images_verified'] = True

    def redact(self, value):
        for key in ('access','secret','token'):
            value = value.replace(self.config[key], '[REDACTED]')
        return value

    def close(self):
        states = []
        present = set(self.docker('ps','-aq','--no-trunc','--filter','label='+self.owner).split())
        for identifier in sorted(present):
            value = owned_container(self.docker, identifier, self.run_id)
            if value['State']['Running']: self.docker('stop','--time','60',identifier)
            value = owned_container(self.docker, identifier, self.run_id)
            result = command(['docker','--context',self.args.context,'logs',identifier],check=False)
            (self.root / (identifier[:12]+'.log')).write_text(self.redact(result.stdout+result.stderr))
            states.append({'id':identifier,'running':value['State']['Running'],'exit_code':value['State']['ExitCode'],'oom_killed':value['State']['OOMKilled']})
        self.report['stopped_containers']=states
        self.report['shutdown_verified']=bool(states) and all(not s['running'] and not s['oom_killed'] and s['exit_code']==0 for s in states)
        if not self.report['shutdown_verified']: self.report['completed']=False
        self.save()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context',required=True)
    parser.add_argument('--image',required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--mode',choices=['materialized','extents'],required=True)
    parser.add_argument('--port-base',type=int,default=19800)
    args=parser.parse_args()
    require(1024<=args.port_base<65530,'invalid loopback port range')
    os.umask(0o077)
    q=Qualification(args)
    try:
        q.setup();q.drill()
    except BaseException as error:
        q.report['failure']=type(error).__name__+': '+q.redact(str(error));q.report['completed']=False
        raise
    finally:
        q.close()
    require(q.report['completed'] and q.report['shutdown_verified'],'incomplete drill')
    print('PASS shared-group cold recovery after permanent Docker fencing ('+args.mode+')',flush=True)


if __name__=='__main__': main()
