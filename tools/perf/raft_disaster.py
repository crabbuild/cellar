#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Destructive, isolated three-voter/RustFS disaster-recovery qualification.

Deletes ONLY the voter containers/volumes created by this invocation after exact
owner/ID checks. The controller key never enters a container. All new volumes,
logs and sanitized evidence remain available after the owned processes stop.
Requires Docker, OpenSSL 3 and a source-matching immutable measurement image.
"""
import argparse
import concurrent.futures
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import time

from raft_run import ROOT, RUSTFS, request, require, verify_image
from raft_workload import model
from run import create_bucket

LABEL = 'io.cellar.dr.run'
PURPOSE = b'cellar-docker-disaster-fence-1\0'


def command(args, *, check=True):
    result = subprocess.run(args, capture_output=True, text=True, timeout=240)
    if check and result.returncode:
        # Arguments can contain ephemeral backend credentials. Do not echo them.
        raise RuntimeError(f'{args[0]} failed (exit {result.returncode})')
    return result


def owned_container(docker, identifier, run_id):
    value = json.loads(docker('inspect', identifier))[0]
    require(value['Id'] == identifier and value['Config']['Labels'].get(LABEL) == run_id,
            'container identity/ownership changed')
    return value


def fence_and_delete(docker, run_id, old):
    """Authoritative inventory is the controller's exact created deployment.

    This fixture owns all old voters and starts no other routers/effect workers.
    Removing these immutable container IDs revokes their restart capability.
    The catalog's permanent intent also refuses old-identity process recreation.
    Production adapters must additionally revoke their platform's launch authority.
    """
    proof = []
    for identifier, volume in old:
        value = owned_container(docker, identifier, run_id)
        mounts = [m['Name'] for m in value['Mounts'] if m['Type'] == 'volume']
        require(mounts == [volume], 'unexpected voter storage')
        info = json.loads(docker('volume', 'inspect', volume))[0]
        require(info['Labels'].get(LABEL) == run_id, 'volume owner changed')
    for identifier, volume in old:
        value = owned_container(docker, identifier, run_id)
        docker('update', '--restart=no', identifier)
        if value['State']['Running']:
            docker('kill', identifier)
        value = owned_container(docker, identifier, run_id)
        require(not value['State']['Running'], 'old voter still running')
        docker('rm', identifier)
        require(identifier not in docker('ps', '-aq', '--no-trunc').split(), 'old container still exists')
        docker('volume', 'rm', volume)
        require(volume not in docker('volume', 'ls', '--format', '{{.Name}}').split(), 'old voter disk still exists')
        proof.append({'container_id': identifier, 'volume': volume, 'container_absent': True,
                      'disk_absent': True, 'restart_policy_disabled': True})
    return proof


class Qualification:
    def __init__(self, args):
        self.args = args
        self.root = args.output.resolve()
        self.root.mkdir(mode=0o700, parents=True, exist_ok=False)
        self.config = self.root / 'config'
        self.config.mkdir(mode=0o700)
        self.run_id = secrets.token_hex(12)
        self.prefix = 'cellar-dr-' + self.run_id
        self.owner = LABEL + '=' + self.run_id
        self.token, self.access, self.secret = (secrets.token_hex(32) for _ in range(3))
        self.base = [f'http://127.0.0.1:{args.port_base + n}' for n in range(1, 4)]
        self.containers = []
        self.report = {'format': 1, 'run_id': self.run_id, 'cells': args.cells, 'voters': 3,
                       'context': args.context, 'rustfs_image': RUSTFS, 'completed': False,
                       'checks': {}, 'limitations': ['Single local VM; no independent failure-domain or Amazon S3 qualification.',
                           'The Docker controller owns the complete disposable deployment; production requires an equivalent trusted fencing authority.',
                           'No cross-cell atomic restore or pending-outbox reconciliation qualification.',
                           'Correctness experiment, not a throughput or recovery-time service-level claim.']}

    def docker(self, *args):
        return command(['docker', '--context', self.args.context, *args]).stdout.strip()

    def save(self):
        (self.root / 'report.json').write_text(json.dumps(self.report, indent=2) + '\n')

    def env(self, node=1):
        return ['-e', f'RAFT_NODE={node}', '-e', 'RAFT_NODES=3', '-e', f'RAFT_CELLS={self.args.cells}',
                '-e', 'RAFT_WORKLOAD=reference', '-e', 'RAFT_DURABILITY=s3',
                '-e', f'RAFT_NETWORK_PREFIX={self.network_prefix}',
                '-e', 'AWS_ACCESS_KEY_ID=' + self.access, '-e', 'AWS_SECRET_ACCESS_KEY=' + self.secret]

    def processes(self):
        for n in range(1, 4):
            (self.config / f'process{n}').write_bytes(secrets.token_bytes(16))

    def setup(self):
        self.report['build'] = verify_image(self.args.image, self.docker)
        (self.config / 'token').write_text(self.token)
        self.processes()
        for name in ('ca.der', 'server.der', 'key.der'):
            shutil.copyfile(ROOT / 'crates/cellar/transport/tcp/fixtures' / name, self.config / name)
        for path in self.config.iterdir():
            path.chmod(0o600)
        self.key = self.root / 'controller-key.pem'
        command(['openssl', 'genpkey', '-algorithm', 'ED25519', '-out', str(self.key)])
        self.key.chmod(0o600)
        public = self.root / 'controller-public.der'
        command(['openssl', 'pkey', '-in', str(self.key), '-pubout', '-outform', 'DER', '-out', str(public)])
        data = public.read_bytes()
        require(len(data) == 44 and data[:12].hex() == '302a300506032b6570032100', 'unexpected Ed25519 public encoding')
        (self.config / 'fence-public').write_bytes(data[12:])
        (self.config / 'fence-evidence.json').write_text('{}')
        network_id = self.docker('network', 'create', '--label', self.owner, self.prefix)
        network = json.loads(self.docker('network', 'inspect', network_id))[0]
        net = ipaddress.ip_network(network['IPAM']['Config'][0]['Subnet'])
        require(net.version == 4 and net.prefixlen <= 24, 'unsupported private network')
        self.network_prefix = '.'.join(str(net.network_address).split('.')[:3])
        store = self.prefix + '-store'
        self.docker('volume', 'create', '--label', self.owner, store)
        self.docker('run', '--rm', '--network', 'none', '--label', self.owner, '-v', store + ':/data', 'alpine:3.22', 'chown', '10001:10001', '/data')
        identifier = self.docker('run', '-d', '--name', store, '--label', self.owner, '--network', self.prefix, '--network-alias', 'rustfs',
            '--cpus', '2', '--cpuset-cpus', '3,4', '--memory', '4g', '--memory-swap', '4g',
            '-p', f'127.0.0.1:{self.args.port_base}:9000', '-v', store + ':/data',
            '-e', 'RUSTFS_ACCESS_KEY=' + self.access, '-e', 'RUSTFS_SECRET_KEY=' + self.secret,
            '-e', 'RUSTFS_CONSOLE_ENABLE=false', RUSTFS, '/data')
        self.containers.append(identifier)
        deadline = time.monotonic() + 120
        while True:
            try:
                create_bucket(f'http://127.0.0.1:{self.args.port_base}', 'raft-perf', self.access, self.secret)
                break
            except Exception:
                require(time.monotonic() < deadline, 'RustFS startup timed out')
                time.sleep(1)
        value = owned_container(self.docker, identifier, self.run_id)
        self.report['rustfs_container_id'] = identifier
        self.report['rustfs_actual_image'] = value['Image']
        self.save()

    def start_node(self, n, generation, *, stale=False):
        name = f'{self.prefix}-' + ('stale' if stale else f'g{generation}') + f'-node{n}'
        volume = name
        self.docker('volume', 'create', '--label', self.owner, volume)
        network = ['--network', self.prefix]
        if not stale:
            network += ['--ip', f'{self.network_prefix}.{10+n}', '-p', f'127.0.0.1:{self.args.port_base+n}:8082']
        identifier = self.docker('run', '-d', '--name', name, '--label', self.owner, '--restart=no', *network,
            '--cpus', '1', '--cpuset-cpus', str(n-1), '--memory', '3g', '--memory-swap', '3g',
            '--ulimit', 'nofile=65536:65536', '-v', volume + ':/data', '-v', str(self.config) + ':/config:ro',
            *self.env(n), '-e', f'RAFT_DISASTER_GENERATION={generation}', self.args.image)
        self.containers.append(identifier)
        return identifier, volume

    def operator(self, action, *, check=True):
        result = command(['docker', '--context', self.args.context, 'run', '--rm', '--label', self.owner,
            '--network', self.prefix, '-v', str(self.config) + ':/config:ro', *self.env(),
            '-e', 'RAFT_DISASTER_ACTION=' + action, self.args.image], check=False)
        with (self.root / 'operator.log').open('a') as stream:
            stream.write(f'\n{action}: exit {result.returncode}\n{result.stdout}\n{result.stderr}')
        if check:
            require(result.returncode == 0, 'operator failed; inspect private operator.log')
        return result

    def leader(self, cell):
        for n, base in enumerate(self.base):
            try:
                status, view = request(base, f'/admin/{cell}', self.token)
                if status == 200 and view.get('ready') and view.get('role') == 'Leader':
                    return n
            except (OSError, ValueError):
                pass
        return None

    def ready(self):
        deadline = time.monotonic() + 180
        pending = set(range(self.args.cells))
        while pending:
            pending = {cell for cell in pending if self.leader(cell) is None}
            require(time.monotonic() < deadline, 'quorum formation timed out')
            if pending:
                time.sleep(1)

    def write(self, cell, body, headers):
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            leader = self.leader(cell)
            if leader is not None:
                status, reply = request(self.base[leader], f'/cells/{cell}', self.token, method='POST', body=body, headers=headers, timeout=20)
                if status == 200:
                    require(reply['durability'] == 's3', 'write lacks S3 acknowledgment')
                    return reply
                require(status == 503 and isinstance(reply, dict) and reply.get('error', '').startswith(
                    ('NotLeader', 'Overloaded', 'Unavailable', 'Ambiguous', 'ArchiveUnavailable')), 'unexpected command refusal')
            time.sleep(.1)
        raise RuntimeError('command did not resolve under its original identity')

    def seed_cell(self, cell):
        client = hashlib.sha256(f'{self.run_id}:{cell}'.encode()).hexdigest()[:32]
        state = None
        replies = []
        for sequence in range(1, 10):
            body = {'kind': 'initialize', 'database_bytes': 32768} if sequence == 1 else {
                'kind': 'set' if sequence % 2 else 'transfer', 'expected_revision': state['revision'],
                'value': 100 + cell + sequence, 'payload': hashlib.shake_256(f'{cell}:{sequence}'.encode()).hexdigest(4096)}
            headers = {'x-command-id': f'{client}:{sequence}', 'x-retry-until-ms': str(int(time.time()*1000) + 540000)}
            reply = self.write(cell, body, headers)
            state, changed = model(state, body)
            require(reply['state'] == state and reply['changed'] == changed, 'source differs from independent model')
            replies.append({'body': body, 'headers': headers, 'reply': reply})
        return {'cell': cell, 'state': state, 'commands': replies}

    def read_all(self, cells, *, old_tokens=False):
        count = 0
        for item in cells:
            for base in self.base:
                headers = {'x-read-consistency': 'linearizable'}
                if old_tokens:
                    headers = {'x-read-consistency': 'at-least', 'x-min-version': item['commands'][-1]['reply']['version_token']}
                status, reply = request(base, f"/cells/{item['cell']}", self.token, headers=headers, timeout=20)
                if old_tokens:
                    require(status == 503 and reply['error'] == 'WrongIncarnation', 'old token was not refused')
                else:
                    require(status == 200 and reply['state'] == item['state'], 'restored replica state differs')
                    require(reply['incarnation'] == '05' * 16 and reply['group'] != item['commands'][-1]['reply']['group'], 'old history reused')
                count += 1
        return count

    def sign(self, plans):
        receipts = {}
        for plan in plans:
            digest = bytes.fromhex(plan['fence_digest'])
            message, signature = self.root / 'fence-message', self.root / 'fence-signature'
            message.write_bytes(PURPOSE + digest)
            command(['openssl', 'pkeyutl', '-sign', '-rawin', '-inkey', str(self.key), '-in', str(message), '-out', str(signature)])
            data = signature.read_bytes()
            require(len(data) == 64, 'bad controller signature length')
            receipts[digest.hex()] = (digest + data).hex()
        (self.config / 'fence-evidence.json').write_text(json.dumps(receipts))

    def run(self):
        self.setup()
        old = [self.start_node(n, 0) for n in range(1, 4)]
        self.ready()
        print('Old quorum ready; issuing concurrent S3-durable commands.', flush=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            cells = list(pool.map(self.seed_cell, range(self.args.cells)))
        (self.root / 'acknowledged.json').write_text(json.dumps(cells, indent=2))
        plans = json.loads(self.operator('plan').stdout)
        require(len(plans) == self.args.cells, 'incomplete recovery plan')
        require(all(plan['cell'] == item['cell'] and plan['archive']['range']['last']['index'] >= item['commands'][-1]['reply']['index']
                    for plan, item in zip(plans, cells)), 'selected archive does not cover acknowledged commands')
        (self.config / 'dr-plan.json').write_text(json.dumps(plans))
        self.report['checks']['source_acknowledged_commands'] = 9 * self.args.cells
        self.report['plan_sha256'] = hashlib.sha256((self.config / 'dr-plan.json').read_bytes()).hexdigest()
        require(self.operator('recover', check=False).returncode != 0, 'missing fence was accepted')
        self.report['checks']['missing_fence_refused'] = True
        # A syntactically correct but unsigned receipt must not authorize a birth.
        (self.config / 'fence-evidence.json').write_text(json.dumps({p['fence_digest']: p['fence_digest'] + '00'*64 for p in plans}))
        require(self.operator('recover', check=False).returncode != 0, 'forged fence was accepted')
        self.report['checks']['forged_fence_refused'] = True
        self.report['deleted_voters'] = fence_and_delete(self.docker, self.run_id, old)
        self.report['checks']['all_voter_disks_deleted'] = len(old)
        self.save()
        self.sign(plans)
        print('All three old voter disks deleted; publishing fenced recovery births.', flush=True)
        started = time.monotonic()
        self.operator('recover')
        self.operator('recover')
        self.report['checks']['identical_recovery_retry'] = True
        self.processes()
        new = [self.start_node(n, 1) for n in range(1, 4)]
        self.ready()
        self.report['checks']['restored_replica_states'] = self.read_all(cells)
        self.report['checks']['old_version_tokens_refused'] = self.read_all(cells, old_tokens=True)
        self.report['restore_to_verified_seconds'] = time.monotonic() - started
        for item in cells:
            last = item['commands'][-1]
            duplicate = self.write(item['cell'], last['body'], last['headers'])
            require(duplicate['state'] == item['state'] and duplicate['changed'] == last['reply']['changed'], 'restored receipt changed')
            body = {'kind': 'transfer', 'expected_revision': item['state']['revision'], 'value': 1, 'payload': 'post-disaster'}
            headers = dict(last['headers'])
            headers['x-command-id'] = headers['x-command-id'].rsplit(':', 1)[0] + ':10'
            reply = self.write(item['cell'], body, headers)
            item['state'], changed = model(item['state'], body)
            require(reply['state'] == item['state'] and reply['changed'] == changed, 'new write failed independent model')
        self.report['checks']['duplicate_receipts'] = self.args.cells
        self.report['checks']['new_s3_writes'] = self.args.cells
        self.report['checks']['post_write_replica_states'] = self.read_all(cells)
        # Recreating an old node with its old configuration must fail admission,
        # even with a fresh empty volume and still-valid peer/SDK signing keys.
        stale, _ = self.start_node(1, 0, stale=True)
        deadline = time.monotonic() + 60
        while owned_container(self.docker, stale, self.run_id)['State']['Running']:
            require(time.monotonic() < deadline, 'old history process remained live')
            time.sleep(.2)
        state = owned_container(self.docker, stale, self.run_id)['State']
        logs = self.docker('logs', stale)
        (self.root / 'stale-stdout.log').write_text(logs)
        # Docker logs may send startup diagnostics on stderr; capture both.
        logs = command(['docker', '--context', self.args.context, 'logs', stale]).stderr + logs
        require(state['ExitCode'] != 0 and 'Fenced' in logs, 'stale startup did not fail specifically on fence')
        self.report['checks']['old_history_recreation_fenced'] = True
        self.docker('stop', '--time', '90', *(identifier for identifier, _ in new))
        self.processes()
        self.docker('start', *(identifier for identifier, _ in new))
        self.ready()
        self.report['checks']['retained_disk_restart_states'] = self.read_all(cells)
        self.report['completed'] = True
        self.save()
        print('Verified restored state, receipts, new S3 writes, old-host fencing and restart.', flush=True)

    def close(self):
        states = []
        present = set(self.docker('ps', '-aq', '--no-trunc', '--filter', 'label=' + self.owner).split())
        for identifier in reversed(self.containers):
            if identifier not in present:
                continue
            value = owned_container(self.docker, identifier, self.run_id)
            if value['State']['Running']:
                self.docker('stop', '--time', '90', identifier)
            value = owned_container(self.docker, identifier, self.run_id)
            result = command(['docker', '--context', self.args.context, 'logs', identifier], check=False)
            (self.root / (identifier[:12] + '.log')).write_text(result.stdout + result.stderr)
            states.append({'container_id': identifier, 'name': value['Name'].removeprefix('/'), 'running': value['State']['Running'],
                           'exit_code': value['State']['ExitCode'], 'oom_killed': value['State']['OOMKilled']})
        self.report['stopped_containers'] = states
        verified = all(not state['running'] and not state['oom_killed'] and
                       (state['exit_code'] != 0 if '-stale-' in state['name'] else state['exit_code'] == 0)
                       for state in states)
        self.report['shutdown_verified'] = verified
        if not verified:
            self.report['completed'] = False
        self.save()
        require(verified, 'owned process shutdown failed; inspect retained logs')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', required=True)
    parser.add_argument('--image', required=True, help='immutable source-matching Docker image ID')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--cells', type=int, default=24)
    parser.add_argument('--port-base', type=int, default=19800)
    args = parser.parse_args()
    require(1 <= args.cells <= 24 and 1024 <= args.port_base <= 65532, 'invalid fixture limits')
    os.umask(0o077)
    qualification = Qualification(args)
    try:
        qualification.run()
    finally:
        qualification.close()


if __name__ == '__main__':
    main()
