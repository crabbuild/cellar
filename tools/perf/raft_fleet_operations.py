#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Admit measured Colima capacity and initialize managed RF3 histories.

Every mutation uses the deployment runner's persisted exact-request protocol.
An unknown result stops this invocation; rerun the same mode to reconcile it.
Admission and birth alone do not qualify failure recovery or performance.
"""
import argparse
import hashlib
import json
from pathlib import Path
import time

from raft_fleet import apply, engine, save_new
from raft_fleet_storage import GIB, MOUNTS, Storage, loop_binding, validate_storage
from raft_run import read_json, request, require
from raft_topology import members


def kernel_limits(lines):
    require(len(lines) == 4, 'incomplete kernel capacity measurement')
    quota, period = [int(value) for value in lines[0].split()]
    memory, swap, used = [int(value) for value in lines[1:]]
    require(quota == period and period > 0 and memory == 3 * GIB and swap == 0
            and 0 < used <= memory, 'kernel capacity differs from the node profile')
    return {'cpu_millis': quota * 1000 // period, 'memory_bytes': memory,
            'memory_swap_disabled': True, 'used_memory_bytes': used}


def mount_bindings(mounts):
    # Engine inspection enumerates mounts from a map; list order is not an
    # identity. Keep every field and reject duplicate destinations before
    # comparing the two observations.
    bindings = {}
    for mount in mounts:
        destination = mount['Destination']
        require(destination not in bindings, 'duplicate mount destination')
        bindings[destination] = mount
    return bindings


class Run:
    def __init__(self, root):
        self.root = root.resolve()
        self.report = read_json(self.root / 'report.json')
        require(self.report['registered'] and self.report.get('bounded_storage'),
                'a completed bounded-storage deployment is required')
        self.policy = self.report['policy']
        validate_storage(self.policy)
        self.token = (self.root / 'config/token').read_text().strip()
        self.docker = engine(self.report['context'], self.root)

    def base(self, node):
        require(type(node) is int and 1 <= node <= self.policy['nodes'], 'invalid node')
        return f"http://127.0.0.1:{self.report['port_base'] + node}"

    def status(self, node):
        deadline = time.monotonic() + 15
        while True:
            try:
                code, value = request(self.base(node), '/fleet', self.token, timeout=2)
                if code == 200 and value.get('trust_revision') == value['revision']:
                    require(value['boot'] is not None, 'missing admitted boot')
                    return value
            except (OSError, ValueError):
                pass
            require(time.monotonic() < deadline, 'fresh rooted status unavailable')
            time.sleep(.2)

    def identity(self, key, revision):
        digest = hashlib.sha256((self.report['run_id'] + ':' + key).encode()).digest()
        return {'id': list(digest[:16]), 'operation': self.policy['incarnation'],
                'principal': self.policy['owner'], 'expected_revision': revision}

    def command(self, node, key, make):
        # The factory (including fresh revision/evidence capture) is never run
        # for an existing intent. Retrying cannot silently change its payload.
        digest = hashlib.sha256(key.encode()).hexdigest()
        path = self.root / f'scenario-{digest}.json'
        if not path.exists():
            body = make()
            save_new(path, {'node': node, 'key': key, 'command': body})
        intent = read_json(path)
        require(intent['node'] == node and intent['key'] == key, 'scenario intent conflict')
        command = self.root / f'scenario-{digest}-command.json'
        if command.exists():
            require(read_json(command) == intent['command'], 'scenario payload changed')
        else:
            save_new(command, intent['command'])
        apply(argparse.Namespace(run=self.root, node=node, command=command))

    def measure(self, node):
        before = self.status(node)
        require(before['state'] == 'Registered' and before['registered_histories'] == 0
                and before['groups'] == 0, 'initial qualification requires an empty Registered node')
        cid = self.report['containers'][node - 1]
        inspected = json.loads(self.docker('inspect', cid))[0]
        config = inspected['HostConfig']
        labels = inspected['Config']['Labels']
        require(inspected['Id'] == cid and inspected['State']['Running']
                and inspected['RestartCount'] == 0
                and labels.get('io.cellar.fleet.lab') == self.report['run_id']
                and labels.get('io.cellar.fleet.node') == bytes([node] * 16).hex()
                and config['NanoCpus'] == 10**9 and config['Memory'] == 3 * GIB
                and config['MemorySwap'] == 3 * GIB and config['ReadonlyRootfs']
                and config['RestartPolicy']['Name'] == 'no'
                and config['Tmpfs'] == {'/tmp': 'rw,nosuid,nodev,size=134217728'}
                and config['LogConfig'] == {'Type': 'local', 'Config': {'max-size': '8m', 'max-file': '2'}},
                'container identity or enforced resource policy changed')
        kernel = kernel_limits(self.docker('exec', cid, 'cat',
            '/sys/fs/cgroup/cpu.max', '/sys/fs/cgroup/memory.max',
            '/sys/fs/cgroup/memory.swap.max', '/sys/fs/cgroup/memory.current').splitlines())
        storage = read_json(self.root / 'storage-verification.json')['volumes']
        mounted = mount_bindings(inspected['Mounts'])
        measured = {}
        for role, destination in MOUNTS.items():
            record = storage[f'node{node}-{role}']
            require(record['bytes'] == self.policy['storage'][role + '_bytes']
                    and record['volume'] == f"{self.report['prefix']}-node{node}-{role}",
                    'retained storage record differs from deployment policy')
            mount = mounted.get(destination, {})
            require(mount.get('Name') == record['volume'] and mount.get('RW') is True,
                    'native storage mount differs from its retained filesystem')
            volume = json.loads(self.docker('volume', 'inspect', record['volume']))[0]
            require(volume['Labels'].get('io.cellar.fleet.lab') == self.report['run_id']
                    and volume['Options'] == {'type': 'ext4', 'device': record['device'], 'o': 'rw,nodiscard'},
                    'storage volume binding changed')
            binding = json.loads(Storage.guest(None, 'sudo', 'losetup', '--list', '--json',
                                 '--output', 'NAME,BACK-FILE', record['device']))
            loop_binding(binding, record['device'], record['backing'])
            require(Storage.guest(None, 'sudo', 'blkid', '-s', 'UUID', '-o', 'value', record['device'])
                    == record['uuid'], 'storage filesystem identity changed')
            block, total, available = [int(n) for n in self.docker('exec', cid, 'stat', '-f',
                                        '-c', '%S %b %a', destination).split()]
            require(block > 0 and 0 < available <= total and total * block <= record['bytes'],
                    'invalid filesystem capacity measurement')
            measured[role] = {'binding': record, 'usable_bytes': total * block,
                              'available_bytes': available * block}
        require(sum(item['usable_bytes'] for item in measured.values()) >= self.policy['durable_bytes'],
                'mounted filesystems cannot cover declared capacity')
        after = self.status(node)
        final = json.loads(self.docker('inspect', cid))[0]
        require(before == after and final['State']['Running']
                and final['State']['StartedAt'] == inspected['State']['StartedAt']
                and final['HostConfig'] == config and mount_bindings(final['Mounts']) == mounted,
                'node changed during capacity measurement')
        record = {'node': node, 'container': cid, 'started_at': final['State']['StartedAt'],
                  'native': after, 'kernel': kernel, 'storage': measured, 'policy': self.policy}
        used_memory = max(kernel['used_memory_bytes'], after['bounded_buffer_bytes'])
        require(used_memory < kernel['memory_bytes'], 'native buffers exceed capacity')
        path = self.root / f'capacity-node{node}-evidence-{time.time_ns()}.json'
        save_new(path, record)
        digest = hashlib.sha256(path.read_bytes()).digest()
        return {'boot': after['boot'], 'profile_revision': 1, 'profile_digest': after['profile_digest'],
                **kernel, 'used_memory_bytes': used_memory,
                'durable_bytes': self.policy['durable_bytes'], 'engine_slots': self.policy['group_capacity'],
                'used_durable_bytes': self.policy['resources']['durable_overhead_bytes'],
                'evidence_digest': list(digest), 'evidence_reference': 'colima-capacity-1:' + digest.hex()}

    def admit(self):
        for node in range(1, self.policy['nodes'] + 1):
            path = self.root / f'capacity-node{node}-attestation.json'
            if not path.exists():
                save_new(path, self.measure(node))
            attestation = read_json(path)
            for action in ('Qualify', 'Activate'):
                key = f'capacity-{node}-{action}'
                self.command(node, key, lambda: {'action': 'Capacity', 'request': {
                    'identity': self.identity(key, self.status(node)['revision']),
                    'attestation': attestation, 'action': action}})
                state = self.status(node)['state']
                require(state in ('Ready', 'Active') if action == 'Qualify' else state == 'Active',
                        'capacity transition did not settle')
            print(f'Node {node}: measured capacity Active', flush=True)

    def seed(self):
        require(all(self.status(n)['state'] == 'Active' for n in range(1, self.policy['nodes'] + 1)),
                'all intended initial voters must have Active capacity')
        for cell in range(self.report['cells']):
            digest = hashlib.sha256(f"{self.report['run_id']}:cell:{cell}".encode()).digest()
            history = {'cell': cell, 'group': list(digest[:16]), 'incarnation': list(digest[16:])}
            voters = members(cell, self.policy['nodes'])
            for action in ('Reserve', 'Confirm'):
                key = f'birth-{cell}-{action}'
                self.command(1, key, lambda: {'action': action, 'history': history,
                    'request': self.identity(key, self.status(1)['revision']),
                    **({'voters': voters} if action == 'Reserve' else {})})
            for node in voters:
                self.command(node, f'birth-{cell}-activate-{node}',
                             lambda: {'action': 'ActivateInitial', 'history': history})
            print(f'Cell {cell}: initial members activated on {voters}', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('admit', 'seed'))
    parser.add_argument('--run', type=Path, required=True)
    args = parser.parse_args()
    getattr(Run(args.run), args.mode)()


if __name__ == '__main__':
    main()
