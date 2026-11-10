# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Sole-launcher trust boundary for permanent cell-group deployment fences.

The controller owns the complete launch ledger, including obsolete launches.
The request's required voters are a minimum, never an exhaustive inventory.
All launch/restart paths must use this controller; a privileged Docker operator
outside it can violate the contract. Its ledger and signing key stay private.
"""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets

from raft_disaster import LABEL, owned_container
from raft_run import require

PURPOSE = b'cellar-cell-group-permanent-fence-1\0'


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
    """Admission and restart serialize with irreversible component revocation.

    A failed or lost create response leaves an unresolved ledger row. Neither
    a missing container nor the minimum voter count resolves that uncertainty.
    Fencing fails closed, while the deployment remains permanently revoked.
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
        require(type(generation) is int and generation >= 0, 'invalid deployment generation')
        require(isinstance(name, str) and name and isinstance(volume, str) and volume,
                'invalid launch identity')
        with self.lock():
            value = self.read()
            require(generation not in value['revoked'], 'deployment permanently revoked')
            require(all(row['name'] != name for row in value['nodes']), 'duplicate launch identity')
            require(all(row['volume'] != volume for row in value['nodes']), 'duplicate launch disk')
            row = {'generation': generation, 'name': name, 'volume': volume, 'container_id': None}
            value['nodes'].append(row)
            save(self.path, value)
            identifier = invoke()
            require(isinstance(identifier, str) and re.fullmatch('[0-9a-f]{64}', identifier),
                    'unknown launch outcome')
            row['container_id'] = identifier
            save(self.path, value)
            return identifier

    def restart(self, identifier, invoke):
        with self.lock():
            value = self.read()
            nodes = [row for row in value['nodes'] if row['container_id'] == identifier]
            require(len(nodes) == 1, 'unknown restart identity')
            require(nodes[0]['generation'] not in value['revoked'], 'deployment permanently revoked')
            return invoke()

    @staticmethod
    def requirements(minimum):
        require(isinstance(minimum, dict) and minimum and all(
            type(generation) is int and generation >= 0 and type(count) is int and count > 0
            for generation, count in minimum.items()), 'invalid component requirements')

    def inventory(self, value, minimum):
        self.requirements(minimum)
        nodes = [row for row in value['nodes'] if row['generation'] in minimum]
        identifiers = [row['container_id'] for row in nodes]
        volumes = [row['volume'] for row in nodes]
        require(all(sum(row['generation'] == generation for row in nodes) >= count
                    for generation, count in minimum.items())
                and all(isinstance(identifier, str) and re.fullmatch('[0-9a-f]{64}', identifier)
                        for identifier in identifiers)
                and len(set(identifiers)) == len(identifiers)
                and len(set(volumes)) == len(volumes), 'incomplete writer inventory')
        # Never erase a disk or ID also attributed to an unfenced deployment.
        require(all(row['container_id'] not in identifiers and row['volume'] not in volumes
                    for row in value['nodes'] if row['generation'] not in minimum),
                'writer inventory overlaps another deployment')
        return nodes

    def revoke_component(self, minimum):
        """Close every generation before checking its complete writer inventory.

        The trusted controller maps the recovery request to all deployment
        generations in its component. Include all retained/obsolete writers,
        even when their count exceeds the request's minimum live voter roster.
        """
        self.requirements(minimum)
        with self.lock():
            value = self.read()
            value['revoked'] = sorted(set(value['revoked']).union(minimum))
            save(self.path, value)
            return [(row['container_id'], row['volume']) for row in self.inventory(value, minimum)]

    def revoke(self, generation):
        return self.revoke_component({generation: 3})

    def certificate_payload(self, digest, minimum, docker):
        """Return signed bytes only after positive, complete Docker absence reads.

        The signature attests the controller's inventory and permanent admission
        revocation. A Rust verifier cannot independently infer Docker state.
        """
        self.requirements(minimum)
        require(isinstance(digest, str) and re.fullmatch('[0-9a-f]{64}', digest)
                and digest != '0' * 64, 'invalid recovery request digest')
        with self.lock():
            value = self.read()
            require(set(minimum).issubset(value['revoked']), 'deployment is not permanently revoked')
            nodes = self.inventory(value, minimum)
            containers = set(docker('ps', '-aq', '--no-trunc').split())
            volumes = set(docker('volume', 'ls', '--format', '{{.Name}}').split())
            require(all(row['container_id'] not in containers and row['volume'] not in volumes
                        for row in nodes), 'old writer or disk still exists')
            inventory = {'format': 1, 'run_id': self.run_id, 'revoked': sorted(minimum),
                         'nodes': sorted(nodes, key=lambda row: (row['generation'], row['name']))}
            commitment = hashlib.sha256(json.dumps(inventory, sort_keys=True,
                                                   separators=(',', ':')).encode()).digest()
            return bytes([1]) + bytes.fromhex(digest) + commitment


def fence_and_delete(docker, run_id, old):
    """Idempotently erase a resolved, permanently revoked controller inventory.

    Check every remaining resource's ownership before any deletion. Positive
    list responses distinguish absence from Docker/inspection failures. A retry
    after partial deletion never needs to recreate a source container or disk.
    """
    containers = set(docker('ps', '-aq', '--no-trunc').split())
    volumes = set(docker('volume', 'ls', '--format', '{{.Name}}').split())
    for identifier, volume in old:
        if identifier in containers:
            value = owned_container(docker, identifier, run_id)
            mounts = [m['Name'] for m in value['Mounts'] if m['Type'] == 'volume']
            require(mounts == [volume], 'unexpected voter storage')
        if volume in volumes:
            info = json.loads(docker('volume', 'inspect', volume))[0]
            require(info['Labels'].get(LABEL) == run_id, 'volume owner changed')
    proof = []
    for identifier, volume in old:
        if identifier in containers:
            docker('update', '--restart=no', identifier)
            value = owned_container(docker, identifier, run_id)
            if value['State']['Running']:
                docker('kill', identifier)
            require(not owned_container(docker, identifier, run_id)['State']['Running'],
                    'old voter still running')
            docker('rm', identifier)
        require(identifier not in docker('ps', '-aq', '--no-trunc').split(), 'old container still exists')
        if volume in volumes:
            docker('volume', 'rm', volume)
        require(volume not in docker('volume', 'ls', '--format', '{{.Name}}').split(),
                'old voter disk still exists')
        proof.append({'container_id': identifier, 'volume': volume, 'container_absent': True,
                      'disk_absent': True, 'restart_policy_disabled': True})
    return proof
