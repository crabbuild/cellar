# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Bind a managed SDK fence request to the sole launcher's historical inventory.

The authenticated caller supplies the expected SDK request digest separately.
The canonical intent, rather than a supplied voter count, determines the minimum
lineages. Deployment declarations and launch identities are controller-owned;
they must be persisted before starting their writers. This reference controller
does not infer inventory from unreachable peers or recreate an unknown launch.
"""
import hashlib
import json
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'tools/perf'))

from group_fence import Launcher, fence_and_delete, save
from raft_run import decode_json, require

INTENT_LIMIT = 4 * 1024 * 1024
CHALLENGE_LIMIT = 2 * INTENT_LIMIT + 1024
FENCE_DOMAIN = b'cellar-managed-transition-fence-1\0'


def identifier(value, size):
    require(isinstance(value, str) and re.fullmatch('[0-9a-f]{' + str(size * 2) + '}', value)
            and value != '0' * (size * 2), 'invalid managed fence identity')
    return value


def array_identifier(value, size):
    require(isinstance(value, list) and len(value) == size
            and all(type(byte) is int and 0 <= byte <= 255 for byte in value),
            'invalid managed fence identity bytes')
    return identifier(bytes(value).hex(), size)


def physical_identity(value):
    require(isinstance(value, dict) and set(value) == {'node', 'disk', 'physical_host'},
            'invalid physical writer identity')
    return {key: identifier(value[key], 16) for key in ('node', 'disk', 'physical_host')}


def history_key(catalog, namespace, group, incarnation):
    return '/'.join([identifier(catalog, 32), identifier(namespace, 32),
                     identifier(group, 16), identifier(incarnation, 16)])


def challenge(expected, encoded):
    """Check the exact SDK wire record and derive identities from its bound body."""
    identifier(expected, 32)
    require(isinstance(encoded, bytes) and len(encoded) <= CHALLENGE_LIMIT,
            'managed fence challenge exceeds its bound')
    value = decode_json(encoded)
    require(isinstance(value, dict) and set(value) == {
        'format', 'request_digest', 'directory_digest', 'intent'}
        and type(value['format']) is int and value['format'] == 1,
        'invalid managed fence challenge format')
    require(value['request_digest'] == expected, 'different managed fence request')
    directory = bytes.fromhex(identifier(value['directory_digest'], 32))
    wire = value['intent']
    require(isinstance(wire, str) and len(wire) <= 2 * INTENT_LIMIT
            and len(wire) % 2 == 0 and re.fullmatch('[0-9a-f]+', wire),
            'invalid managed intent encoding')
    wire = bytes.fromhex(wire)
    require(len(wire) >= 45 and wire[:9] == b'CLRCAT01' + bytes([222])
            and int.from_bytes(wire[9:13], 'big') + 45 == len(wire)
            and hashlib.sha256(wire[:-32]).digest() == wire[-32:],
            'invalid managed intent framing or checksum')
    intent_digest = hashlib.sha256(wire).digest()
    require(hashlib.sha256(FENCE_DOMAIN + intent_digest + directory).hexdigest() == expected,
            'managed intent or directory does not bind the request')
    intent = decode_json(wire[13:-32])
    require(isinstance(intent, dict) and set(intent) == {
        'catalog', 'namespace', 'operation', 'principal', 'basis', 'holds', 'lineages'},
        'invalid managed intent fields')
    catalog, namespace = array_identifier(intent['catalog'], 32), array_identifier(intent['namespace'], 32)
    require(isinstance(intent['lineages'], list) and intent['lineages'], 'missing managed voter lineages')
    histories, writers, exact = set(), set(), set()
    for row in intent['lineages']:
        require(isinstance(row, dict) and set(row) == {'group', 'incarnation', 'member', 'node', 'disk', 'host'},
                'invalid managed voter lineage')
        require(type(row['member']) is int and 1 <= row['member'] < 2**64, 'invalid managed voter member')
        history = history_key(catalog, namespace, array_identifier(row['group'], 16),
                              array_identifier(row['incarnation'], 16))
        writer = tuple(array_identifier(row[key], 16) for key in ('node', 'disk', 'host'))
        lineage = (history, row['member'], *writer)
        require(lineage not in exact, 'duplicate managed voter lineage')
        exact.add(lineage)
        histories.add(history)
        writers.add(writer)
    return {'intent_digest': intent_digest.hex(), 'directory_digest': directory.hex(),
            'histories': sorted(histories), 'writers': [list(writer) for writer in sorted(writers)]}


class ManagedLauncher(Launcher):
    """One private format-1 ledger; managed receipts never accept a manual roster.

    Group assignments must be declared before their physical deployment starts.
    Every historical declaration matching a requested history is selected, even
    if all of its voters were removed before the archived cut. New incarnations
    use distinct histories; declaring an old history under a new generation is
    permanently refused. The signing key stays outside voters and operators.
    """
    ledger_name = 'managed-launcher.json'

    def initial_state(self):
        return {**super().initial_state(), 'deployments': {}, 'requests': {}, 'revoked_histories': []}

    def read(self):
        value = super().read()
        require(set(value) == {'format', 'run_id', 'revoked', 'nodes', 'deployments',
                               'requests', 'revoked_histories'}, 'invalid managed launch ledger')
        return value

    def declare(self, generation, catalog, namespace, histories):
        require(type(generation) is int and generation >= 0, 'invalid deployment generation')
        require(isinstance(histories, list) and histories, 'empty managed deployment')
        keys = []
        for history in histories:
            require(isinstance(history, dict) and set(history) == {'group', 'incarnation'},
                    'invalid deployment history')
            keys.append(history_key(catalog, namespace, history['group'], history['incarnation']))
        require(len(set(keys)) == len(keys), 'duplicate deployment history')
        keys.sort()
        with self.lock():
            value = self.read()
            require(generation not in value['revoked']
                    and not set(keys).intersection(value['revoked_histories']),
                    'managed history permanently revoked')
            old = value['deployments'].get(str(generation))
            require(old is None or old == keys, 'different managed deployment declaration')
            value['deployments'][str(generation)] = keys
            save(self.path, value)

    def launch(self, generation, name, volume, invoke, *, identity):
        identity = physical_identity(identity)
        with self.lock():
            value = self.read()
            histories = value['deployments'].get(str(generation))
            require(histories is not None, 'managed deployment was not declared')
            require(not set(histories).intersection(value['revoked_histories']),
                    'managed history permanently revoked')
            return self._launch_locked(value, generation, name, volume, invoke, identity)

    def revoke(self, generation):
        raise ValueError('managed fencing requires the canonical SDK challenge')

    def revoke_component(self, minimum):
        raise ValueError('managed fencing requires the canonical SDK challenge')

    def _nodes_for(self, value, request):
        generations = request['generations']
        covered = {history for generation in generations
                   for history in value['deployments'][str(generation)]}
        require(set(request['histories']).issubset(covered), 'incomplete managed deployment inventory')
        nonempty = {generation: 1 for generation in generations
                    if any(row['generation'] == generation for row in value['nodes'])}
        require(nonempty, 'incomplete managed writer inventory')
        nodes = self.inventory(value, nonempty)
        known = set()
        for row in nodes:
            identity = physical_identity(row['identity'])
            known.add(tuple(identity[key] for key in ('node', 'disk', 'physical_host')))
        require(all(tuple(writer) in known for writer in request['writers']),
                'incomplete managed physical writer inventory')
        return nodes

    def bind(self, expected, encoded):
        """Persist permanent revocation before resolving minimum writer bindings."""
        request = challenge(expected, encoded)
        with self.lock():
            value = self.read()
            request['generations'] = sorted(int(generation) for generation, histories
                                            in value['deployments'].items()
                                            if set(histories).intersection(request['histories']))
            previous = value['requests'].get(expected)
            require(previous is None or previous == request, 'different managed fence binding')
            value['requests'][expected] = request
            value['revoked'] = sorted(set(value['revoked']).union(request['generations']))
            value['revoked_histories'] = sorted(set(value['revoked_histories']).union(request['histories']))
            save(self.path, value)
            return [(row['container_id'], row['volume']) for row in self._nodes_for(value, request)]

    def _bound_request(self, value, expected):
        identifier(expected, 32)
        request = value['requests'].get(expected)
        require(request is not None and set(request['generations']).issubset(value['revoked'])
                and set(request['histories']).issubset(value['revoked_histories']),
                'managed fence request was not durably bound')
        return request

    def fence(self, expected, docker):
        """Resume the exact recorded operation; unknown launches cannot be erased by guess."""
        with self.lock():
            value = self.read()
            request = self._bound_request(value, expected)
            nodes = self._nodes_for(value, request)
            return fence_and_delete(docker, self.run_id,
                                    [(row['container_id'], row['volume']) for row in nodes])

    def certificate_payload(self, expected, docker):
        with self.lock():
            value = self.read()
            request = self._bound_request(value, expected)
            nodes = self._nodes_for(value, request)
            containers = set(docker('ps', '-aq', '--no-trunc').split())
            volumes = set(docker('volume', 'ls', '--format', '{{.Name}}').split())
            require(all(row['container_id'] not in containers and row['volume'] not in volumes
                        for row in nodes), 'old managed writer or disk still exists')
            inventory = {'format': 1, 'run_id': self.run_id, 'request': request,
                         'deployments': {str(g): value['deployments'][str(g)] for g in request['generations']},
                         'nodes': sorted(nodes, key=lambda row: (row['generation'], row['name']))}
            commitment = hashlib.sha256(json.dumps(inventory, sort_keys=True,
                                                   separators=(',', ':')).encode()).digest()
            return bytes([1]) + bytes.fromhex(expected) + commitment


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Validate an SDK managed fence challenge; this does not fence hosts.')
    parser.add_argument('--expected-digest', required=True)
    args = parser.parse_args()
    try:
        result = challenge(args.expected_digest, sys.stdin.buffer.read(CHALLENGE_LIMIT + 1))
        print(json.dumps(result))
    except (ValueError, KeyError, TypeError) as error:
        parser.exit(1, 'ERROR: ' + str(error) + '\n')
