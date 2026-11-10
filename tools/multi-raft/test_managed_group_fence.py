# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Managed request binding, including writers absent from the SDK minimum."""
import hashlib
import json
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest

# The qualification entry point installs the perf helpers' import path.
import group_disaster
from managed_group_fence import FENCE_DOMAIN, ManagedLauncher, challenge
from test_group_disaster import DockerInventory


def word(value, size=16):
    return bytes([value] * size).hex()


def identity(value):
    return {'node': word(value), 'disk': word(value + 20), 'physical_host': word(value + 40)}


def history(value):
    return {'group': word(value), 'incarnation': word(value + 1)}


def request(lineages=None, directory=70):
    if lineages is None:
        lineages = [lineage(node, 10 if node < 7 else 12) for node in range(4, 10)]
    body = {'catalog': [1]*32, 'namespace': [2]*32, 'operation': [3]*16,
            'principal': 'operator', 'basis': {}, 'holds': [], 'lineages': lineages}
    payload = json.dumps(body, separators=(',', ':')).encode()
    wire = b'CLRCAT01' + bytes([222]) + len(payload).to_bytes(4, 'big') + payload
    wire += hashlib.sha256(wire).digest()
    barrier = bytes([directory]*32)
    digest = hashlib.sha256(FENCE_DOMAIN + hashlib.sha256(wire).digest() + barrier).hexdigest()
    encoded = json.dumps({'format': 1, 'request_digest': digest,
                          'directory_digest': barrier.hex(), 'intent': wire.hex()}).encode()
    return digest, encoded


def lineage(node, group):
    who = identity(node)
    return {'group': [group]*16, 'incarnation': [group + 1]*16, 'member': node,
            'node': list(bytes.fromhex(who['node'])), 'disk': list(bytes.fromhex(who['disk'])),
            'host': list(bytes.fromhex(who['physical_host']))}


class ManagedFenceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.controller = ManagedLauncher(self.root, 'owned')

    def declare(self, generation, value):
        self.controller.declare(generation, word(1, 32), word(2, 32), [history(value)])

    def launch(self, generation, node):
        return (self.controller.launch(generation, f'node-{node}', f'disk-{node}',
                                       lambda: f'{node:064x}', identity=identity(node)), f'disk-{node}')

    def populated(self):
        # All three voters of deployment 0 were removed from the current roster.
        # They are nevertheless possible old writers for the very same history.
        self.declare(0, 10)
        self.declare(1, 10)
        self.declare(2, 12)
        return [self.launch(0 if node < 4 else 1 if node < 7 else 2, node)
                for node in range(1, 10)]

    def test_all_historical_deployments_not_only_minimum_voters_are_selected(self):
        old = self.populated()
        digest, encoded = request()
        self.assertEqual(self.controller.bind(digest, encoded), old)
        reloaded = ManagedLauncher(self.root, 'owned')
        self.assertEqual(reloaded.bind(digest, encoded), old)
        self.assertEqual(reloaded.read()['revoked'], [0, 1, 2])
        for identifier, _ in old:
            with self.assertRaisesRegex(ValueError, 'deployment permanently revoked'):
                reloaded.restart(identifier, lambda: self.fail('old restart admitted'))
        with self.assertRaisesRegex(ValueError, 'managed history permanently revoked'):
            self.declare(3, 10)

    def test_changed_lineage_with_repaired_checksum_cannot_keep_the_request(self):
        digest, encoded = request()
        outer = json.loads(encoded)
        wire = bytes.fromhex(outer['intent'])
        body = json.loads(wire[13:-32])
        body['lineages'][0]['disk'] = [99]*16
        payload = json.dumps(body, separators=(',', ':')).encode()
        wire = b'CLRCAT01' + bytes([222]) + len(payload).to_bytes(4, 'big') + payload
        wire += hashlib.sha256(wire).digest()
        outer['intent'] = wire.hex()
        with self.assertRaisesRegex(ValueError, 'does not bind the request'):
            self.controller.bind(digest, json.dumps(outer).encode())
        self.assertEqual(self.controller.read()['requests'], {})

    def test_changed_directory_and_foreign_expected_digest_refuse(self):
        digest, encoded = request()
        changed = json.loads(encoded)
        changed['directory_digest'] = word(71, 32)
        with self.assertRaisesRegex(ValueError, 'does not bind the request'):
            challenge(digest, json.dumps(changed).encode())
        with self.assertRaisesRegex(ValueError, 'different managed fence request'):
            challenge(word(72, 32), encoded)

    def test_bad_frame_duplicate_fields_and_duplicate_lineages_refuse(self):
        digest, encoded = request()
        outer = json.loads(encoded)
        outer['intent'] = '00' + outer['intent'][2:]
        with self.assertRaisesRegex(ValueError, 'framing or checksum'):
            challenge(digest, json.dumps(outer).encode())
        with self.assertRaises(ValueError):
            challenge(digest, b'{"format":1,"format":1}')
        digest, encoded = request([lineage(4, 10), lineage(4, 10)])
        with self.assertRaisesRegex(ValueError, 'duplicate managed voter lineage'):
            challenge(digest, encoded)

    def test_missing_deployment_closes_admission_for_every_requested_history(self):
        self.declare(0, 10)
        self.launch(0, 4)
        digest, encoded = request()
        with self.assertRaisesRegex(ValueError, 'incomplete managed deployment inventory'):
            self.controller.bind(digest, encoded)
        reloaded = ManagedLauncher(self.root, 'owned')
        self.assertEqual(reloaded.read()['revoked'], [0])
        with self.assertRaisesRegex(ValueError, 'managed history permanently revoked'):
            reloaded.declare(8, word(1, 32), word(2, 32), [history(12)])
        with self.assertRaisesRegex(ValueError, 'incomplete managed deployment inventory'):
            reloaded.certificate_payload(digest, DockerInventory([]))

    def test_wrong_disk_or_host_cannot_substitute_for_the_minimum_writer(self):
        for field in ('disk', 'physical_host'):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as root:
                controller = ManagedLauncher(root, 'owned')
                controller.declare(0, word(1, 32), word(2, 32), [history(10)])
                who = identity(4)
                who[field] = word(100)
                controller.launch(0, 'node', 'disk', lambda: 'a'*64, identity=who)
                digest, encoded = request([lineage(4, 10)])
                with self.assertRaisesRegex(ValueError, 'incomplete managed physical writer inventory'):
                    controller.bind(digest, encoded)
                self.assertEqual(controller.read()['revoked'], [0])

    def test_unknown_historical_launch_prevents_certification(self):
        self.populated()
        with self.assertRaisesRegex(RuntimeError, 'lost create reply'):
            self.controller.launch(0, 'uncertain', 'uncertain-disk',
                                   lambda: (_ for _ in ()).throw(RuntimeError('lost create reply')),
                                   identity=identity(11))
        digest, encoded = request()
        with self.assertRaisesRegex(ValueError, 'incomplete writer inventory'):
            self.controller.bind(digest, encoded)
        with self.assertRaisesRegex(ValueError, 'incomplete writer inventory'):
            ManagedLauncher(self.root, 'owned').certificate_payload(digest, DockerInventory([]))

    def test_partial_deletion_resumes_and_payload_is_stable_across_fresh_deployment(self):
        old = self.populated()
        digest, encoded = request()
        self.controller.bind(digest, encoded)
        docker = DockerInventory(old)
        with self.assertRaisesRegex(ValueError, 'old managed writer or disk still exists'):
            self.controller.certificate_payload(digest, docker)
        docker.lost_delete_reply = old[0][0]
        with self.assertRaisesRegex(RuntimeError, 'lost remove reply'):
            self.controller.fence(digest, docker)
        reloaded = ManagedLauncher(self.root, 'owned')
        self.assertEqual(len(reloaded.fence(digest, docker)), 9)
        payload = reloaded.certificate_payload(digest, docker)
        self.assertEqual(payload[:33], bytes([1]) + bytes.fromhex(digest))
        self.assertEqual(len(payload), 65)
        self.declare(3, 80)
        fresh = self.launch(3, 10)
        self.assertEqual(reloaded.restart(fresh[0], lambda: 'started'), 'started')
        self.assertEqual(reloaded.certificate_payload(digest, docker), payload)

    def test_manual_rosters_and_undeclared_launches_are_not_managed_fences(self):
        with self.assertRaisesRegex(ValueError, 'not declared'):
            self.launch(0, 1)
        for method in (self.controller.revoke, self.controller.revoke_component):
            with self.assertRaisesRegex(ValueError, 'canonical SDK challenge'):
                method({0: 3})
        with self.assertRaisesRegex(ValueError, 'not durably bound'):
            self.controller.certificate_payload(word(1, 32), DockerInventory([]))

    def test_wire_cli_checks_expected_digest_before_returning_the_minimum(self):
        digest, encoded = request()
        cli = Path(__file__).with_name('managed_group_fence.py')
        accepted = subprocess.run([sys.executable, str(cli), '--expected-digest', digest],
                                  input=encoded, capture_output=True, check=True)
        self.assertEqual(json.loads(accepted.stdout), challenge(digest, encoded))
        refused = subprocess.run([sys.executable, str(cli), '--expected-digest', word(90, 32)],
                                 input=encoded, capture_output=True)
        self.assertNotEqual(refused.returncode, 0)
        self.assertEqual(refused.stdout, b'')


if __name__ == '__main__':
    unittest.main()
