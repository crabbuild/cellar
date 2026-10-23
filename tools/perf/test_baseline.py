"""Native-baseline contract, history and ambiguity refusal regressions."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock
from baseline_analyze import proof_delta
from baseline_history import reconcile, position
from baseline_run import effective_profile
from compare_runs import compare_verified, contract, normalized
from raft_run import read_json, verify_image
from raft_workload import Ledger, input_digest, model, run_load


class Profiles(unittest.TestCase):
    def source(self):
        return read_json(Path(__file__).with_name('raft_profiles.json'))['profiles']['local-q0']

    def test_effective_profile_preserves_load_and_declares_changed_contract(self):
        source = self.source()
        before = copy.deepcopy(source)
        profile = effective_profile(source, 'fleet')
        self.assertEqual(source, before)
        self.assertEqual(profile['durability'], 'fleet-or-bucket')
        self.assertEqual(profile['read_consistency'], 'owner')
        self.assertEqual(profile['topology']['voters'], 0)
        for key in ('arrival', 'write_bytes', 'database_bytes', 'cells', 'resources', 'seed'):
            self.assertEqual(profile[key], source[key])
        self.assertEqual(effective_profile(source, 'bucket')['durability'], 'bucket')

    def test_unsupported_density_soak_and_receipt_capacity_are_refused(self):
        for changes in ({'cells': {'total': 4097, 'active': 1}},
                        {'timing': {**self.source()['timing'], 'soak_seconds': 1}},
                        {'timing': {**self.source()['timing'], 'retry_window_ms': 300001}},
                        {'arrival': {**self.source()['arrival'], 'rate_per_second': 1000}}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                effective_profile({**self.source(), **changes}, 'fleet')

    def test_baseline_verifies_its_own_executable(self):
        digest = 'a' * 64
        docker = Mock(side_effect=['sha256:' + digest, digest + '  crates/new.rs\n', 'b' * 64 + '  /usr/local/bin/cellar-perf-baseline'])
        verify_image('sha256:' + digest, docker, {'crates/new.rs': digest}, binary_name='cellar-perf-baseline')
        self.assertEqual(docker.call_args.args[-1], '/usr/local/bin/cellar-perf-baseline')


class History(unittest.TestCase):
    def events(self):
        seed = {'kind': 'initialize', 'database_bytes': 4096}
        state, _ = model(None, seed)
        identity = '1' * 32 + ':1'
        reply = {'state': state, 'changed': True, 'sequence': 1, 'scope': 'run:/cells/0',
                 'command_id': identity, 'owner': 'node1', 'durability': 'bucket', 'receipt_kind': 'application-transaction'}
        return [{'event': 'seed_invoke', 'cell': 0, 'input': seed, 'input_sha256': input_digest(seed), 'command_id': identity, 'retry_until_ms': 100},
                {'event': 'seed_complete', 'cell': 0, 'status': 200, 'response': reply}]

    def check(self, events):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'ledger'
            with path.open('w') as stream:
                for order, event in enumerate(events, 1):
                    stream.write(json.dumps({**event, 'order': order}) + '\n')
            return reconcile(path, 'run', 'bucket')

    def test_app_receipt_never_claims_a_raft_proof(self):
        self.assertEqual(self.check(self.events())['commands_including_seed'], 1)
        for key, value in [('index', 3), ('version_token', 'fake'), ('group', 'fake'), ('incarnation', 'fake'), ('term', 1),
                           ('sequence', 2), ('scope', 'other:/cells/0'), ('receipt_kind', 'raft-command'), ('durability', 's3')]:
            events = self.events()
            events[-1]['response'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.check(events)

    def test_failed_mutation_cannot_be_declared_a_safe_rejection(self):
        events = self.events()
        change = {'kind': 'set', 'value': 123, 'expected_revision': 0, 'payload': 'value'}
        events.extend([{'event': 'invoke', 'cell': 0, 'id': 'm:0', 'operation': 'command', 'input': change,
                        'input_sha256': input_digest(change), 'command_id': '1' * 32 + ':2'},
                       {'event': 'complete', 'cell': 0, 'id': 'm:0', 'operation': 'command', 'status': 500, 'outcome': 'rejected'}])
        with self.assertRaisesRegex(ValueError, 'not proven'):
            self.check(events)

    def test_owner_read_checks_receipt_sequence_even_for_no_op(self):
        events = self.events()
        query = {'consistency': 'owner'}
        reply = {'state': events[-1]['response']['state'], 'scope': 'run:/cells/0', 'sequence': 1,
                 'owner': 'node2', 'read_contract': 'owner'}
        events.extend([{'event': 'invoke', 'cell': 0, 'id': 'm:0', 'operation': 'query', 'input': query, 'input_sha256': input_digest(query)},
                       {'event': 'complete', 'cell': 0, 'id': 'm:0', 'operation': 'query', 'status': 200, 'outcome': 'completed', 'response': reply}])
        self.assertEqual(self.check(events)['queries'], 1)
        reply['sequence'] = 0
        with self.assertRaisesRegex(ValueError, 'preceding acknowledgement'):
            self.check(events)

    def test_shared_scheduler_records_baseline_without_raft_tokens(self):
        source = read_json(Path(__file__).with_name('raft_profiles.json'))['profiles']['local-q0']
        source['cells'] = {'total': 1, 'active': 1}
        source['read_fraction'] = 0
        profile = effective_profile(source, 'bucket')
        initial = self.events()[-1]['response']['state']
        state = {0: {'state': initial, 'next_sequence': 2, 'sequence': 1}}
        def send(cell, kind, body, headers):
            expected, changed = model(state[cell]['state'], body)
            return 200, {'state': expected, 'changed': changed, 'durability': 'bucket', 'sequence': state[cell]['sequence'] + 1}
        with tempfile.TemporaryDirectory() as directory:
            ledger = Ledger(Path(directory) / 'ledger')
            result = run_load(profile, 'measurement', .01, state, ledger, send, 'run', position=position)
            ledger.close()
        self.assertEqual(result['completed'], 1)
        self.assertEqual(state[0]['sequence'], 2)
        self.assertNotIn('version_token', state[0])


class Comparisons(unittest.TestCase):
    def test_scenario_and_fault_instrumentation_are_comparison_inputs(self):
        profile = read_json(Path(__file__).with_name('raft_profiles.json'))['profiles']['local-q0']
        report = {'profile': profile, 'scenario': 'healthy', 'fault_profile': None, 'vm': {}, 'limits': [],
                  'build': {'build_profile': 'lab', 'source_manifest_sha256': 'source', 'binary_sha256': 'binary'}}
        verified = {'verified': True, 'summary': {'completed_per_second': 10}}
        left = normalized(report, verified)
        for changes in ({'scenario': 'slow-follower'}, {'fault_profile': {'node': 3}}):
            result = compare_verified(left, normalized({**report, **changes}, verified))
            self.assertFalse(result['matched_contract'])
            self.assertIn('workload_contract', result['differences'])
            self.assertNotIn('throughput_ratio', result)

    def test_quorum_s3_fleet_and_bucket_contracts_are_distinct(self):
        contracts = [contract({'adapter': mode, 'profile': {'durability': durability, 'read_consistency': 'at-least'}})
                     for mode, durability in [('raft', 'quorum'), ('raft', 's3'), ('fleet', 'fleet-or-bucket'), ('bucket', 'bucket')]]
        self.assertEqual(len({json.dumps(c, sort_keys=True) for c in contracts}), 4)
        for left in contracts:
            for right in contracts:
                if left == right:
                    continue
                a = {'durability_contract': left, 'hardware': {}, 'workload_contract': {}, 'build_profile': {}}
                b = {**a, 'durability_contract': right}
                result = compare_verified(a, b)
                self.assertFalse(result['matched_contract'])
                self.assertNotIn('throughput_ratio', result)

    def test_proof_sources_are_observed_and_cannot_reset(self):
        before = [{'node': n, 'sample': {'output_proofs': {key: 0 for key in ('started', 'bucket', 'fleet', 'errors', 'elapsed_us')}}} for n in range(1, 4)]
        after = copy.deepcopy(before)
        after[0]['sample']['output_proofs'].update(started=1, bucket=1, elapsed_us=100)
        self.assertEqual(proof_delta({'before': before, 'after': after}, 'bucket')['bucket'], 1)
        after[0]['sample']['output_proofs']['fleet'] = 1
        with self.assertRaises(ValueError):
            proof_delta({'before': before, 'after': after}, 'bucket')
        with self.assertRaises(ValueError):
            proof_delta({'before': after, 'after': before}, 'fleet')


if __name__ == '__main__':
    unittest.main()
