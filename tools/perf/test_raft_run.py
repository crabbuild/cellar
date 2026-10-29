# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Refusal, arrival accounting and independent workload oracle regressions."""
import contextlib
import concurrent.futures
import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from raft_run import (capacity, main, matched_comparison, parse_manifest, read_json,
                      summarize_ledger, validate_profile, verify_image)
from raft_workload import Ledger, input_digest, model, run_load


class Profiles(unittest.TestCase):
    def profile(self):
        return read_json(Path(__file__).with_name('raft_profiles.json'))['profiles']['local-q0']

    def test_q1_preserves_the_real_capacity_gap(self):
        catalog = read_json(Path(__file__).with_name('raft_profiles.json'))
        result = capacity(validate_profile(catalog['profiles']['q1-capacity-plan']))
        self.assertEqual(result['average_replicas_per_node'], 5000)
        self.assertEqual(result['minimum_peak_with_20_percent_repair_reserve'], 6000)
        self.assertFalse(result['host_bound_admits_balanced_placement'])
        self.assertFalse(result['density_qualified'])

    def test_density_ladder_changes_only_live_cell_population(self):
        catalog = read_json(Path(__file__).with_name('raft_profiles.json'))['profiles']
        from raft_topology import local_placement
        for cells in (500, 1500):
            profile = validate_profile(catalog['local-density-' + str(cells)])
            expected = copy.deepcopy(catalog['local-five'])
            expected['cells']['total'] = cells
            expected['qualification'] = profile['qualification']
            self.assertEqual(profile, expected)
            self.assertEqual({r['replica_cells'] for r in local_placement(profile)}, {cells * 3 // 5})
            self.assertFalse(capacity(profile)['density_qualified'])

    def test_missing_and_unknown_workload_fields_are_refused(self):
        for key in self.profile():
            profile = self.profile()
            del profile[key]
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_profile(profile)
        profile = self.profile()
        profile['bucket'] = 'production'
        with self.assertRaises(ValueError):
            validate_profile(profile)

    def test_nonnumeric_invalid_bounds_and_unknown_nested_fields_are_refused(self):
        for path, value in [(('arrival', 'rate_per_second'), float('nan')),
                            (('arrival', 'max_inflight'), 0),
                            (('cells', 'active'), 13),
                            (('resources', 'node_memory_bytes'), 0),
                            (('topology', 'nodes'), True),
                            (('timing', 'request_timeout_ms'), 5001),
                            (('slo', 'max_error_fraction'), 2)]:
            profile = self.profile()
            profile[path[0]][path[1]] = value
            with self.subTest(path=path), self.assertRaises(ValueError):
                validate_profile(profile)
        profile = self.profile()
        profile['arrival']['queue'] = 1000000000
        with self.assertRaises(ValueError):
            validate_profile(profile)

    def test_unavailable_fault_capability_refuses_before_docker_or_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'run'
            for scenario in ('leader-loss', 'minority-partition', 'disk-stall', 'snapshot-crash'):
                args = ['raft_run.py', '--profile', str(Path(__file__).with_name('raft_profiles.json')),
                        '--scenario', scenario, '--output', str(output)]
                with self.subTest(scenario=scenario), patch('sys.argv', args), patch('raft_run.subprocess.run') as docker:
                    with self.assertRaisesRegex(ValueError, 'fault capability'):
                        main()
                    docker.assert_not_called()
                    self.assertFalse(output.exists())

    def test_slow_follower_requires_a_checked_fault_profile_before_resources(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'run'
            args = ['raft_run.py', '--profile', str(Path(__file__).with_name('raft_profiles.json')),
                    '--scenario', 'slow-follower', '--output', str(output)]
            with patch('sys.argv', args), patch('raft_run.subprocess.run') as docker:
                with self.assertRaisesRegex(ValueError, 'requires a fault profile'):
                    main()
                docker.assert_not_called()
                self.assertFalse(output.exists())

    def test_local_placement_enforces_actual_peak_and_the_five_node_ceiling(self):
        from raft_topology import local_placement, members, placement, vm_requirements
        self.assertEqual(members(4, 5), (5, 1, 2))
        self.assertEqual([row['replica_cells'] for row in placement(3, 5)], [1, 2, 3, 2, 1])
        for nodes in (3, 4, 5):
            profile = self.profile()
            profile['topology']['nodes'] = nodes
            profile['cells'] = {'total': nodes * 7, 'active': 1}
            self.assertTrue(all(row['replica_cells'] == 21 and row['leader_cells'] == 7 for row in local_placement(profile)))
            self.assertEqual(vm_requirements(nodes)['cpus'], nodes + 3)
        for nodes, cells in ((2, 12), (6, 12), (5, 1), (5, 6827), (3, 4097)):
            profile = self.profile()
            profile['topology']['nodes'] = nodes
            profile['cells'] = {'total': cells, 'active': 1}
            with self.subTest(nodes=nodes, cells=cells), self.assertRaises(ValueError):
                local_placement(profile)
        profile = self.profile()
        profile['topology']['nodes'] = 5
        profile['cells'] = {'total': 6826, 'active': 1}
        self.assertEqual(max(row['replica_cells'] for row in local_placement(profile)), 4096)
        for nodes in (2, 6, True):
            with self.assertRaises(ValueError):
                members(0, nodes)


class SeedDiagnostics(unittest.TestCase):
    def test_failed_seed_inspects_all_assigned_peers_and_preserves_partial_errors(self):
        from raft_run import observe_failed_seed
        bases = ['node1', 'node2', 'node3', 'node4', 'node5']
        nodes = bases + ['store']
        with patch('raft_run.request', side_effect=[TimeoutError('private context'), (200, {'leader': None}), (200, {'leader': 2})]) as request, \
             patch('raft_run.capture_kernel', side_effect=[ValueError('private context')] + [[{'node': node}] for node in nodes[1:]]) as kernel:
            result = observe_failed_seed(781, bases, 'private-token', Mock(), nodes, 'owner')
        self.assertEqual([row['node'] for row in result['replicas']], [2, 3, 4])
        self.assertEqual(result['replicas'][0]['error_type'], 'TimeoutError')
        self.assertEqual(result['kernel'][0]['error_type'], 'ValueError')
        self.assertEqual(len(result['kernel']), 6)
        self.assertNotIn('private', json.dumps(result))
        self.assertEqual(kernel.call_count, 6)
        for call, base in zip(request.call_args_list, bases[1:4]):
            self.assertEqual(call.args, (base, '/admin/781', 'private-token'))
            self.assertEqual(call.kwargs, {'timeout': .5})



class Freshness(unittest.TestCase):
    def test_harness_rejects_stale_binary(self):
        digest = 'a' * 64
        docker = Mock(side_effect=['sha256:' + digest, digest + '  crates/old.rs\n'])
        with self.assertRaisesRegex(ValueError, 'stale image'):
            verify_image('sha256:' + digest, docker, {'crates/new.rs': digest})
        self.assertEqual(docker.call_count, 2)
        self.assertTrue(all('volume' not in call.args and 'create' not in call.args for call in docker.call_args_list))

    def test_tag_is_not_an_immutable_executable_identity(self):
        docker = Mock()
        with self.assertRaisesRegex(ValueError, 'immutable'):
            verify_image('cellar:latest', docker, {})
        docker.assert_not_called()

    def test_equal_source_and_recorded_binary_hash_pass(self):
        digest = 'a' * 64
        binary = 'b' * 64
        docker = Mock(side_effect=['sha256:' + digest, digest + '  crates/new.rs\n', binary + '  /usr/local/bin/cellar-perf-raft'])
        self.assertEqual(verify_image('sha256:' + digest, docker, {'crates/new.rs': digest})['binary_sha256'], binary)

    def test_embedded_and_generated_inputs_participate_in_freshness(self):
        from raft_run import source_manifest
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ('Cargo.toml', 'Cargo.lock', 'crates/app/schema.sql',
                         'vendor/parser/wire.proto', 'examples/app/native.c',
                         'tools/perf/driver.py', 'tools/xtask/template.html'):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('original')
            before = source_manifest(root)
            (root / 'crates/app/schema.sql').write_text('changed schema')
            after_sql = source_manifest(root)
            self.assertNotEqual(before, after_sql)
            (root / 'vendor/parser/wire.proto').write_text('changed protocol')
            self.assertNotEqual(after_sql, source_manifest(root))
            self.assertIn('examples/app/native.c', before)
            self.assertIn('tools/xtask/template.html', before)
            self.assertIn('tools/perf/driver.py', before)
            cache = root / 'tools/perf/__pycache__/generated.pyc'
            cache.parent.mkdir()
            cache.write_bytes(b'cache')
            self.assertNotIn('tools/perf/__pycache__/generated.pyc', source_manifest(root))

    def test_duplicate_manifest_path_is_refused(self):
        with self.assertRaises(ValueError):
            parse_manifest(('a' * 64 + '  crates/source.rs\n') * 2)


class Comparison(unittest.TestCase):
    def report(self):
        return {'durability_contract': {'replicas_before_ack': 2, 's3_before_ack': False},
                'hardware': {'node_cpus': 1, 'nodes': 3}, 'workload_contract': {'reads': .5},
                'build_profile': 'lab', 'reconciled': True, 'completed_per_second': 10}

    def test_matching_contract_is_required_for_speedup(self):
        left = self.report()
        right = copy.deepcopy(left)
        right['completed_per_second'] = 15
        self.assertEqual(matched_comparison(left, right)['throughput_ratio'], 1.5)
        for key in ('durability_contract', 'hardware', 'workload_contract', 'build_profile', 'reconciled'):
            changed = copy.deepcopy(right)
            changed[key] = None
            with self.subTest(key=key), self.assertRaises(ValueError):
                matched_comparison(left, changed)
        right['durability_contract']['s3_before_ack'] = True
        with self.assertRaisesRegex(ValueError, 'durability_contract'):
            matched_comparison(left, right)


class OfferedLoad(unittest.TestCase):
    def profile(self):
        profile = read_json(Path(__file__).with_name('raft_profiles.json'))['profiles']['local-q0']
        profile['cells'] = {'total': 1, 'active': 1}
        profile['read_fraction'] = 0
        profile['arrival'] = {'rate_per_second': 100, 'max_inflight': 2, 'max_lateness_ms': 1000}
        return profile

    def state(self):
        initial, _ = model(None, {'kind': 'initialize'})
        return {0: {'state': initial, 'next_sequence': 2, 'version_token': 'seed', 'index': 3}}

    def test_slow_server_does_not_reduce_or_hide_offered_arrivals(self):
        state = self.state()
        observed = []
        def send(cell, kind, body, headers):
            observed.append(headers['command_id'])
            time.sleep(.05)
            next_state, changed = model(state[cell]['state'], body)
            return 200, {'state': next_state, 'changed': changed, 'durability': 'quorum',
                         'version_token': 'new', 'index': state[cell]['index'] + 1}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'ledger.jsonl'
            ledger = Ledger(path)
            result = run_load(self.profile(), 'measurement', .12, state, ledger, send, 'test')
            ledger.close()
            self.assertEqual(result['offered'], 12)
            self.assertGreater(result['generator_rejected'], 0)
            self.assertEqual(result['completed'], len(observed))
            self.assertEqual([int(identity.split(':')[1]) for identity in observed], list(range(2, 2+len(observed))))
            self.assertEqual(len({identity.split(':')[0] for identity in observed}), 1)
            summary = summarize_ledger(path, 'measurement', .12)
            self.assertEqual(summary['counts']['offered'], 12)
            self.assertEqual(summary['offered_per_second'], 100)
            self.assertGreater(summary['error_fraction'], 0)

    def test_lost_response_quarantines_the_cell_across_phases(self):
        state = self.state()
        send = Mock(side_effect=TimeoutError)
        with tempfile.TemporaryDirectory() as directory:
            ledger = Ledger(Path(directory) / 'ledger.jsonl')
            first = run_load(self.profile(), 'warmup', .03, state, ledger, send, 'test')
            second = run_load(self.profile(), 'measurement', .03, state, ledger, send, 'test')
            ledger.close()
        self.assertEqual(send.call_count, 1)
        self.assertEqual(first['ambiguous'], 1)
        self.assertEqual(second['submitted'], 0)
        self.assertEqual(second['generator_rejected'], 3)
        self.assertEqual(state[0]['next_sequence'], 2)

    def test_admission_cannot_change_future_offered_cells_or_operation_mix(self):
        profile = self.profile()
        profile['cells'] = {'total': 2, 'active': 2}
        profile['read_fraction'] = .5
        profile['skew'] = {'hot_fraction': .5, 'hot_probability': .5}
        plans = []
        for delay in (0, .04):
            state = {cell: copy.deepcopy(self.state()[0]) for cell in range(2)}
            def send(cell, kind, body, headers):
                time.sleep(delay)
                if kind == 'query':
                    return 200, {'state': state[cell]['state']}
                next_state, changed = model(state[cell]['state'], body)
                return 200, {'state': next_state, 'changed': changed, 'durability': 'quorum',
                    'version_token': 'new', 'index': state[cell]['index']+1}
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'ledger'
                ledger = Ledger(path)
                run_load(profile, 'measurement', .12, state, ledger, send, 'test')
                ledger.close()
                events = [json.loads(line) for line in path.read_text().splitlines()]
                offered = sorted((event for event in events if event['event'] in ('invoke', 'reject')),
                                 key=lambda event: int(event['id'].split(':')[1]))
                plans.append([(event['cell'], event['operation']) for event in offered])
        self.assertEqual(len(plans[0]), 12)
        self.assertEqual(plans[0], plans[1])

    def test_read_timeout_is_counted_without_inventing_mutation_ambiguity(self):
        profile = self.profile()
        profile['read_fraction'] = 1
        state = self.state()
        send = Mock(side_effect=TimeoutError)
        # This case checks completed read accounting. A real worker can still
        # own the cell's completion fsync at the next 10ms arrival, correctly
        # rejecting it as busy. Control completion and time here; the separate
        # slow-server case checks real concurrent admission and offered load.
        now = [0.0]
        def advance(seconds):
            now[0] += seconds
        def submit(work, *args):
            future = concurrent.futures.Future()
            try:
                future.set_result(work(*args))
            except BaseException as error:
                future.set_exception(error)
            return future
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'ledger'
            ledger = Ledger(path)
            with patch('raft_workload.concurrent.futures.ThreadPoolExecutor') as executor:
                executor.return_value.__enter__.return_value.submit.side_effect = submit
                result = run_load(profile, 'measurement', .03, state, ledger, send, 'test',
                                  clock=lambda: now[0], sleep=advance)
            ledger.close()
            summary = summarize_ledger(path, 'measurement', .03)
        self.assertEqual(send.call_count, 3)
        self.assertEqual(result['offered'], 3)
        self.assertEqual(result['submitted'], 3)
        self.assertEqual(result['generator_rejected'], 0)
        self.assertEqual(result['ambiguous'], 0)
        self.assertEqual(result['blocked_cells'], [])
        self.assertEqual(result['failed'], 3)
        self.assertEqual(summary['counts']['timed_out'], 3)
        self.assertEqual(state, self.state())

    def test_command_is_durably_journaled_before_send(self):
        state = self.state()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'ledger.jsonl'
            ledger = Ledger(path)
            def send(cell, kind, body, headers):
                record = json.loads(path.read_text().splitlines()[-1])
                self.assertEqual(record['event'], 'invoke')
                self.assertEqual(record['command_id'], headers['command_id'])
                return 503, {'error': 'Overloaded', 'ambiguous': False}
            result = run_load(self.profile(), 'measurement', .01, state, ledger, send, 'test')
            ledger.close()
        self.assertEqual(result['rejected'], 1)
        self.assertEqual(state[0]['next_sequence'], 2)

    def test_conditional_reference_operations_do_not_commute(self):
        state = self.state()[0]['state']
        first = {'kind': 'set', 'expected_revision': 0, 'value': 10, 'payload': 'a'}
        second = {'kind': 'transfer', 'expected_revision': 1, 'value': 3, 'payload': 'b'}
        after, _ = model(*[model(state, first)[0], second])
        self.assertEqual((after['left'], after['right']), (7, 999993))
        self.assertEqual(model(state, second), (state, False))
        self.assertEqual(after['payload_sha256'], hashlib.sha256(b'b').hexdigest())

class HistoryOracle(unittest.TestCase):
    def events(self):
        from raft_workload import input_digest
        initial = {'kind': 'initialize', 'database_bytes': 0}
        state, _ = model(None, initial)
        change = {'kind': 'set', 'expected_revision': 0, 'value': 7, 'payload': 'abc'}
        next_state, _ = model(state, change)
        scope = {'group': 'group1', 'incarnation': 'incarnation1', 'durability': 'quorum'}
        return [
            {'event': 'seed_invoke', 'cell': 0, 'command_id': 'client:1', 'input': initial, 'input_sha256': input_digest(initial)},
            {'event': 'seed_complete', 'cell': 0, 'status': 200, 'response': {**scope, 'state': state, 'changed': True, 'index': 2, 'version_token': 'v2'}},
            {'event': 'invoke', 'cell': 0, 'phase': 'measurement', 'id': 'm:1', 'operation': 'command', 'command_id': 'client:2', 'input': change, 'input_sha256': input_digest(change)},
            {'event': 'complete', 'cell': 0, 'phase': 'measurement', 'id': 'm:1', 'operation': 'command', 'status': 200, 'outcome': 'completed', 'response': {**scope, 'state': next_state, 'changed': True, 'index': 3, 'version_token': 'v3'}},
            {'event': 'invoke', 'cell': 0, 'phase': 'measurement', 'id': 'm:2', 'operation': 'query', 'min_version': 'v3', 'input': {'consistency': 'at-least'}, 'input_sha256': input_digest({'consistency': 'at-least'})},
            {'event': 'complete', 'cell': 0, 'phase': 'measurement', 'id': 'm:2', 'operation': 'query', 'status': 200, 'outcome': 'completed', 'response': {**scope, 'state': next_state, 'index': 3, 'version_token': 'v3'}}]

    def check(self, events):
        from raft_workload import reconcile
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'ledger'
            path.write_text(''.join(json.dumps({**event, 'order': index}) + '\n' for index, event in enumerate(events, 1)))
            return reconcile(path)

    def test_independent_replay_checks_result_scope_sequence_and_prefix(self):
        self.assertEqual(self.check(self.events())['queries'], 1)
        for name, corrupt in {
            'wrong_value': lambda e: e[3]['response']['state'].update(left=8),
            'wrong_scope': lambda e: e[3]['response'].update(incarnation='other'),
            'wrong_sequence': lambda e: e[2].update(command_id='client:3'),
            'old_read': lambda e: e[5]['response'].update(index=2),
            'weaker_durability': lambda e: e[3]['response'].update(durability='local'),
            'wrong_minimum': lambda e: e[4].update(min_version='v1'),
            'ambiguous': lambda e: e[3].update(outcome='ambiguous'),
        }.items():
            events = self.events()
            corrupt(events)
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.check(events)


class IndependentReport(unittest.TestCase):
    def fixture(self, root, node_count=3):
        from raft_topology import local_placement, members, vm_requirements
        from raft_metrics import phases
        from test_raft_metrics import rows
        total = 1 if node_count == 3 else node_count
        events = []
        for cell in range(total):
            for event in copy.deepcopy(HistoryOracle().events()):
                event['cell'] = cell
                if 'id' in event:
                    event['id'] = str(cell) + ':' + event['id']
                if 'response' in event:
                    event['response']['group'] = 'group-' + str(cell)
                if event['event'] == 'complete':
                    event['latency_ms'] = 10.0
                    event['response']['served_by'] = members(cell, node_count)[1]
                events.append(event)
        path = root / 'operations.jsonl'
        path.write_text(''.join(json.dumps({**event, 'order': index}) + '\n' for index, event in enumerate(events, 1)))
        profile = Profiles().profile()
        profile['topology']['nodes'] = node_count
        profile['cells'] = {'total': total, 'active': total}
        profile['arrival']['rate_per_second'] = total
        profile['timing']['measurement_seconds'] = 2
        planned = local_placement(profile)
        report = {'completed': True, 'reconciled': True, 'profile': profile, 'prefix': 'fixture',
            'run_id': 'owned',
            'scenario': 'healthy', 'fault_profile': None, 'fault_profile_sha256': input_digest(None), 'fault_result': None,
            'profile_sha256': input_digest(profile), 'summary': summarize_ledger(path, 'measurement', 2),
            'measurement': {'offered': 2 * total}, 'final_archive': [[200, {'cell': cell, 'archived': 3}] for cell in range(total)],
            'cgroups': [{'node': f'fixture-node{n}', 'cpu_max': '100000 100000', 'memory_max': 3 * 1024**3, 'swap_max': 0} for n in range(1, node_count + 1)],
            'placement': planned, 'vm': vm_requirements(node_count),
            'limits': [{'NanoCpus': 10**9, 'CpusetCpus': str(n), 'Memory': 3 * 1024**3, 'MemorySwap': 3 * 1024**3, 'Tmpfs': {}} for n in range(node_count)],
            'store_limits': {'NanoCpus': 2 * 10**9, 'CpusetCpus': f'{node_count},{node_count+1}', 'Memory': 4 * 1024**3, 'MemorySwap': 4 * 1024**3, 'Tmpfs': {}},
            'limitations': ['synthetic test evidence']}
        def physical(stamp):
            result = rows(stamp, node_count)
            for sample, node in zip(result, planned):
                sample['sample']['resources']['groups'].update(durable_groups=node['replica_cells'], attached_groups=node['replica_cells'], parked_groups=0)
            return result
        measurements = {'measurement': {'before': physical(3), 'after': physical(4)}}
        if profile['timing']['warmup_seconds']:
            measurements['warmup'] = {'before': physical(1), 'after': physical(2)}
        (root / 'measurements.json').write_text(json.dumps(measurements))
        (root / 'node-samples.jsonl').write_text(json.dumps(physical(4)) + '\n')
        report['performance'] = phases(measurements, bool(profile['timing']['warmup_seconds']), node_count)
        replicas = []
        topology = []
        for cell in range(total):
            assigned = members(cell, node_count)
            response = next(event['response'] for event in reversed(events) if event['cell'] == cell)
            replicas.extend({'cell': cell, 'node': n, 'response': {**response, 'served_by': n}} for n in assigned)
            for node in range(1, node_count + 1):
                view = {'cell': cell, 'node': node, 'registered': node in assigned}
                if node in assigned:
                    view.update(leader=assigned[0], role='Leader' if node == assigned[0] else 'Voter',
                                voters=sorted(assigned) if node == assigned[0] else None)
                topology.append({'cell': cell, 'node': node, 'status': 200, 'response': view})
        (root / 'topology.json').write_text(json.dumps({'before': topology, 'after': topology}))
        from test_raft_kernel import rows as kernel_rows
        from raft_kernel import analyze as analyze_kernel
        kernel = {'measurement': {'before': kernel_rows(report, 30), 'after': kernel_rows(report, 40)}}
        report['measurement'].update(started_monotonic_seconds=31, elapsed_seconds=2)
        if profile['timing']['warmup_seconds']:
            kernel['warmup'] = {'before': kernel_rows(report, 1), 'after': kernel_rows(report, 20)}
            report['warmup'] = {'started_monotonic_seconds': 2, 'elapsed_seconds': profile['timing']['warmup_seconds']}
        report['kernel'] = analyze_kernel(kernel, report)
        report['memory_objectives_met'] = True
        (root / 'kernel-measurements.json').write_text(json.dumps(kernel))
        return report, replicas

    def test_four_and_five_node_evidence_covers_only_assigned_replicas(self):
        from raft_analyze import analyze
        for nodes in (4, 5):
            for corrupt in ('valid', 'unassigned_replica', 'missing_node', 'extra_registration', 'wrong_voters', 'wrong_population', 'shared_cpu', 'short_vm', 'wrong_cgroup', 'false_durability', 'false_read_mode', 'false_memory', 'changed_kernel'):
                with self.subTest(nodes=nodes, corrupt=corrupt), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    report, replicas = self.fixture(root, nodes)
                    if corrupt == 'false_memory':
                        report['memory_objectives_met'] = False
                    elif corrupt == 'changed_kernel':
                        raw = json.loads((root / 'kernel-measurements.json').read_text())
                        raw['measurement']['after'][0]['identity']['started'] = 'changed-process'
                        (root / 'kernel-measurements.json').write_text(json.dumps(raw))
                    if corrupt == 'unassigned_replica':
                        replicas[0]['node'] = 4
                        replicas[0]['response']['served_by'] = 4
                    elif corrupt == 'missing_node':
                        raw = json.loads((root / 'measurements.json').read_text())
                        raw['measurement']['after'].pop()
                        (root / 'measurements.json').write_text(json.dumps(raw))
                    elif corrupt in ('extra_registration', 'wrong_voters'):
                        raw = json.loads((root / 'topology.json').read_text())
                        if corrupt == 'extra_registration':
                            raw['before'][3]['response']['registered'] = True
                        else:
                            raw['before'][0]['response']['voters'] = [1, 2, 4]
                        (root / 'topology.json').write_text(json.dumps(raw))
                    elif corrupt == 'wrong_population':
                        raw = json.loads((root / 'node-samples.jsonl').read_text())
                        raw[-1]['sample']['resources']['groups']['durable_groups'] += 1
                        (root / 'node-samples.jsonl').write_text(json.dumps(raw) + '\n')
                    elif corrupt == 'shared_cpu':
                        report['limits'][-1]['CpusetCpus'] = '0'
                    elif corrupt == 'short_vm':
                        report['vm']['memory_bytes'] -= 1
                    elif corrupt in ('false_durability', 'false_read_mode'):
                        report['profile']['durability' if corrupt == 'false_durability' else 'read_consistency'] = 's3' if corrupt == 'false_durability' else 'linearizable'
                        report['profile_sha256'] = input_digest(report['profile'])
                    elif corrupt == 'wrong_cgroup':
                        report['cgroups'][-1]['node'] = 'other-run'
                    (root / 'report.json').write_text(json.dumps(report))
                    (root / 'replicas.json').write_text(json.dumps(replicas))
                    if corrupt == 'valid':
                        result = analyze(root)
                        self.assertEqual(result['replicas'], nodes * 3)
                        self.assertTrue(all(row['leader_cells'] == 1 and row['replica_cells'] == 3 for row in result['placement']))
                    else:
                        with self.assertRaises(ValueError):
                            analyze(root)

    def test_report_claims_cannot_override_ledger_or_replica_failures(self):
        from raft_analyze import analyze
        for name, corrupt in {
            'valid': lambda report, replicas: None,
            'unsupported_scenario': lambda report, replicas: report.update(scenario='disk-stall'),
            'unobserved_slow_follower': lambda report, replicas: report.update(scenario='slow-follower'),
            'missing_replica': lambda report, replicas: replicas.pop(),
            'wrong_scope': lambda report, replicas: replicas[0]['response'].update(incarnation='other'),
            'inflated_tps': lambda report, replicas: report['summary'].update(completed_per_second=1000),
            'wrong_archive': lambda report, replicas: report['final_archive'][0][1].update(archived=2),
            'uncapped_cpu': lambda report, replicas: report['cgroups'][0].update(cpu_max='max 100000'),
            'inflated_fsync': lambda report, replicas: report['performance']['measurement'][0]['counters'].update(engine_persist_groups=100),
        }.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                report, replicas = self.fixture(root)
                corrupt(report, replicas)
                (root / 'report.json').write_text(json.dumps(report))
                (root / 'replicas.json').write_text(json.dumps(replicas))
                if name == 'valid':
                    self.assertTrue(analyze(root)['verified'])
                else:
                    with self.assertRaises(ValueError):
                        analyze(root)



class PayloadPattern(unittest.TestCase):
    def test_retention_workload_has_deterministic_high_entropy_payloads(self):
        import zlib
        from raft_workload import payload_for
        profile = Profiles().profile()
        self.assertEqual(payload_for(profile, 'measurement', 1, 1024), 'x' * 1024)
        profile['payload_pattern'] = 'random_ascii'
        validate_profile(profile)
        value = payload_for(profile, 'measurement', 1, 32768)
        self.assertEqual(len(value), 32768)
        self.assertGreater(len(zlib.compress(value.encode())), len(value) * .7)
        self.assertEqual(value, payload_for(profile, 'measurement', 1, 32768))
        self.assertNotEqual(value, payload_for(profile, 'measurement', 2, 32768))
        profile['payload_pattern'] = 'unknown'
        with self.assertRaises(ValueError): validate_profile(profile)


if __name__ == '__main__':
    unittest.main()
