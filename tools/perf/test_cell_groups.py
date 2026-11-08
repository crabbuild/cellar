# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Campaign contracts; synthetic fixtures never count as measured release runs."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import cell_group_campaign as campaign
from cell_group_resources import Engine, OwnedRun, save, stop
from raft_run import input_digest, read_json, summarize_ledger
from raft_workload import Ledger as OperationLedger, run_load
import test_raft_run as existing

OWNER = 'a' * 24
IDENTITY = 'b' * 64
CAMPAIGN = '3c8c75d1-cb6f-448c-a7ec-2fb4e095930b'
CONTEXT = 'colima-cellar-multi-raft-cg14'
ENGINE = 'test-engine-identity'


class FakeEngine:
    def __init__(self):
        self.calls = []
        self.identity = ENGINE
        self.containers = {}

    def invoke(self, *args):
        self.calls.append(args)
        if args[0] == 'info':
            return self.identity
        if args[0] == 'inspect':
            target = args[-1]
            matches = [row for name, row in self.containers.items() if target in (name, row['id'])]
            if not matches:
                raise ValueError('no such retained container')
            return json.dumps(matches[0])
        if args[0] == 'run':
            name = args[args.index('--name') + 1]
            self.containers[name] = {'id': IDENTITY, 'name': '/' + name, 'owner': OWNER,
                                     'running': True, 'paused': False, 'exit_code': 0, 'finished': ''}
            return IDENTITY
        if args[0] in ('stop', 'unpause'):
            row = next(row for row in self.containers.values() if row['id'] == args[-1])
            row.update(running=False, paused=False, finished='joined')
            return IDENTITY
        if args[0] == 'stats':
            return 'owned-stats'
        raise AssertionError('unexpected fake Engine operation')

    def engine(self, context=CONTEXT, identity=ENGINE):
        return Engine(context, identity, self.invoke)


class Ownership(unittest.TestCase):
    def test_sampler_observes_only_exact_owned_process_ids(self):
        with tempfile.TemporaryDirectory() as temporary:
            fake = FakeEngine()
            owned = self.owned(Path(temporary).resolve(), fake)
            name = self.launch(owned)
            self.assertEqual(owned('stats', '--no-stream', '--format', '{{json .}}', name), 'owned-stats')
            self.assertEqual(fake.calls[-1], ('stats', '--no-stream', '--format', '{{json .}}', IDENTITY))
            for args in [('stats', name), ('stats', '--no-stream', '--format', '{{json .}}', 'unowned')]:
                with self.subTest(args=args), self.assertRaises(ValueError):
                    owned(*args)
            fake.containers[name]['owner'] = 'foreign'
            with self.assertRaises(ValueError):
                owned('stats', '--no-stream', '--format', '{{json .}}', name)

    def owned(self, root, fake):
        return OwnedRun(fake.engine(), root, OWNER, CAMPAIGN)

    def launch(self, owned):
        name = 'cellar-mr14-' + OWNER + '-node1'
        owned('run', '-d', '--name', name, '--label', 'io.cellar.mr14.run=' + OWNER,
              'sha256:' + 'c' * 64)
        return name

    def test_stop_joins_only_immutable_owned_id_and_retains_resources(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, fake = Path(temporary).resolve(), FakeEngine()
            owned = self.owned(root, fake)
            self.launch(owned)
            result = stop(root, CAMPAIGN, fake.engine)
            self.assertEqual(result, {'containers': 1, 'joined_processes': 1, 'data_retained': True})
            self.assertIn(('stop', '--time', '10', IDENTITY), fake.calls)
            self.assertTrue(all(call[0] not in ('rm', 'prune') for call in fake.calls))
            self.assertTrue(read_json(root / 'resources.json')['stops'])

    def test_wrong_engine_label_identity_directory_or_campaign_never_signals(self):
        for fault in ('engine', 'label', 'identity', 'directory', 'campaign'):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                root, fake = Path(temporary).resolve(), FakeEngine()
                owned = self.owned(root, fake)
                name = self.launch(owned)
                state = read_json(root / 'resources.json')
                if fault == 'engine':
                    fake.identity = 'another-engine'
                elif fault == 'label':
                    fake.containers[name]['owner'] = 'foreign'
                elif fault == 'identity':
                    fake.containers[name]['id'] = 'd' * 64
                elif fault == 'directory':
                    state['directory'] = str(root / 'unowned')
                else:
                    state['campaign'] = 'foreign'
                save(root / 'resources.json', state)
                fake.calls.clear()
                with self.assertRaises(ValueError):
                    stop(root, CAMPAIGN, fake.engine)
                self.assertFalse(any(call[0] in ('stop', 'unpause') for call in fake.calls))

    def test_all_targets_are_checked_before_any_signal(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, fake = Path(temporary).resolve(), FakeEngine()
            owned = self.owned(root, fake)
            self.launch(owned)
            state = read_json(root / 'resources.json')
            state['containers']['cellar-mr14-' + OWNER + '-node2'] = {'id': 'd' * 64}
            save(root / 'resources.json', state)
            fake.calls.clear()
            with self.assertRaises(ValueError):
                stop(root, CAMPAIGN, fake.engine)
            self.assertFalse(any(call[0] == 'stop' for call in fake.calls))

    def test_creation_requires_exact_label_and_never_deletes(self):
        with tempfile.TemporaryDirectory() as temporary:
            fake = FakeEngine()
            owned = self.owned(Path(temporary).resolve(), fake)
            for args in [('run', '-d', '--name', 'unowned', '--label', 'foreign=x'),
                         ('volume', 'create', '--label', 'foreign=x', 'unowned'),
                         ('rm', '-f', IDENTITY), ('system', 'prune', '-af'), ('image', 'prune', '-af')]:
                with self.subTest(args=args), self.assertRaises(ValueError):
                    owned(*args)
            self.assertTrue(all(call[0] == 'info' for call in fake.calls))

    def test_correct_label_does_not_authorize_unowned_mounts_or_networks(self):
        with tempfile.TemporaryDirectory() as temporary:
            fake = FakeEngine()
            owned = self.owned(Path(temporary).resolve(), fake)
            for extra in [('-v', '/unowned:/data'), ('-v', 'someone-elses-volume:/data'),
                          ('--mount', 'type=bind,src=/unowned,dst=/data'), ('--network', 'someone-elses-network')]:
                with self.subTest(extra=extra), self.assertRaises(ValueError):
                    owned('run', '--rm', '--label', 'io.cellar.mr14.run=' + OWNER,
                          *extra, 'sha256:' + 'c' * 64)
            self.assertTrue(all(call[0] == 'info' for call in fake.calls))

    def test_lost_creation_response_keeps_intent_without_relaunch(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, fake = Path(temporary).resolve(), FakeEngine()
            real = fake.invoke
            def lose(*args):
                result = real(*args)
                if args[0] == 'run':
                    raise TimeoutError('lost creation reply')
                return result
            owned = OwnedRun(Engine(CONTEXT, ENGINE, lose), root, OWNER, CAMPAIGN)
            with self.assertRaises(TimeoutError):
                self.launch(owned)
            self.assertEqual(len(read_json(root / 'resources.json')['intents']), 1)
            stop(root, CAMPAIGN, fake.engine)
            self.assertEqual(sum(call[0] == 'run' for call in fake.calls), 1)

    def test_existing_foreign_volume_cannot_be_adopted_or_mounted(self):
        with tempfile.TemporaryDirectory() as temporary:
            fake = FakeEngine()
            name = 'cellar-mr14-' + OWNER + '-node1'
            def invoke(*args):
                if args[:2] == ('volume', 'create'):
                    return name
                if args[:2] == ('volume', 'inspect'):
                    return json.dumps([{'Name': name, 'Driver': 'local', 'Options': None,
                                        'Labels': {'io.cellar.mr14.run': 'foreign'}}])
                return fake.invoke(*args)
            owned = OwnedRun(Engine(CONTEXT, ENGINE, invoke), Path(temporary).resolve(), OWNER, CAMPAIGN)
            with self.assertRaisesRegex(ValueError, 'foreign'):
                owned('volume', 'create', '--label', 'io.cellar.mr14.run=' + OWNER, name)
            self.assertEqual(owned.state['volumes'], [])
            with self.assertRaises(ValueError):
                owned('run', '--rm', '--label', 'io.cellar.mr14.run=' + OWNER, '-v', name + ':/data', 'image')

    def test_default_context_is_refused_before_external_call(self):
        for context in ('default', '', 'colima', 'colima-cellar-multi-raft; rm'):
            invoke = Mock()
            with self.subTest(context=context), self.assertRaises(ValueError):
                Engine(context, ENGINE, invoke)
            invoke.assert_not_called()


class Ledger(existing.OfferedLoad, existing.HistoryOracle):
    def test_idle_has_no_generated_arrivals_and_does_not_send(self):
        from test_raft_run import Profiles as ExistingProfiles
        profile = ExistingProfiles().profile()
        profile['arrival']['rate_per_second'] = 0
        send = Mock()
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'idle.jsonl'
            ledger = OperationLedger(path)
            result = run_load(profile, 'measurement', .001, {}, ledger, send, 'idle')
            ledger.close()
            summary = summarize_ledger(path, 'measurement', .001)
        send.assert_not_called()
        self.assertEqual(result['offered'], 0)
        self.assertEqual(summary['counts']['offered'], 0)


class Evidence(existing.IndependentReport, existing.Freshness):
    def campaign_fixture(self, base):
        root = base / CAMPAIGN
        root.mkdir()
        profiles = Profiles()
        settings = profiles.settings(base)
        profile = profiles.frozen()
        profile.update(cells=1, nodes=3, occupancies=[2], gate_occupancies=[2], image_modes=['extents'],
                       repetitions=1, duration_seconds=2, offered_requests_per_second=1,
                       historical_control=False, ratios={}, checks={'p99_ms': {'max': 200}, 'latency_samples': {'min': 2}})
        save(base / 'profiles.json', {'format': 1, 'profiles': [profile]})
        settings.update(source_revision='a' * 40, profiles_sha256=campaign.digest(base / 'profiles.json'),
                        context=CONTEXT, engine_id=ENGINE, data_root=str(base))
        plan = campaign.cohorts(settings, profile)[0]
        directory = root / 'cohort-000'
        workload = directory / 'workload'
        workload.mkdir(parents=True)
        report, replicas = self.fixture(workload)
        build = {'binary_sha256': 'c' * 64}
        report.update(profile=plan['workload'], profile_sha256=input_digest(plan['workload']),
                      build=build, run_id=plan['owner'], context=CONTEXT, network=plan['subnet'])
        kernel = read_json(workload / 'kernel-measurements.json')
        for phase in kernel.values():
            for rows in phase.values():
                for row in rows:
                    row['identity']['owner'] = plan['owner']
        save(workload / 'kernel-measurements.json', kernel)
        save(workload / 'report.json', report)
        save(workload / 'replicas.json', replicas)
        for name in ('resources.jsonl', 'probe-timings.jsonl'):
            (workload / name).write_text('{}\n')
        prefix = 'cellar-mr14-' + plan['owner']
        containers = {prefix + '-' + suffix: {'id': f'{i:064x}'} for i, suffix in enumerate(
            ['node1', 'node2', 'node3', 'store', 'config-copy'], 1)}
        from cell_group_observations import CLOSE
        from raft_groups import profile_layout
        layout = profile_layout(plan['workload'])
        acknowledgements = []
        for line in (workload / 'operations.jsonl').read_text().splitlines():
            event = json.loads(line)
            if event['event'] in ('seed_complete', 'complete') and event.get('status') == 200 and (
                    event['event'] == 'seed_complete' or event['operation'] == 'command'):
                acknowledgements.append({'event': 'ack', 'cell': event['cell'], 'group': layout.group(event['cell'])['ordinal'],
                    'index': event['response']['index'], 'phase': event.get('phase', 'seed'),
                    'started_ns': 1, 'finished_ns': 2})
        observed = {'event': 'archive', 'cell': 0, 'group': 0, 'started_ns': 3, 'finished_ns': 4,
            'status': 200, 'response': {'cell': 0, 'registered': True, 'group': layout.group(0)['group'],
                'group_incarnation': layout.group(0)['incarnation'], 'archived': 1000}}
        (directory / 'archive-observations.jsonl').write_text(''.join(json.dumps({**row, 'order': n}) + '\n'
            for n, row in enumerate(acknowledgements + [observed], 1)))
        joins = []
        for n in range(1, 4):
            closed = {'node': n, 'groups_total': 1, 'omitted_groups': 0, 'failed_groups': 0,
                      'mutation_outcomes': 'no_unresolved_local_producers'}
            identity = containers[prefix + f'-node{n}']['id']
            joins.append({'node': n, 'id': identity, 'state': {'id': identity, 'running': False, 'paused': False, 'exit_code': 0},
                          'reports': [closed]})
            (directory / f'joined-node{n}.log').write_text(CLOSE + json.dumps(closed) + '\n')
        save(directory / 'joins.json', joins)
        settings['storage'] = {'node_bytes': 1024**3, 'object_store_bytes': 1024**3}
        volumes, raw_disks = {}, []
        for n, name in enumerate(list(containers)[:-1]):
            volumes[name] = {'volume': name, 'device': f'/dev/loop{n}', 'uuid': 'u', 'bytes': 1024**3, 'available_bytes': 1024**3}
            raw_disks.append({'Name': name, 'Labels': {'io.cellar.mr14.run': plan['owner']},
                'Options': {'type': 'ext4', 'device': f'/dev/loop{n}', 'o': 'rw,nodiscard'}})
        save(directory / 'disks.json', {'owner': plan['owner'], 'engine_id': ENGINE, 'volumes': volumes})
        save(directory / 'disk-verification.json', raw_disks)
        save(directory / 'resources.json', {'campaign': CAMPAIGN, 'owner': plan['owner'],
            'context': CONTEXT, 'engine_id': ENGINE, 'directory': str(directory), 'containers': containers,
            'stops': [{'after': {'id': item['id'], 'running': False, 'paused': False}} for item in containers.values()]})
        save(root / 'campaign.json', {'settings': settings, 'profile': profile['id'], 'completed': True,
            'preflight': {'source_manifest': {'fixture': 'source'}, 'profile': profile, 'build': build},
            'cohorts': [{'plan': plan, 'completed': True}]})
        save(root / 'receipt.json', {'format': 1, 'artifacts': campaign.artifacts(root)})
        return root

    def test_archive_lag_retains_debt_and_refuses_missing_ack_or_wrong_history(self):
        from cell_group_observations import archive_lag
        from raft_groups import profile_layout
        with tempfile.TemporaryDirectory() as temporary:
            root = self.campaign_fixture(Path(temporary).resolve())
            directory = root / 'cohort-000'
            layout = profile_layout(read_json(directory / 'workload/report.json')['profile'])
            path = directory / 'archive-observations.jsonl'
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            valid = archive_lag(path, directory / 'workload/operations.jsonl', layout)
            self.assertGreater(valid['acknowledged_commands'], 0)
            # Delayed coverage increases the reported upper bound. A final
            # snapshot never turns the unobserved outage into a measured zero.
            rows[-1]['finished_ns'] = 80_000_000_000
            path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            self.assertGreater(archive_lag(path, directory / 'workload/operations.jsonl', layout)['max_archive_lag_seconds'], 60)
            original = copy.deepcopy(rows)
            for fault in ('missing', 'ack', 'history', 'frontier'):
                rows = copy.deepcopy(original)
                if fault == 'missing':
                    rows.pop()
                elif fault == 'ack':
                    rows[0]['index'] += 1
                elif fault == 'history':
                    rows[-1]['response']['group_incarnation'] = 'bad'
                else:
                    rows[-1]['response']['archived'] = None
                path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
                with self.subTest(fault=fault), self.assertRaises(ValueError):
                    archive_lag(path, directory / 'workload/operations.jsonl', layout)

    def test_join_requires_native_close_after_exact_physical_exit(self):
        from cell_group_observations import joined_owners
        with tempfile.TemporaryDirectory() as temporary:
            root = self.campaign_fixture(Path(temporary).resolve()) / 'cohort-000'
            rows, owner = read_json(root / 'joins.json'), read_json(root / 'resources.json')
            planned = read_json(root / 'workload/report.json')['placement']
            self.assertEqual(joined_owners(rows, planned, owner), 0)
            for fault in ('killed', 'absent', 'omitted', 'unobserved'):
                changed = copy.deepcopy(rows)
                if fault == 'killed':
                    changed[0]['state']['exit_code'] = 137
                elif fault == 'absent':
                    changed[0]['reports'] = []
                elif fault == 'omitted':
                    changed[0]['reports'][0]['omitted_groups'] = 1
                else:
                    changed[0]['reports'][0]['mutation_outcomes'] = 'unobserved'
                self.assertEqual(joined_owners(changed, planned, owner), 1)
            rows[0]['id'] = 'foreign'
            with self.assertRaises(ValueError):
                joined_owners(rows, planned, owner)

    def test_bounded_disks_refuse_missing_swapped_or_inflated_evidence(self):
        from cell_group_storage import verify_evidence
        with tempfile.TemporaryDirectory() as temporary:
            root = self.campaign_fixture(Path(temporary).resolve())
            directory = root / 'cohort-000'
            owner = read_json(directory / 'resources.json')
            policy = read_json(root / 'campaign.json')['settings']['storage']
            self.assertEqual(verify_evidence(directory, owner, policy, 3)['bounded_volumes'], 4)
            original = read_json(directory / 'disk-verification.json')
            for fault in ('missing', 'device', 'owner'):
                rows = copy.deepcopy(original)
                if fault == 'missing':
                    rows.pop()
                elif fault == 'device':
                    rows[0]['Options']['device'] = '/dev/loop999'
                else:
                    rows[0]['Labels']['io.cellar.mr14.run'] = 'foreign'
                save(directory / 'disk-verification.json', rows)
                with self.subTest(fault=fault), self.assertRaises(ValueError):
                    verify_evidence(directory, owner, policy, 3)

    def test_complete_campaign_replays_raw_evidence_without_claiming_release(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = self.campaign_fixture(Path(temporary).resolve())
            with patch.object(campaign, 'source_manifest', return_value={'fixture': 'source'}), \
                 patch.object(campaign, 'committed_manifest', return_value={'fixture': 'source'}):
                result = campaign.verify(root)
        self.assertTrue(result['verified'])
        self.assertFalse(result['qualified'])
        self.assertFalse(result['release_profile_catalog'])
        self.assertEqual(result['cohorts'][0]['gates']['p99_ms']['value'], 10)

    def test_source_profiles_ledger_and_binary_drift_cannot_reach_analysis(self):
        for fault in ('source', 'profile', 'missing_ledger', 'changed_binary'):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                root = self.campaign_fixture(Path(temporary).resolve())
                manifest = {'fixture': 'different'} if fault == 'source' else {'fixture': 'source'}
                if fault == 'profile':
                    (root.parent / 'profiles.json').write_text('{}')
                elif fault == 'missing_ledger':
                    (root / 'cohort-000/workload/operations.jsonl').unlink()
                elif fault == 'changed_binary':
                    report = read_json(root / 'cohort-000/workload/report.json')
                    report['build']['binary_sha256'] = 'd' * 64
                    save(root / 'cohort-000/workload/report.json', report)
                with patch.object(campaign, 'source_manifest', return_value=manifest), \
                     patch.object(campaign, 'committed_manifest', return_value={'fixture': 'source'}), \
                     self.assertRaises(ValueError):
                    campaign.verify(root)

    def test_artifact_manifest_detects_changed_and_missing_logs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'stdout.log').write_text('one test passed\n')
            before = campaign.artifacts(root)
            (root / 'stdout.log').write_text('zero tests passed\n')
            self.assertNotEqual(before, campaign.artifacts(root))
            (root / 'stdout.log').unlink()
            self.assertNotEqual(before, campaign.artifacts(root))

    def test_symlink_evidence_cannot_borrow_an_unowned_artifact(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'actual').write_text('data')
            (root / 'borrowed').symlink_to(root / 'actual')
            with self.assertRaisesRegex(ValueError, 'symbolic'):
                campaign.artifacts(root)

    def test_private_credentials_do_not_enter_public_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'config').mkdir()
            (root / 'config/token').write_text('private')
            (root / 'report.json').write_text('{}')
            self.assertEqual(set(campaign.artifacts(root)), {'report.json'})



class Profiles(unittest.TestCase):
    def frozen(self, identifier='mixed_quorum'):
        path = Path(__file__).resolve().parents[2] / 'docs/design/multi-raft/cell-groups/profiles.json'
        return copy.deepcopy(next(row for row in read_json(path)['profiles'] if row['id'] == identifier))

    def settings(self, root):
        from test_raft_run import Profiles as ExistingProfiles
        path = root / 'profiles.json'
        save(path, {'format': 1, 'profiles': [self.frozen()]})
        return {'profiles_file': str(path), 'run_id': CAMPAIGN, 'workload': ExistingProfiles().profile(),
                'network_cidr': '10.246.0.0/19'}

    def private(self, root):
        settings = self.settings(root)
        disk = root / '_lima/_disks' / CONTEXT / 'datadisk'
        disk.parent.mkdir(parents=True, exist_ok=True)
        disk.touch()
        settings['storage'] = {'colima_home': str(root), 'profile': CONTEXT.removeprefix('colima-'),
                               'backing_disk': str(disk), 'node_bytes': 1024**3, 'object_store_bytes': 1024**3}
        settings.update(format=1, source_revision='a' * 40, context=CONTEXT, engine_id=ENGINE,
                        image='sha256:' + 'c' * 64, rustfs_image=campaign.RUSTFS,
                        data_root=str(root), port_base=22000, profiles_sha256=campaign.digest(root / 'profiles.json'))
        return settings

    def test_private_configuration_requires_all_owners_hashes_and_explicit_limits(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            settings = self.private(root)
            path = root / 'private.json'
            save(path, settings)
            with patch.object(campaign, 'revision', return_value='a' * 40):
                self.assertEqual(campaign.config(path), settings)
                for name in settings:
                    changed = dict(settings)
                    del changed[name]
                    save(path, changed)
                    with self.subTest(missing=name), self.assertRaises(ValueError):
                        campaign.config(path)
                for key, value in [('source_revision', 'b' * 40), ('context', 'default'),
                                   ('profiles_sha256', 'b' * 64), ('port_base', True),
                                   ('image', 'cellar:latest'), ('rustfs_image', 'rustfs:latest'),
                                   ('data_root', '/'), ('profiles_file', 'relative.json')]:
                    save(path, {**settings, key: value})
                    with self.subTest(key=key), self.assertRaises(ValueError):
                        campaign.config(path)

    def test_insufficient_real_resources_refuse_without_images_or_provisioning(self):
        with tempfile.TemporaryDirectory() as temporary:
            settings = self.private(Path(temporary).resolve())
            engine = Mock()
            engine.invoke.return_value = json.dumps({'os': 'linux', 'cpus': 8, 'memory': 16 * 1024**3,
                                                     'memory_limit': True, 'cpu_quota': True})
            with self.assertRaisesRegex(ValueError, 'insufficient'):
                campaign.preflight(settings, 'mixed_quorum', engine)
            self.assertEqual(engine.invoke.call_count, 1)

    def test_invalid_numeric_gate_contracts_refuse_before_any_engine_call(self):
        for checks, ratios, occupancies in [({}, {}, [64]), ({'p99_ms': {'max': True}}, {}, [64]),
                ({'ratio': {'max': 2}}, {'ratio': {'numerator': 'p99_ms', 'denominator': 'p99_ms', 'control_occupancy': 1}}, [64])]:
            with self.subTest(checks=checks), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                settings = self.settings(root)
                profile = self.frozen()
                profile.update(checks=checks, ratios=ratios, occupancies=occupancies, gate_occupancies=occupancies)
                save(root / 'profiles.json', {'format': 1, 'profiles': [profile]})
                engine = Mock()
                with self.assertRaises(ValueError):
                    campaign.preflight(settings, 'mixed_quorum', engine)
                self.assertEqual(engine.mock_calls, [])

    def test_network_matrix_is_explicit_and_nonoverlapping(self):
        with tempfile.TemporaryDirectory() as temporary:
            settings = self.settings(Path(temporary))
            rows = campaign.cohorts(settings, self.frozen())
            self.assertEqual(len({row['subnet'] for row in rows}), len(rows))
            for pool in ('0.0.0.0/16', '10.246.0.0/25', '8.8.0.0/16', '10.246.1.0/19'):
                with self.subTest(pool=pool), self.assertRaises(ValueError):
                    campaign.network_pool(pool)
            with self.assertRaisesRegex(ValueError, 'every fresh cohort'):
                campaign.cohorts({**settings, 'network_cidr': '10.246.0.0/24'}, self.frozen())

    def test_retained_network_overlap_refuses_before_image_or_provisioning(self):
        with tempfile.TemporaryDirectory() as temporary:
            settings = self.private(Path(temporary).resolve())
            engine = Mock()
            engine.invoke.side_effect = [json.dumps({'os': 'linux', 'cpus': 8, 'memory': 24 * 1024**3,
                'memory_limit': True, 'cpu_quota': True}), '', 'network-id', '[{"Subnet":"10.246.1.0/24"}]']
            with self.assertRaisesRegex(ValueError, 'overlaps'):
                campaign.preflight(settings, 'mixed_quorum', engine)
            self.assertEqual(engine.invoke.call_count, 4)

    def test_frozen_matrix_uses_equal_cells_and_same_trace_at_every_occupancy(self):
        with tempfile.TemporaryDirectory() as temporary:
            settings = self.settings(Path(temporary))
            profile = self.frozen()
            rows = campaign.cohorts(settings, profile)
        self.assertEqual(len(rows), 30)
        self.assertEqual({row['occupancy'] for row in rows}, {1, 8, 32, 64, 128})
        self.assertEqual({row['image_mode'] for row in rows}, {'materialized', 'extents'})
        self.assertEqual({row['workload']['cells']['total'] for row in rows}, {4096})
        self.assertEqual(len({row['owner'] for row in rows}), 30)
        for repetition in range(3):
            matched = [row['workload'] for row in rows if row['repetition'] == repetition]
            self.assertEqual(len({input_digest({**row, 'image_mode': '', 'cells': {}}) for row in matched}), 1)

    def test_lifecycle_and_false_independent_topologies_refuse_before_docker(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            settings = self.settings(root)
            for identifier in ('split_merge', 'hibernate_wake', 'rustfs_disaster', 'independent_domain_loss', 'soak_24h'):
                save(root / 'profiles.json', {'format': 1, 'profiles': [self.frozen(identifier)]})
                engine = Mock()
                with self.subTest(identifier=identifier), self.assertRaisesRegex(ValueError, 'unfinished'):
                    campaign.preflight(settings, identifier, engine)
                self.assertEqual(engine.mock_calls, [])
            profile = self.frozen()
            profile['topology'] = 'independent_hosts'
            save(root / 'profiles.json', {'format': 1, 'profiles': [profile]})
            with self.assertRaisesRegex(ValueError, 'independent'):
                campaign.selected_profile(settings, 'mixed_quorum')

    def test_historical_control_cannot_relabel_materialized_as_extents(self):
        from cell_group_controls import validate
        profile = self.frozen()
        control = {'checkout': '/absent', 'run': '/absent', 'repetition': 0,
                   'image_mode': 'extents', 'artifacts_sha256': 'a' * 64}
        with self.assertRaisesRegex(ValueError, 'extent'):
            validate(control, profile, {})

    def test_storage_requires_exact_profile_disk_and_finite_integer_bounds(self):
        from cell_group_storage import validate
        with tempfile.TemporaryDirectory() as temporary:
            settings = self.private(Path(temporary).resolve())
            policy = settings['storage']
            validate(policy, CONTEXT)
            for key, value in [('profile', 'default'), ('backing_disk', '/dev/null'),
                               ('node_bytes', True), ('object_store_bytes', 0)]:
                with self.subTest(key=key), self.assertRaises(ValueError):
                    validate({**policy, key: value}, CONTEXT)

    def test_numeric_gates_do_not_accept_booleans_missing_metrics_or_nan(self):
        profile = {'checks': {'p99_ms': {'max': 200}, 'samples': {'min': 2}}}
        self.assertEqual(campaign.numeric_gates(profile, {'p99_ms': 201})['p99_ms']['status'], 'failed')
        self.assertEqual(campaign.numeric_gates(profile, {'p99_ms': 1})['samples']['status'], 'unmeasured')
        self.assertTrue(all(row['status'] == 'passed' for row in campaign.numeric_gates(profile, {'p99_ms': 199, 'samples': 2}).values()))
        for value in (True, False, -1, float('nan'), float('inf'), 'passed'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                campaign.numeric_gates(profile, {'p99_ms': value})

    def test_ratios_require_positive_matching_control_and_derive_from_measurements(self):
        profile = {'ratios': {'p99_ratio': {'numerator': 'p99_ms', 'denominator': 'p99_ms', 'control_occupancy': 1}}}
        control = {'repetition': 0, 'image_mode': 'extents', 'occupancy': 1, 'metrics': {'p99_ms': 100}}
        candidate = {**control, 'occupancy': 64, 'metrics': {'p99_ms': 125}}
        self.assertEqual(campaign.ratios(profile, candidate, [control, candidate])['p99_ratio'], 1.25)
        for rows in ([candidate], [control, control, candidate], [{**control, 'image_mode': 'materialized'}, candidate],
                     [{**control, 'metrics': {'p99_ms': 0}}, candidate]):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                campaign.ratios(profile, candidate, rows)


if __name__ == '__main__':
    unittest.main()
