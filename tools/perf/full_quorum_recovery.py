# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Evidence contract for the all-replica-loss scenario in process_recovery.py."""
import recovery


def validate(evidence, image_bytes):
    require = recovery.require
    require(evidence.get('complete') is True, 'full quorum drill incomplete')
    require(evidence.get('failure_mode') == 'full_quorum_sigkill' and evidence.get('backend') == 'rustfs'
            and evidence.get('storage') == 'extents' and evidence.get('image_bytes') == image_bytes, 'wrong disaster mode/image')
    events = evidence.get('events', [])
    expected = ['quorum_write_before_loss', 'archive_current_before_loss', 'fault',
                *(['replica_killed_and_erased'] * 3), 'disaster_preflight_started', 'new_history_committed',
                'fresh_quorum_spawn_started', *(['fresh_process_ready'] * 3), *(['fresh_seed_installed'] * 3),
                *(['restored_read'] * 3), 'old_receipt_and_token_verified', 'first_write_verified',
                'quorum_payloads_and_receipts_verified', 'new_history_archive_current',
                *(['fresh_worker_joined'] * 3), 'restart_reads_and_receipts_verified',
                *(['resumed_worker_joined'] * 3), 'cleanup_joined']
    require([event.get('name') for event in events] == expected, 'incomplete or reordered disaster phases')
    times = [event.get('elapsed_us') for event in events]
    require(all(type(t) is int and t >= 0 for t in times) and times == sorted(times), 'invalid controller clock')
    def all_details(name):
        return [event['detail'] for event in events if event['name'] == name]
    def detail(name):
        return all_details(name)[0]
    def time(name):
        return next(event['elapsed_us'] for event in events if event['name'] == name)
    def pids(values):
        require(len(values) == len(set(values)) == 3 and all(type(pid) is int and pid > 0 for pid in values), 'workers not distinct OS processes')
        return set(values)
    before = detail('quorum_write_before_loss')
    old = pids(before['pids'])
    first = before['receipt']
    require(first['value'] == 8 and first['durability'] == 'quorum', 'original write not quorum acknowledged')
    status = detail('archive_current_before_loss')['status']
    require(status['archived'] >= status['committed'] >= first['index']
            and status['archive']['readiness'] == 'current' and status['archive']['lag_entries'] == 0, 'archive behind before failure')
    require(set(detail('fault')['pids']) == old and detail('fault')['signal'] == 9, 'wrong fault targets')
    killed = all_details('replica_killed_and_erased')
    require({item['pid'] for item in killed} == old and all(item['signal'] == 9 and item['absent'] is True for item in killed), 'all replicas were not killed and erased')
    require(len({item['directory'] for item in killed}) == 3, 'replica disks not distinct')
    ready = all_details('fresh_process_ready')
    fresh = pids([item['pid'] for item in ready])
    require(old.isdisjoint(fresh) and all(item['initially_empty'] is True for item in ready)
            and detail('fresh_quorum_spawn_started')['all_absent'] is True, 'new quorum not fresh')
    require({item['member'] for item in ready} == {1, 2, 3}, 'new quorum membership incomplete')
    seeds = all_details('fresh_seed_installed')
    require({item['pid'] for item in seeds} == fresh, 'wrong seed process')
    get_bytes = []
    for seed in seeds:
        counts = {item['name']: int(item['value']) for item in seed['performance']['counters']}
        require(seed['from_s3'] is True and seed['snapshot_timeout_ms'] == 3600000
                and counts['object_get_body_bytes'] >= image_bytes and counts['recovery_peer_received_bytes'] == 0, 'seed not exclusively restored from complete object store reads')
        get_bytes.append(counts['object_get_body_bytes'])
    reads = all_details('restored_read')
    require({item['served_by'] for item in reads} == {1, 2, 3}
            and all(item['verified'] is True and item['value'] == 8 for item in reads), 'restored quorum cannot read')
    new = detail('new_history_committed')
    require(new['group'] != first['group'] and new['incarnation'] != first['incarnation'], 'old authority reused')
    old_receipt = detail('old_receipt_and_token_verified')
    require(old_receipt['wrong_incarnation'] is True and old_receipt['callbacks'] == 0
            and old_receipt['receipt']['value'] == 8
            and all(old_receipt['receipt'][key] == new[key] for key in ['group', 'incarnation']), 'old receipt/token contract violated')
    write = detail('first_write_verified')
    require(write['value'] == 9 and write['durability'] == 'quorum'
            and all(write[key] == new[key] for key in ['group', 'incarnation']), 'new quorum write invalid')
    payloads = detail('quorum_payloads_and_receipts_verified')
    require(payloads['workers'] == 3 and payloads['callbacks'] == 1 and payloads['payload_bytes'] > 0
            and len(payloads['sample_ids']) == len(payloads['sample_hashes']) == 3, 'payload/receipt checks incomplete')
    archived = detail('new_history_archive_current')
    require(archived['archived'] >= archived['committed'] >= write['index']
            and archived['archive']['readiness'] == 'current', 'new quorum did not archive')
    restart = detail('restart_reads_and_receipts_verified')
    resumed = pids(restart['pids'])
    require(resumed.isdisjoint(old | fresh) and restart['value'] == 9 and restart['callbacks'] == 0, 'restart lost durable writes/receipts')
    for phase, group in [('fresh_worker_joined', fresh), ('resumed_worker_joined', resumed)]:
        joined = all_details(phase)
        require({item['pid'] for item in joined} == group and all(item['report']['owners_closed'] is True for item in joined), 'child owners did not join')
    require(detail('cleanup_joined') == {'workers': 9, 'hard_killed': 3, 'gracefully_closed': 6}, 'cleanup incomplete')
    return {'fault_to_verified_write_seconds': (time('first_write_verified') - time('fault')) / 1e6,
            'operator_preflight_and_fence_seconds': (time('new_history_committed') - time('disaster_preflight_started')) / 1e6,
            'fresh_quorum_start_through_all_seeds_seconds': (max(event['elapsed_us'] for event in events if event['name'] == 'fresh_seed_installed') - time('fresh_quorum_spawn_started')) / 1e6,
            'per_worker_object_get_body_bytes': get_bytes, 'peer_recovery_bytes': 0,
            'original_worker_pids': sorted(old), 'fresh_worker_pids': sorted(fresh), 'resumed_worker_pids': sorted(resumed)}
