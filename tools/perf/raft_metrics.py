"""Validate raw owner observations and derive bounded, source-stable deltas.

These are API/socket byte counts and operation histograms, not device I/O,
business acknowledgements or exact quantiles. Completion windows include work
started before the window; in-flight counts at both boundaries remain visible.
"""
import re

OPERATIONS = ('engine_queue', 'engine_file_sync', 'engine_directory_sync', 'engine_persist',
              'sql_queue', 'sql_stage', 'sql_prepare', 'sql_publish', 'sql_query',
              'sql_archive_capture', 'sql_other', 'object_get', 'object_put_create', 'object_put_update',
              'object_put_overwrite', 'controller_probe', 'controller_action')
COUNTERS = ('engine_file_read_bytes', 'engine_file_write_requested_bytes',
            'engine_file_write_completed_bytes', 'engine_persist_groups',
            'engine_persist_entries', 'engine_persist_entry_bytes', 'sql_stage_input_bytes',
            'sql_prepared_proposal_bytes', 'sql_prepared_ltx_bytes',
            'sql_query_input_bytes', 'sql_query_output_bytes', 'object_get_body_bytes',
            'object_put_requested_bytes', 'object_put_confirmed_bytes', 'transport_control_read_bytes',
            'transport_control_write_bytes', 'transport_data_read_bytes', 'transport_data_write_bytes',
            'transport_query_read_bytes', 'transport_query_write_bytes')
BOUNDS = (50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000,
          100000, 250000, 1000000, 5000000, 30000000, None)
FIELDS = ('started', 'succeeded', 'failed', 'abandoned', 'timing_unavailable', 'count', 'sum_us')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def decimal(value):
    require(isinstance(value, str) and re.fullmatch(r'0|[1-9][0-9]{0,19}', value),
            'invalid decimal observation')
    result = int(value)
    # A saturated value cannot distinguish overflow from an exact MAX sample.
    require(result < 2**64 - 1, 'saturated observation')
    return result


def histogram(operation):
    require(operation['count'] == operation['succeeded'] + operation['failed'] + operation['abandoned'],
            'outcome/histogram mismatch')
    require(operation['timing_unavailable'] == 0, 'unavailable operation timing')
    previous, minimum, maximum = 0, 0, 0
    for index, (bound, count) in enumerate(zip(BOUNDS, operation['buckets'])):
        require(count >= previous, 'nonmonotone histogram')
        population = count - previous
        minimum += population * (0 if index == 0 else BOUNDS[index-1] + 1)
        if bound is None:
            if population:
                maximum = None
        else:
            maximum += population * bound
        previous = count
    require(previous == operation['count'], 'histogram count mismatch')
    require(operation['sum_us'] >= minimum and (maximum is None or operation['sum_us'] <= maximum),
            'impossible histogram duration sum')


def snapshot(raw):
    require(isinstance(raw, dict) and set(raw) == {'source', 'available', 'saturated', 'sampled_at_us', 'counters', 'operations'},
            'invalid performance fields')
    require(raw['available'] is True and raw['saturated'] is False, 'unavailable performance sample')
    require(isinstance(raw['source'], str) and re.fullmatch('[0-9a-f]{32}', raw['source']), 'invalid performance source')
    counters, operations = {}, {}
    require(isinstance(raw['counters'], list) and len(raw['counters']) == len(COUNTERS), 'counter vocabulary differs')
    for counter in raw['counters']:
        require(set(counter) == {'name', 'value'}, 'invalid counter fields')
        name = counter['name']
        require(name in COUNTERS and name not in counters, 'counter vocabulary differs')
        counters[name] = decimal(counter['value'])
    require(isinstance(raw['operations'], list) and len(raw['operations']) == len(OPERATIONS), 'operation vocabulary differs')
    for operation in raw['operations']:
        require(set(operation) == {'name', 'buckets', *FIELDS}, 'invalid operation fields')
        name = operation['name']
        require(name in OPERATIONS and name not in operations, 'operation vocabulary differs')
        parsed = {field: decimal(operation[field]) for field in FIELDS}
        buckets = operation['buckets']
        require(isinstance(buckets, list) and len(buckets) == len(BOUNDS), 'histogram bounds differ')
        parsed['buckets'] = []
        for bucket, expected in zip(buckets, BOUNDS):
            require(set(bucket) == {'upper_us', 'count'}, 'invalid bucket fields')
            bound = None if bucket['upper_us'] is None else decimal(bucket['upper_us'])
            require(bound == expected, 'histogram bounds differ')
            parsed['buckets'].append(decimal(bucket['count']))
        histogram(parsed)
        require(parsed['started'] >= parsed['count'], 'completions exceed starts')
        operations[name] = parsed
    return {'source': raw['source'], 'sampled_at_us': decimal(raw['sampled_at_us']),
            'counters': counters, 'operations': operations}


def delta(before_raw, after_raw):
    before, after = snapshot(before_raw), snapshot(after_raw)
    require(before['source'] == after['source'], 'performance source changed')
    require(after['sampled_at_us'] > before['sampled_at_us'], 'owner sample clock did not advance')
    def difference(left, right):
        require(right >= left, 'performance counter reset')
        return right - left
    counters = {name: difference(before['counters'][name], after['counters'][name]) for name in COUNTERS}
    operations = {}
    for name in OPERATIONS:
        left, right = before['operations'][name], after['operations'][name]
        operation = {field: difference(left[field], right[field]) for field in FIELDS}
        operation['buckets'] = [difference(a, b) for a, b in zip(left['buckets'], right['buckets'])]
        histogram(operation)
        operation['inflight_before'] = left['started'] - left['count']
        operation['inflight_after'] = right['started'] - right['count']
        operation['quantile_upper_us'] = {}
        for label, numerator, denominator in [('p50', 1, 2), ('p95', 95, 100), ('p99', 99, 100), ('p99_9', 999, 1000)]:
            rank = (operation['count'] * numerator + denominator - 1) // denominator
            bound = next((bound for bound, count in zip(BOUNDS, operation['buckets']) if count >= rank), None) if rank else None
            operation['quantile_upper_us'][label] = bound
        operations[name] = operation
    return {'source': before['source'], 'owner_sample_interval_us': after['sampled_at_us'] - before['sampled_at_us'],
            'counters': counters, 'operations': operations}


def samples(rows):
    require(isinstance(rows, list) and len(rows) == 3, 'missing node samples')
    result, sources = {}, set()
    for row in rows:
        require(set(row) == {'node', 'started_ns', 'finished_ns', 'sample'}, 'invalid sample envelope')
        node = row['node']
        require(type(node) is int and node in (1, 2, 3) and node not in result, 'invalid or duplicate sampled node')
        require(type(row['started_ns']) is int and type(row['finished_ns']) is int
                and 0 <= row['started_ns'] < row['finished_ns'], 'invalid request interval')
        require(isinstance(row['sample'], dict) and set(row['sample']) == {'resources', 'performance'}, 'invalid node sample')
        require(isinstance(row['sample']['resources'], dict) and row['sample']['resources'].get('sql') is not None
                and row['sample']['resources'].get('groups') is not None, 'unavailable resource sample')
        observed = snapshot(row['sample']['performance'])
        require(observed['source'] not in sources, 'duplicate physical measurement source')
        sources.add(observed['source'])
        result[node] = row
    return result


def phases(raw, warmup):
    expected = ['warmup', 'measurement'] if warmup else ['measurement']
    require(isinstance(raw, dict) and set(raw) == set(expected), 'missing or unexpected measurement phase')
    result, previous = {}, None
    for phase in expected:
        require(set(raw[phase]) == {'before', 'after'}, 'missing phase boundary')
        before, after = samples(raw[phase]['before']), samples(raw[phase]['after'])
        require(max(row['finished_ns'] for row in before.values()) <= min(row['started_ns'] for row in after.values()),
                'overlapping phase boundaries')
        if previous is not None:
            require(max(row['finished_ns'] for row in previous.values()) <= min(row['started_ns'] for row in before.values()),
                    'overlapping phases')
            for node in before:
                delta(previous[node]['sample']['performance'], before[node]['sample']['performance'])
        result[phase] = [{'node': node, 'request_window_ns': [before[node]['started_ns'], after[node]['finished_ns']],
                          **delta(before[node]['sample']['performance'], after[node]['sample']['performance'])} for node in (1, 2, 3)]
        previous = after
    return result


def periodic(observations, sources):
    previous, count = None, 0
    for raw in observations:
        current = samples(raw)
        for node, row in current.items():
            require(row['sample']['performance']['source'] == sources[node], 'periodic source differs from phase source')
        if previous is not None:
            require(max(row['finished_ns'] for row in previous.values()) <= min(row['started_ns'] for row in current.values()),
                    'overlapping periodic samples')
            for node in current:
                delta(previous[node]['sample']['performance'], current[node]['sample']['performance'])
        previous, count = current, count + 1
    require(count > 0, 'missing periodic node samples')
    return count
