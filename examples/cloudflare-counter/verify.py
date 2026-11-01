#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
"""Check actual HTTP state changes, duplicates, stale identities and reads."""
import argparse
import concurrent.futures
import json
import re
import urllib.error
import urllib.parse
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('origin')
    parser.add_argument('--max-read-cell-ms', type=float,
                        help='Require three warm reads to finish inside the native cell within this budget')
    parser.add_argument('--max-read-dispatch-ms', type=float,
                        help='Also bound the complete native named-cell dispatch, including its response gate')
    args = parser.parse_args()
    origin = args.origin.rstrip('/')
    client = str(uuid.uuid4())

    def call(method, params=None):
        url = origin + '/api/counters/check'
        if params:
            url += '?' + urllib.parse.urlencode(params)
        request = urllib.request.Request(url, method=method,
            headers={'x-counter-action': '1', 'User-Agent': 'CellarCounterLab/1.0'})
        try:
            response = urllib.request.urlopen(request, timeout=60)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            body = json.load(response)
            return response.status, body, response.headers

    def command(sequence, delta):
        return {'client': client, 'sequence': sequence, 'action': 'add', 'delta': delta}

    status, before, _ = call('GET')
    assert status == 200
    status, first, headers = call('POST', command(1, 7))
    assert status == 200 and not first['duplicate']
    assert int(first['count']) == int(before['count']) + 7
    assert 'durability;dur=' in headers.get('server-timing', '')
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        repeated = list(pool.map(lambda _: call('POST', command(1, 7)), range(4)))
    assert all(code == 200 and result['duplicate'] and result['count'] == first['count'] for code, result, _ in repeated)
    assert call('POST', command(1, 9))[0] == 409
    status, second, _ = call('POST', command(2, -7))
    assert status == 200 and second['count'] == before['count']
    assert call('POST', command(1, 7))[0] == 409
    status, final, _ = call('GET')
    assert status == 200 and final['count'] == before['count']
    assert int(final['version']) == int(before['version']) + 2
    if args.max_read_cell_ms is not None or args.max_read_dispatch_ms is not None:
        observations = []
        dispatch = []
        for _ in range(3):
            status, value, headers = call('GET')
            assert status == 200 and value['count'] == final['count'] and value['version'] == final['version']
            timings = dict((key, float(value)) for key, value in re.findall(
                r'([\w-]+);dur=([\d.]+)', ','.join(headers.get_all('server-timing', []))))
            assert 'cell' in timings, 'Native cell timing is unavailable'
            assert 'cellar' in timings, 'Native dispatch timing is unavailable'
            observations.append(timings['cell'])
            dispatch.append(timings['cellar'])
        if args.max_read_cell_ms is not None:
            assert max(observations) <= args.max_read_cell_ms, f'Warm read cell latency exceeded budget: {observations} ms'
        if args.max_read_dispatch_ms is not None:
            assert max(dispatch) <= args.max_read_dispatch_ms, f'Warm read dispatch latency exceeded budget: {dispatch} ms'
        print(json.dumps({'warm_read_cell_ms': observations, 'warm_read_dispatch_ms': dispatch}))
    print(json.dumps({'http_state_checks': 'passed', 'concurrent_duplicate_retries': 4,
                      'stale_and_conflicting_retries_refused': True,
                      'durability_timing_present': True, 'counter_restored_to_initial_value': True}))


if __name__ == '__main__':
    main()
