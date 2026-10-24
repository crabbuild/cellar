#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Check native and Python routing failures through a compiled private host."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from smoke import Host, private_project, request
from python.smoke import check_report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cellar', type=Path, required=True)
    parser.add_argument('--project', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--expect-free-threaded', action='store_true')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    project = private_project(args.project, args.output)
    config = json.loads(project.read_text())
    bindings = config['primary']['bindings']
    assert sum(binding['name'] == 'ORDERS' for binding in bindings) == 1
    # Remove only this private deployment's binding. This forces a real native
    # namespace failure before Python cell execution, without injecting an
    # exception, changing application code, or touching another host's state.
    config['primary']['bindings'] = [binding for binding in bindings if binding['name'] != 'ORDERS']
    project.write_text(json.dumps(config, indent=2) + '\n')
    host = Host(args.cellar.resolve(), project, args.output,
                extra_env={'CELLAR_SHUTDOWN_REPORT_JSON': '1', 'PYTHONDONTWRITEBYTECODE': '1'})
    try:
        origin = host.ready()
        health = request(origin, '/health')
        assert health['framework'] == 'fastapi'
        assert health['free_threaded'] == args.expect_free_threaded, health
        if args.expect_free_threaded:
            assert not health['gil_enabled'], health
        payload = dict(customer_id='customer', items=[dict(sku='coffee', quantity=1, unit_price_cents=1250)])
        for path in ('/orders/order-1', '/orders/%6frder-1'):
            for method, body in [('GET', None), ('POST', payload),
                                 ('PATCH', dict(expected_version=1, status='confirmed'))]:
                response = request(origin, path, method=method, data=body, status=503)
                assert response == dict(detail='Cellar operation failed', code='Ambiguous', outcome='unknown'), response
        assert request(origin, '/health')['framework'] == 'fastapi'
    finally:
        host.stop()
    check_report(host.log)
    reports = [json.loads(line[line.index('{'):])['report'] for line in host.log.read_text().splitlines()
               if '"event":"embedded_python_shutdown"' in line]
    operations = reports[-1]['bridge']['operations']
    # Only the three gateway calls cross the Python SDK bridge. The three
    # canonical calls fail in Rust, and neither path retries its mutation.
    assert operations['admitted'] == operations['started'] == operations['failed'] == 3, operations
    print('PASS native ingress: sanitized failures, gateway equivalence, no replay and complete drain')


if __name__ == '__main__':
    main()
