#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
"""Deploy the SQLite counter. Credentials remain in process memory/Worker secrets."""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import urllib.request

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def environment(path):
    required = {'CF_ACCOUNT_ID', 'CF_API_TOKEN', 'CF_ACCESS_KEY_ID',
                'CF_SECRET_ACCESS_KEY', 'CF_S3_API_ENDPOINT'}
    values = {}
    for line in path.expanduser().read_text().splitlines():
        line = line.strip().removeprefix('export ')
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        if key.strip() in required:
            parsed = shlex.split(value, comments=True)
            values[key.strip()] = parsed[0] if parsed else ''
    missing = required - values.keys()
    if missing or any(not values[k] for k in required):
        raise ValueError('Missing required Cloudflare/R2 settings; see README')
    env = os.environ.copy()
    env['CLOUDFLARE_ACCOUNT_ID'] = values['CF_ACCOUNT_ID']
    env['CLOUDFLARE_API_TOKEN'] = values['CF_API_TOKEN']
    return values, env


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--credentials', type=Path, default=Path('~/.cf.env'))
    parser.add_argument('--docker-context', required=True)
    args = parser.parse_args()
    values, env = environment(args.credentials)
    docker = ['docker', '--context', args.docker_context]
    subprocess.run([*docker, 'info', '--format', '{{.OSType}}/{{.Architecture}}'], check=True)
    endpoint = json.loads(subprocess.check_output([*docker, 'context', 'inspect', args.docker_context], text=True))[0]['Endpoints']['docker']['Host']
    env['DOCKER_HOST'] = endpoint
    env['DOCKER_CONTEXT'] = args.docker_context
    config = json.loads((HERE / 'wrangler.jsonc').read_text())
    revision = subprocess.check_output(['git', '-C', str(ROOT), 'rev-parse', '--short=8', 'HEAD'], text=True).strip()
    config['vars']['CELLAR_REVISION'] = revision
    config['containers'][0]['image_vars']['CELLAR_COMMIT'] = revision
    (HERE / 'wrangler.jsonc').write_text(json.dumps(config, indent=2) + '\n')
    base = f"https://api.cloudflare.com/client/v4/accounts/{values['CF_ACCOUNT_ID']}"

    def api(method, suffix, body=None):
        request = urllib.request.Request(base + suffix, method=method,
            headers={'Authorization': 'Bearer ' + values['CF_API_TOKEN'], 'Content-Type': 'application/json'},
            data=None if body is None else json.dumps(body).encode())
        with urllib.request.urlopen(request, timeout=40) as response:
            result = json.load(response)
        if not result.get('success'):
            raise RuntimeError('Cloudflare API refused the deployment operation')
        return result['result']

    name = config['vars']['R2_BUCKET']
    existing = api('GET', '/r2/buckets')['buckets']
    if not any(bucket['name'] == name for bucket in existing):
        api('POST', '/r2/buckets', {'name': name})
    wrangler = str(HERE / 'node_modules/.bin/wrangler')
    common = ['--config', str(HERE / 'wrangler.jsonc')]
    secrets = {'R2_ENDPOINT': values['CF_S3_API_ENDPOINT'],
               'R2_ACCESS_KEY_ID': values['CF_ACCESS_KEY_ID'],
               'R2_SECRET_ACCESS_KEY': values['CF_SECRET_ACCESS_KEY']}
    subprocess.run([wrangler, 'secret', 'bulk', *common], input=json.dumps(secrets),
                   text=True, env=env, check=True)
    subprocess.run([wrangler, 'deploy', *common], env=env, cwd=ROOT, check=True)
    subdomain = api('GET', '/workers/subdomain')['subdomain']
    print(f"Counter URL: https://{config['name']}.{subdomain}.workers.dev")
    print('Verify actions and receipt deduplication with verify.py before sharing the result.')


if __name__ == '__main__':
    main()
