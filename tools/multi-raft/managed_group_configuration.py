# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Exact private config/TLS copies into stopped, controller-owned containers."""
import hashlib
import io
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile

from raft_disaster import owned_container
from raft_run import require

FILES = frozenset(('config.json', 'ca.der', 'server.der', 'key.der'))


def copy(context, identifier, run_id, files):
    require(re.fullmatch(r'colima-cellar-multi-raft(?:-[a-z0-9-]+)?', context)
            and re.fullmatch('[0-9a-f]{64}', identifier), 'isolated immutable container required')
    require(set(files) in ({'config.json'}, FILES)
            and all(isinstance(value, bytes) and 0 < len(value) <= 256*1024
                    for value in files.values()), 'only bounded configuration and peer TLS files may be copied')

    def docker(*args):
        result = subprocess.run(['docker', '--context', context, *args], capture_output=True)
        require(result.returncode == 0, 'owned configuration Docker operation failed')
        return result.stdout

    info = owned_container(lambda *args: docker(*args).decode(), identifier, run_id)
    require(not info['State']['Running'], 'configuration cannot change under a live process')
    with tempfile.TemporaryDirectory(prefix='cellar-managed-config-') as temporary:
        snapshot = Path(temporary)/'config'
        snapshot.mkdir(mode=0o700)
        for name, data in files.items():
            with (snapshot/name).open('xb') as stream:
                os.chmod(snapshot/name, 0o400)
                stream.write(data)
        docker('cp', str(snapshot), identifier+':/')
    for name, expected in files.items():
        with tarfile.open(fileobj=io.BytesIO(docker('cp', identifier+':/config/'+name, '-'))) as archive:
            members = archive.getmembers()
            require(len(members) == 1 and members[0].isfile() and members[0].name == name
                    and members[0].size == len(expected), 'invalid configuration readback')
            require(archive.extractfile(members[0]).read() == expected,
                    'configuration differs from controller snapshot')
    return {name: hashlib.sha256(data).hexdigest() for name, data in files.items()}
