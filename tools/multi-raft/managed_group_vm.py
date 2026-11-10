# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Control only existing dedicated Colima VMs through Lima's scoped API.

Colima's macOS filesystem watcher can stall before VM operations. Lima controls
the same VM and Docker disk without that watcher. Resource metadata stays in
sync; default/remote contexts, instance creation and unrelated profiles refuse.
"""
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import unquote, urlsplit

from raft_disaster import command
from raft_run import require


def profile(context):
    require(re.fullmatch(r'colima-cellar-multi-raft-[a-z0-9-]+',context),'explicit dedicated VM context required')
    return context.removeprefix('colima-')


def scope(context,endpoint):
    name=profile(context);url=urlsplit(endpoint)
    require(url.scheme=='unix' and not url.netloc and not url.query and not url.fragment,
            'dedicated VM requires a local Unix Docker socket')
    socket=Path(unquote(url.path))
    require(socket.is_absolute() and '..' not in socket.parts and socket.name=='docker.sock' and socket.parent.name==name,
            'Docker socket does not match the dedicated profile')
    return socket.parent.parent,context


class DedicatedVM:
    def __init__(self,context):
        profile(context)
        self.context=context
        data=json.loads(command(['docker','context','inspect',context]).stdout)
        require(len(data)==1 and data[0]['Name']==context,'different Docker context')
        self.home,self.instance=scope(context,data[0]['Endpoints']['docker']['Host'])
        self.config=self.home/profile(context)/'colima.yaml'
        require(self.config.is_file() and (self.home/'_lima'/self.instance/'lima.yaml').is_file(),
                'dedicated VM must already exist')

    def call(self,*args):
        return command(['env','LIMA_HOME='+str(self.home/'_lima'),'limactl','--tty=false',*args])

    def status(self):
        data=json.loads(self.call('list','--json',self.instance).stdout)
        require(data['name']==self.instance,'different Lima instance')
        return {key:data[key] for key in ('name','status','cpus','memory')}

    def resources(self,cpus,memory):
        text=self.config.read_text();require(len(text)<=65536,'VM configuration exceeds bound')
        for key,value in [('cpu',cpus),('memory',memory)]:
            text,count=re.subn(r'^'+key+r': [0-9]+$',key+': '+str(value),text,flags=re.MULTILINE)
            require(count==1,'VM resource configuration is not explicit')
        with tempfile.NamedTemporaryFile(mode='w',dir=self.config.parent,prefix='.cellar-vm-',delete=False) as output:
            temporary=Path(output.name)
            output.write(text)
        try:
            temporary.chmod(self.config.stat().st_mode & 0o777)
            os.replace(temporary,self.config)
        finally:temporary.unlink(missing_ok=True)

    def stop(self):
        state=self.status()['status']
        if state=='Stopped':return
        require(state=='Running','VM stop requires a known running state')
        require(not command(['docker','--context',self.context,'ps','-q']).stdout.strip(),
                'VM stop requires every container to be stopped')
        self.call('stop',self.instance)
        require(self.status()['status']=='Stopped','VM stop remains unconfirmed')

    def start(self,cpus,memory):
        require(type(cpus) is int and type(memory) is int and cpus>0 and memory>0,'explicit VM resources required')
        state=self.status()
        if state['status']=='Stopped':
            self.call('edit','--cpus',str(cpus),'--memory',str(memory),self.instance)
            self.resources(cpus,memory)
            self.call('start',self.instance)
            self.shell('sudo','systemctl','start','docker.service')
            state=self.status()
        require(state['status']=='Running' and state['cpus']==cpus and state['memory']==memory*1024**3,
                'VM does not have the exact declared resources')

    def shell(self,*args):
        return self.call('shell',self.instance,'--',*args)
