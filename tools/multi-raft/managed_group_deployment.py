# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Fresh-VM recovery placement; retain the source VM only for its RustFS disk.

The caller permanently erases the old voter cohort before this handoff. A
preloaded replacement VM supplies a genuinely different Docker host identity.
No physical-host ID is invented to bypass the SDK's old-host exclusion.
"""
import hashlib
import ipaddress
import json
import re

from group_disaster import verify_image
from managed_group_configuration import copy as copy_managed
from raft_disaster import LABEL, command, owned_container
from raft_run import require


def profile(context):
    require(re.fullmatch(r'colima-cellar-multi-raft-[a-z0-9-]+',context),'explicit dedicated VM context required')
    return context.removeprefix('colima-')


def colima(context,*args):
    return command(['colima',*args,'--profile',profile(context)])


def prepare(driver):
    source=driver.source_context;replacement=driver.replacement_context
    require(source!=replacement,'a different recovery VM is required')
    require(not driver.docker('ps','-q').strip(),'source VM must have no running containers before setup')
    # Prepare image transport externally, while both VMs are idle and the source
    # has only 8 GiB. This preflight verifies it before any source disk can die.
    colima(source,'stop')
    colima(source,'start','--activate=false','--cpu','4','--memory','8')
    colima(replacement,'start','--activate=false','--cpu','6','--memory','16')
    driver.args.context=replacement
    try:
        require(not driver.docker('ps','-q').strip(),'replacement VM must be idle before setup')
        driver.args.replacement_image=driver.docker('image','inspect','--format','{{.Id}}',driver.args.replacement_image)
        built=verify_image(driver.args.replacement_image,driver.docker)
        info=json.loads(driver.docker('info','--format','{{json .}}'))
        driver.prepared_host=hashlib.sha256(('docker-daemon/'+info['ID']).encode()).hexdigest()[:32]
        driver.report['prepared_replacement']={'context':replacement,'host':driver.prepared_host,'build':built}
    finally:
        driver.args.context=source
    colima(replacement,'stop')
    colima(source,'stop')
    colima(source,'start','--activate=false','--cpu','8','--memory','24')
    driver.save()


def handoff(driver):
    source=driver.source_context;replacement=driver.replacement_context
    require(source!=replacement,'a different recovery VM is required')
    old_host=driver.physical_host
    store=driver.report['rustfs']['container_id']
    require(all(row['container_absent'] and row['disk_absent'] for row in driver.report['fence']),
            'source voter/disk fence must precede VM handoff')
    require(driver.launcher.certificate_payload(driver.model['selection']['fence_digest'],driver.docker),
            'source inventory is no longer permanently absent')
    # The 32-GiB development machine cannot reserve both a 24-GiB source lab
    # and a 16-GiB replacement VM at once. No SDK request or voter is entered
    # during this declared object-store restart; its disk and identity persist.
    driver.docker('stop','--time','60',store)
    colima(source,'stop')
    colima(source,'start','--activate=false','--cpu','4','--memory','8')
    info=json.loads(driver.docker('info','--format','{{json .}}'))
    require(hashlib.sha256(('docker-daemon/'+info['ID']).encode()).hexdigest()[:32]==old_host,
            'source VM identity changed')
    driver.docker('start',store)
    driver.until(driver.bucket,90)
    colima(replacement,'start','--activate=false','--cpu','6','--memory','16')
    driver.args.context=replacement
    driver.active_contexts.append(replacement)
    info=json.loads(driver.docker('info','--format','{{json .}}'))
    require(info['NCPU']>=6 and info['MemTotal']>=14.5*1024**3,'insufficient replacement VM resources')
    new_host=hashlib.sha256(('docker-daemon/'+info['ID']).encode()).hexdigest()[:32]
    require(new_host!=old_host and new_host==driver.prepared_host,'replacement VM reuses or changes the prepared host')
    # The exact tag is preloaded before the source drill; no build or image
    # transfer may overlap the running Cellar/RustFS qualification actors.
    driver.args.image=driver.docker('image','inspect','--format','{{.Id}}',driver.args.replacement_image)
    build=verify_image(driver.args.image,driver.docker)
    require(build['binary_sha256']==driver.report['build']['binary_sha256']
            and build['source_manifest_sha256']==driver.report['build']['source_manifest_sha256'],
            'replacement executable or source differs')
    driver.report['replacement_build']=build
    network=driver.docker('network','create','--label',driver.owner,driver.prefix)
    value=json.loads(driver.docker('network','inspect',network))[0]
    require(value['Labels'].get(LABEL)==driver.run_id,'replacement network ownership changed')
    subnet=ipaddress.ip_network(value['IPAM']['Config'][0]['Subnet'])
    require(subnet.version==4 and subnet.prefixlen<=24,'private replacement network required')
    driver.report['source_network']=driver.report['network']
    driver.report['network']={'id':value['Id'],'subnet':str(subnet)}
    gateway=command(['colima','ssh','--profile',profile(replacement),'--','getent','ahostsv4','host.lima.internal']).stdout.split()[0]
    require(ipaddress.ip_address(gateway).is_private,'private VM-to-host gateway required')
    relay=driver.docker('create','--label',driver.owner,'--network',driver.prefix,'--network-alias','rustfs',
        '--cpus','1','--memory','1g','--memory-swap','1g','-e','CELLAR_GROUP_ACTION=managed-s3-relay',driver.args.image)
    copy_managed(replacement,relay,driver.run_id,{'config.json':json.dumps({'upstream':gateway+':'+str(driver.args.port_base)}).encode()})
    driver.capture_resources(owned_container(driver.docker,relay,driver.run_id),1)
    driver.docker('start',relay)
    driver.until(lambda:'"managed_relay":"ready"' in driver.docker('logs',relay),30)
    driver.physical_host=new_host
    driver.report['deployment']={'source_context':source,'replacement_context':replacement,
        'source_host':old_host,'replacement_host':new_host,'relay':relay,
        'source_service_memory_gib':8,'replacement_memory_gib':16,
        'replacement_cpus':6,'physical_host_reused':False}
    driver.save()
    return driver.report['deployment']
