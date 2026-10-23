#!/usr/bin/env node
// Stage a target-specific, relocatable directory with the real Node runtime,
// native Cellar host, emitted modules, and installed npm dependencies.
import {cp, mkdir, mkdtemp, readFile, writeFile, rename, rm, readdir, lstat, chmod} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {resolve, dirname, join, relative, isAbsolute} from 'node:path';
import {fileURLToPath} from 'node:url';

const verifier = `import {readFile,readdir,lstat} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {dirname,join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
const root=dirname(fileURLToPath(import.meta.url));
const manifest=JSON.parse(await readFile(join(root,'node-package.json'),'utf8'));
if(manifest.format!==1 || manifest.kind!=='cellar-node-directory' || manifest.platform!==process.platform || manifest.arch!==process.arch || manifest.node!==process.versions.node) throw new Error('Node package runtime or platform mismatch');
const seen=new Set();
async function walk(base){for(const entry of await readdir(join(root,base),{withFileTypes:true})){const path=base?base+'/'+entry.name:entry.name; if(path==='node-package.json')continue; const full=join(root,path);const info=await lstat(full); if(info.isDirectory()){await walk(path);continue;} if(!info.isFile())throw new Error('Package contains unsupported file: '+path); const expected=manifest.files[path];if(!expected)throw new Error('Untracked package file: '+path); const actual=createHash('sha256').update(await readFile(full)).digest('hex');if(actual!==expected.sha256 || info.size!==expected.bytes)throw new Error('Package checksum mismatch: '+path);seen.add(path);}}
await walk(''); if(seen.size!==Object.keys(manifest.files).length)throw new Error('Package file missing');
`;
export async function pack(configFile, outputDirectory, {runtimeOnly = false} = {}) {
  const base = dirname(resolve(configFile));
  const config = JSON.parse(await readFile(configFile,'utf8'));
  for (const key of Object.keys(config)) if (!['host','node','application','entry','files'].includes(key)) throw new Error('Unknown Node package field: '+key);
  for (const key of (runtimeOnly ? ['application','entry'] : ['host','application','entry'])) if (typeof config[key] !== 'string') throw new Error('Missing '+key);
  if (!Array.isArray(config.files) || !config.files.length) throw new Error('Explicit application files required');
  const safe = path => typeof path==='string' && path.length>0 && !isAbsolute(path) && !/[\n\r\x00]/.test(path) && !path.split(/[\\/]/).some(p=>p==='..'||p==='.'||p==='');
  if (!safe(config.entry) || !config.files.every(safe)) throw new Error('Application paths must be relative and contained');
  const output=resolve(outputDirectory), app=resolve(base,config.application);
  if (output===app || output.startsWith(app+'/')) throw new Error('Package output must be outside the application');
  await mkdir(dirname(output),{recursive:true});
  try { await lstat(output); throw new Error('Package output already exists'); } catch(error) {if(error.code!=='ENOENT')throw error;}
  const stage=await mkdtemp(join(dirname(output),'.node-package-'));
  try {
    await mkdir(join(stage,'bin')); await mkdir(join(stage,'app'));
    if (!runtimeOnly) await cp(resolve(base,config.host),join(stage,'bin','cellar-host'),{dereference:true});
    const node= config.node ? resolve(base,config.node) : process.execPath;
    // Metadata is tied to the tool's actual Node runtime; reject a different
    // executable rather than silently writing incorrect ABI/platform metadata.
    const a=await readFile(node), b=await readFile(process.execPath);
    if(!a.equals(b)) throw new Error('Run the packager with the Node executable being packaged');
    await cp(node,join(stage,'bin','node'),{dereference:true});
    for(const file of config.files) await cp(resolve(app,file),join(stage,'app',file),{recursive:true,dereference:true});
    if(!(await lstat(join(stage,'app',config.entry))).isFile())throw new Error('Entry module missing');
    await writeFile(join(stage,'verify.mjs'),verifier);
    const shellEntry = "'" + config.entry.replaceAll("'", "'\"'\"'") + "'";
    if (!runtimeOnly) await writeFile(join(stage,'launch.sh'),`#!/bin/sh\nset -eu\nROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)\nunset NODE_OPTIONS NODE_PATH\n"$ROOT/bin/node" "$ROOT/verify.mjs"\nexport CELLAR_NODE_EXECUTABLE="$ROOT/bin/node"\nexport CELLAR_NODE_ENTRY="$ROOT/app/"${shellEntry}\nexec "$ROOT/bin/cellar-host" "$@"\n`);
    if (!runtimeOnly) await chmod(join(stage,'launch.sh'),0o755);
    const files={};
    async function walk(directory) {
      for(const entry of (await readdir(directory,{withFileTypes:true})).sort((a,b)=>a.name.localeCompare(b.name))) {
        const path=join(directory,entry.name), info=await lstat(path);
        if(info.isDirectory()){await walk(path);continue;}
        if(!info.isFile())throw new Error('Package contains unsupported file');
        const data=await readFile(path);
        files[relative(stage,path).split('\\').join('/')]={sha256:createHash('sha256').update(data).digest('hex'),bytes:data.length};
      }
    }
    await walk(stage);
    const manifest={format:1,kind:'cellar-node-directory',platform:process.platform,arch:process.arch,node:process.versions.node,entry:config.entry,files};
    await writeFile(join(stage,'node-package.json'),JSON.stringify(manifest,null,2)+'\n');
    await rename(stage,output); return manifest;
  } catch(error) {await rm(stage,{recursive:true,force:true});throw error;}
}
if(process.argv[1] && resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
  const runtimeOnly=process.argv[2]==='--runtime';
  const args=process.argv.slice(runtimeOnly?3:2);
  if(args.length!==2)throw new Error('Usage: node node-package.mjs [--runtime] project.json output-directory');
  await pack(args[0],args[1],{runtimeOnly});
}
