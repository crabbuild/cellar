import {build} from '../../javascript/node_modules/esbuild/lib/main.js';
import {spawnSync} from 'node:child_process';
import {mkdir} from 'node:fs/promises';
import {resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
const root = fileURLToPath(new URL('../../../', import.meta.url));
const out = resolve(process.argv[2] ?? `${root}/.cellar/javascript-perf-app`);
const entry = `${root}/tools/perf/javascript/app.ts`;
const checked = spawnSync(process.execPath, [`${root}/tools/javascript/node_modules/typescript/bin/tsc`,
  '--noEmit', '--strict', '--target', 'es2023', '--module', 'esnext', '--moduleResolution', 'bundler', entry], {stdio:'inherit'});
if (checked.error || checked.status !== 0) throw new Error('TypeScript check failed');
await mkdir(out, {recursive:true});
for (const [name, format, platform] of [['v8.js','iife','neutral'],['node.mjs','esm','node']]) {
  await build({entryPoints:[entry], bundle:true, outfile:`${out}/${name}`, format, platform,
    target:'es2023', ...(format === 'iife' ? {globalName:'CellarApp'} : {})});
}
console.log(out);
