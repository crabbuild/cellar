import {createHash} from 'node:crypto';
import {setTimeout} from 'node:timers/promises';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {Readable} from 'node:stream';
import {hash, verify} from '@node-rs/argon2';
import {jsonResponse, request, response, type Context, type Request} from '@cellar/sdk';
import {z} from 'zod';
const require = createRequire(import.meta.url);
// CommonJS and component SSR use the ordinary Node package loader.
const {camelCase} = require('lodash');
const React = require('react');
const {renderToStaticMarkup} = require('react-dom/server');

export class Counter {
  async fetch(req: Request, ctx: Context) {
    const snapshot = await ctx.storage.read('count');
    let count = snapshot.present ? z.number().parse(snapshot.value) : 0;
    if (req.method === 'POST') {
      count += z.number().int().min(1).max(100).parse(req.json());
      await ctx.storage.apply({checks:{count:snapshot}, puts:{count}});
      await ctx.storage.sync();
    }
    return jsonResponse({count});
  }
}
export const cells = {Counter};

async function compatibility() {
  await setTimeout(2);
  const {basename} = await import('node:path');
  const pkg = JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8'));
  const digest = createHash('sha256').update(Buffer.from('cellar')).digest('hex');
  const encoded = await hash('cellar', {memoryCost:1024, timeCost:1, parallelism:1});
  const chunks = [];
  for await (const chunk of Readable.from(['node','stream'])) chunks.push(chunk);
  const fetched = await globalThis.fetch('data:text/plain,node-fetch');
  return {node:process.versions.node, esm:basename('/a/app.ts'), cjs:camelCase('cellar node'),
    nativeAddon:await verify(encoded,'cellar'), filesystem:pkg.name, digest,
    stream:chunks.join('-'), fetch:await fetched.text(),
    component:renderToStaticMarkup(React.createElement('strong',null,'Cellar + Node'))};
}
export async function fetch(req: Request, ctx: Context) {
  if (new URL(req.url).pathname === '/compatibility') return jsonResponse(await compatibility());
  const name = new URL(req.url).pathname.split('/').pop() || 'default';
  const result = await ctx.cell('COUNTERS',name).fetch(request('http://counter/',{method:req.method,headers:req.headers,body:req.body}));
  return response(result.body,{status:result.status,headers:result.headers});
}
