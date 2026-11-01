// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';

// Execute the actual Worker class with only the Cloudflare runtime base
// replaced. Container control and port calls are the boundary under test.
const source=(await readFile(new URL('./worker.js',import.meta.url),'utf8')).replace(
  'import { DurableObject } from "cloudflare:workers";',
  'class DurableObject { constructor(ctx, env) { this.ctx=ctx; this.env=env; } }');
const {CounterContainer}=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));

function fixture(running=true){
  const calls={timeout:0,start:0,health:0,requests:0};
  let fail=false;
  const container={
    running,
    start(){calls.start++;this.running=true;},
    async setInactivityTimeout(){calls.timeout++;},
    getTcpPort(){return {async fetch(request){
      const url=typeof request==='string'?request:request.url;
      if(url.endsWith('/health')){calls.health++;return new Response('{}');}
      calls.requests++;
      if(fail)throw new Error('Reply connection lost');
      return new Response('{"count":"1","version":"1"}');
    }};}
  };
  const context={container,blockConcurrencyWhile:callback=>callback()};
  const object=new CounterContainer(context,{R2_ENDPOINT:'endpoint',R2_ACCESS_KEY_ID:'key',R2_SECRET_ACCESS_KEY:'secret'});
  return {object,container,calls,setFail:value=>{fail=value;}};
}
const request=(method='GET')=>new Request('https://counter.example/api/counters/demo',{method});

test('warm requests reuse readiness and do not repeat container control calls',async()=>{
  const {object,calls}=fixture();
  await object.fetch(request());
  await object.fetch(request());
  assert.equal(calls.requests,2);
  assert.equal(calls.health,1);
  assert.equal(calls.timeout,1);
  assert.equal(calls.start,0);
});

test('concurrent cold requests share startup and a later stop requires readiness again',async()=>{
  const {object,container,calls}=fixture(false);
  await Promise.all([object.fetch(request()),object.fetch(request())]);
  assert.equal(calls.start,1);
  assert.equal(calls.health,1);
  assert.equal(calls.requests,2);
  container.running=false;
  await object.fetch(request());
  assert.equal(calls.start,2);
  assert.equal(calls.health,2);
});

test('a lost mutation reply is never automatically replayed',async()=>{
  const {object,calls,setFail}=fixture();
  setFail(true);
  await assert.rejects(object.fetch(request('POST')),/connection lost/);
  assert.equal(calls.requests,1);
});
