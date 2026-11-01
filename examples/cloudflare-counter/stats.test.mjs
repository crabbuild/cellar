// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {percentile,parseTimings,resourceTimings,summary} from './public/stats.mjs';
test('percentiles use observed nearest ranks and exclude unavailable values',()=>{
 assert.equal(percentile([null,NaN,1,2,3,4],.95),4); assert.equal(percentile([], .5),null);
 assert.equal(summary([{ok:false,timings:{total:999}},{ok:true,timings:{total:null}},{ok:true,timings:{total:12}}],'total').p99,12);
});
test('durations never invent absent server metrics or TLS measurements',()=>{
 assert.deepEqual(parseTimings('sqlite;dur=1.250, durability;dur=20, broken;dur=nan'),{sqlite:1.25,durability:20});
 assert.deepEqual(parseTimings(null),{});
 const entry={domainLookupStart:2,domainLookupEnd:2,connectStart:2,connectEnd:2,secureConnectionStart:0,requestStart:3,responseStart:12,responseEnd:14,transferSize:800};
 assert.equal(resourceTimings(entry).tls,null);assert.equal(resourceTimings(entry).dns,0);assert.equal(resourceTimings(entry).resource_ttfb,9);
});
