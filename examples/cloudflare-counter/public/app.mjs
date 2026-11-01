// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
import {parseTimings,resourceTimings,summary} from './stats.mjs';
const $ = id => document.getElementById(id);
const ms = value => Number.isFinite(value) ? value.toFixed(value < 10 ? 2 : 1) : '—';
const element = (tag, text, className) => {const e=document.createElement(tag); if(text!==undefined)e.textContent=text; if(className)e.className=className;return e;};
const samples=[];
const nextFrame=()=>new Promise(resolve=>{
  if(document.hidden){resolve(false);return;}
  const timeout=setTimeout(()=>{cancelAnimationFrame(frame);resolve(false);},200);
  const frame=requestAnimationFrame(()=>{clearTimeout(timeout);resolve(true);});
});
let busy=false, sampling=false, stopSampling=false, lastVersion=-1n;
let identity;
try {identity=JSON.parse(sessionStorage.getItem('cellar-counter-client') || 'null');} catch { /* A new client cannot replay an old identity. */ }
if (!identity || !/^[\da-f-]{36}$/.test(identity.client) || !Number.isSafeInteger(identity.sequence)) identity={client:crypto.randomUUID(),sequence:0,pending:null};
function persist(){try{sessionStorage.setItem('cellar-counter-client',JSON.stringify(identity));}catch{ /* The live tab retains the pending identity. */ }}
function message(text,error=false){$('notice').textContent=text;$('notice').className='notice'+(error?' error':'');$('notice').hidden=!text;}
function controls(){
  for(const id of ['increment','decrement','reset'])$(id).disabled=busy||Boolean(identity.pending)||sampling;
  $('custom').querySelector('button').disabled=busy||Boolean(identity.pending)||sampling;
  $('read').disabled=busy||sampling;$('sample').disabled=busy||sampling;$('probe').disabled=busy||sampling;
  $('retry').hidden=!identity.pending;$('retry').disabled=busy||sampling;$('stop').hidden=!sampling;
}
function showCounter(body){if(body.count!==undefined&&BigInt(body.version)>=lastVersion){lastVersion=BigInt(body.version);$('count').textContent=body.count;$('version').textContent=`Version ${body.version}`;}}
function renderLatest(sample){
  $('latest-total').textContent=ms(sample.timings.total);$('request-kind').textContent=`${sample.label} · ${sample.status || 'network error'}`;
  $('cold').hidden=!sample.cold;
  const rows=[['Browser fetch','total'],['To first byte','ttfb'],['Cloudflare edge','edge'],['Container ready','readiness'],['Container request','container'],['Cellar dispatch','cellar'],['SQLite work','sqlite'],['R2 durability gate','durability']];
  if(sample.operation==='probe')rows.push(['R2 PUT probe','r2_put'],['R2 GET probe','r2_get'],['R2 DELETE probe','r2_delete']);
  $('trace').replaceChildren(...rows.map(([label,key])=>{
    const row=element('div',undefined,'trace-row'+(key==='durability'?' gate':key==='edge'?' edge':''));
    const track=element('span',undefined,'track');const bar=element('i');
    const value=sample.timings[key];bar.style.width=Number.isFinite(value)?`${Math.min(100,value/Math.max(1,sample.timings.total)*100)}%`:'0%';track.append(bar);
    row.append(element('span',label),track,element('strong',ms(value)+' ms'));return row;
  }));
  const browser=[['DNS lookup','dns'],['TCP connection','tcp'],['TLS handshake','tls'],['Request → first byte','resource_ttfb'],['Body download','download'],['JSON decode','parse'],['UI → next frame','ui_frame']];
  $('browser-timings').replaceChildren(...browser.flatMap(([label,key])=>[element('dt',label),element('dd',`${ms(sample.timings[key])} ms`)]));
}
function renderHistory(){
  const filter=$('filter').value, metric=$('metric').value;
  const filtered=samples.filter(s=>(filter==='all'?s.operation!=='probe':s.operation===filter)&&($('include-cold').checked||!s.cold));
  $('sample-count').textContent=`${filtered.length} requests`;
  const visible=filtered.filter(s=>Number.isFinite(s.timings[metric])).slice(-100);const max=Math.max(1,...visible.map(s=>s.timings[metric]));
  $('chart').replaceChildren();
  if(!visible.length)$('chart').append(element('span','No measurements yet','placeholder'));
  for(const sample of visible){const bar=element('div',undefined,'bar '+(!sample.ok?'error':sample.operation==='read'?'read':''));bar.style.height=`${Math.max(2,sample.timings[metric]/max*130)}px`;bar.title=`${sample.label}: ${ms(sample.timings[metric])} ms (${sample.status||'network error'})`;$('chart').append(bar);}
  $('chart').append(element('span',`${ms(max)} ms`,'axis'));
  $('chart').setAttribute('aria-label',`${visible.length} recent requests. Maximum ${ms(max)} milliseconds.`);
  const values=summary(filtered,metric);
  const stats=[['p50',values.p50],['p95',values.p95],['p99',values.p99]];
  $('percentiles').replaceChildren(...stats.map(([label,value])=>{const d=element('div',undefined,'metric');d.append(element('div',label.toUpperCase()+' · '+$('metric').selectedOptions[0].text,'label'),element('div',`${ms(value)} ms`,'value'),element('div',`${values.count} observations`,'note'));return d;}));
  const failures=samples.filter(s=>!s.ok).length;const d=element('div',undefined,'metric');d.append(element('div','Unsuccessful requests','label'),element('div',`${failures} / ${samples.length}`,'value'),element('div','Network errors and HTTP refusals','note'));$('percentiles').append(d);
  const rows=samples.toReversed().map(sample=>{
    const tr=document.createElement('tr');
    const first=document.createElement('td');const b=element('button',sample.label,'text-button op '+(!sample.ok?'error':sample.operation==='read'?'read':''));b.addEventListener('click',()=>{$('request-detail').hidden=false;$('request-detail').textContent=JSON.stringify(sample,null,2);});first.append(b);tr.append(first);
    for(const text of [sample.ok?(sample.result?.count??'OK'):`${sample.status||'Error'}`,...['total','ttfb','sqlite','durability','container','edge'].map(k=>ms(sample.timings[k])),new Date(sample.at).toLocaleTimeString()])tr.append(element('td',String(text)));
    return tr;
  });
  if(!rows.length){const tr=document.createElement('tr');const td=element('td','Read or change the counter to capture a request.','empty');td.colSpan=9;tr.append(td);rows.push(tr);}
  $('journal').replaceChildren(...rows);
}
async function run(operation,command=null){
  if(busy)return false;busy=true;controls();
  const label=operation==='read'?'Read':operation==='probe'?'R2 probe':command.action==='reset'?'Reset':`Write ${command.delta>0?'+':''}${command.delta}`;
  const trace=crypto.randomUUID();const url=new URL(operation==='probe'?'/api/probe/r2':'/api/counters/demo',location.origin);url.searchParams.set('_trace',trace);
  if(command)for(const [k,v]of Object.entries(command))url.searchParams.set(k,String(v));
  const started=performance.now();const sample={at:new Date().toISOString(),operation,label,status:null,ok:false,cold:false,timings:{},client:command?.client,sequence:command?.sequence};
  let success=false;
  try{
    const response=await fetch(url,{method:operation==='read'?'GET':'POST',cache:'no-store',headers:{'x-counter-action':'1'},signal:AbortSignal.timeout(45000)});
    sample.timings.ttfb=performance.now()-started;sample.status=response.status;sample.cold=response.headers.get('x-counter-cold-start')==='true';sample.requestId=response.headers.get('x-request-id');
    const text=await response.text();sample.timings.total=performance.now()-started;
    const parsing=performance.now();const body=JSON.parse(text);sample.timings.parse=performance.now()-parsing;
    sample.timings={...sample.timings,...parseTimings(response.headers.get('server-timing'))};sample.result=body;sample.ok=response.ok;
    if(!response.ok)throw new Error(body.error || `HTTP ${response.status}`);
    showCounter(body);
    if(command){identity.sequence=command.sequence;identity.pending=null;persist();}
    message(identity.pending?'A previous action still needs confirmation. Retry its original identity.':'');
    $('connection').textContent='Connected';$('connection').className='status ready';success=true;
  }catch(error){
    sample.timings.total??=performance.now()-started;sample.error=error.message;
    const uncertainty=command?' The action may have committed. Use “Retry the same action safely”; it will not be applied twice.':'';
    message(error.message+uncertainty,true);$('connection').textContent='Request failed';$('connection').className='status';
  }finally{
    const rendering=performance.now();
    const firstFrame=await nextFrame();
    const entries=performance.getEntriesByName(url.href,'resource');Object.assign(sample.timings,resourceTimings(entries.at(-1)));
    samples.push(sample);if(samples.length>500)samples.shift();renderLatest(sample);renderHistory();
    const secondFrame=await nextFrame();if(firstFrame&&secondFrame)sample.timings.ui_frame=performance.now()-rendering;renderLatest(sample);
    performance.clearResourceTimings();busy=false;controls();
  }
  return success;
}
function mutate(action,delta){
  if(busy||identity.pending||sampling)return;
  identity.pending={client:identity.client,sequence:identity.sequence+1,action,delta};persist();void run('write',identity.pending);
}
$('increment').addEventListener('click',()=>mutate('add',1));$('decrement').addEventListener('click',()=>mutate('add',-1));
$('reset').addEventListener('click',()=>mutate('reset',0));$('read').addEventListener('click',()=>void run('read'));
$('custom').addEventListener('submit',event=>{event.preventDefault();const delta=Number($('delta').value);if(Number.isInteger(delta)&&Math.abs(delta)<=1000000)mutate('add',delta);});
$('retry').addEventListener('click',()=>{if(identity.pending)void run('write',identity.pending);});
$('probe').addEventListener('click',()=>void run('probe'));
$('sample').addEventListener('click',async()=>{sampling=true;stopSampling=false;controls();for(let i=0;i<10&&!stopSampling;i++){if(!await run('read'))break;await new Promise(resolve=>setTimeout(resolve,200));}sampling=false;controls();});
$('stop').addEventListener('click',()=>{stopSampling=true;});
$('clear').addEventListener('click',()=>{if(busy)return;samples.length=0;renderHistory();$('request-detail').hidden=true;});
$('export').addEventListener('click',()=>{const blob=new Blob([JSON.stringify({exportedAt:new Date().toISOString(),revision:$('revision').textContent,limitations:['Browser end-to-end includes all network hops. Server stages overlap. Durability is an R2 proof wait, not bare object API latency. UI-frame measurement estimates a rendering opportunity. Percentiles use the successful retained samples only. Cold starts are included.'],samples},null,2)],{type:'application/json'});const a=element('a');a.href=URL.createObjectURL(blob);a.download='cellar-counter-latency.json';a.click();setTimeout(()=>URL.revokeObjectURL(a.href),1000);});
for(const id of ['filter','metric','include-cold'])$(id).addEventListener('change',renderHistory);
renderHistory();controls();
if(identity.pending)message('An earlier action has an unconfirmed outcome. Retry its original identity before sending another write.');
fetch('/api/info',{cache:'no-store'}).then(r=>r.json()).then(info=>{$('revision').textContent=info.revision;$('revision').href=`https://github.com/crabbuild/cellar/commit/${encodeURIComponent(info.revision)}`;}).catch(()=>{$('revision').textContent='Version unavailable';});
void run('read');
