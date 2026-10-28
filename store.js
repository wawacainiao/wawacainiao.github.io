// Remote log store (keyvalue.immanuel.co). Values must be URL-safe; we store base64url(UTF-8 JSON) split in 200-char chunks.
// The service is flaky (frequent 503s), so every call has a timeout + retries, writes go through a local outbox,
// and reads run in parallel with a local cache (events never change once written).
const KV_BASE = 'https://keyvalue.immanuel.co/api/KeyVal';
const KV_APP = 'wrck6xdx';
const CHUNK = 200;
function b64uEncode(str){const bytes=new TextEncoder().encode(str);let bin='';bytes.forEach(b=>bin+=String.fromCharCode(b));return btoa(bin).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');}
function b64uDecode(s){s=s.replace(/-/g,'+').replace(/_/g,'/');while(s.length%4)s+='=';const bin=atob(s);const bytes=new Uint8Array(bin.length);for(let i=0;i<bin.length;i++)bytes[i]=bin.charCodeAt(i);return new TextDecoder().decode(bytes);}
const _sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function _fetchT(url,opt,ms){const ac=new AbortController();const t=setTimeout(()=>ac.abort(),ms);try{return await fetch(url,Object.assign({signal:ac.signal,cache:'no-store'},opt||{}))}finally{clearTimeout(t)}}
async function kvGet(key,tries=6){let last;for(let i=0;i<tries;i++){try{const r=await _fetchT(`${KV_BASE}/GetValue/${KV_APP}/${key}?_=${Date.now()}${i}`,null,7000);
  if(r.ok){const v=await r.json();return (v===null||v===undefined)?'':String(v)}last=new Error('get '+r.status)}catch(e){last=e}await _sleep(250*(i+1)+Math.random()*200)}throw last||new Error('get failed')}
async function kvSet(key,val,tries=6){for(let i=0;i<tries;i++){try{const r=await _fetchT(`${KV_BASE}/UpdateValue/${KV_APP}/${key}/${val}`,{method:'POST',keepalive:true},7000);if(r.ok)return true}catch(e){}await _sleep(300*(i+1)+Math.random()*200)}return false}
// Append an event {type:'visit'|'wish'|'gift', ...}. Adds time automatically (unless already set by the outbox).
async function logEvent(ev){
  if(!ev.t)ev.t=new Date().toISOString();
  const enc=b64uEncode(JSON.stringify(ev));const parts=[];for(let i=0;i<enc.length;i+=CHUNK)parts.push(enc.slice(i,i+CHUNK));
  const n=(parseInt(await kvGet('count'))||0)+1;
  const ok=await Promise.all(parts.map((p,j)=>kvSet(`e${n}p${j}`,p)));
  if(ok.includes(false))throw new Error('chunk write failed');
  if(!await kvSet(`e${n}c`,String(parts.length)))throw new Error('meta write failed');
  // only move the counter forward (never back) in case another write landed meanwhile
  const cur=parseInt(await kvGet('count').catch(()=>'0'))||0;
  if(cur<n&&!await kvSet('count',String(n)))throw new Error('count write failed');
  return n;
}
// ---- outbox: events are saved locally first, removed once the server has them, retried on next visit ----
const OUTBOX='bd_outbox_v1';
function _obRead(){try{return JSON.parse(localStorage.getItem(OUTBOX)||'[]')}catch(e){return[]}}
function _obWrite(a){try{localStorage.setItem(OUTBOX,JSON.stringify(a))}catch(e){}}
let _logChain=Promise.resolve();
function _flushOne(id){_logChain=_logChain.then(async()=>{const it=_obRead().find(x=>x.id===id);if(!it)return;
  try{await logEvent(it.ev);_obWrite(_obRead().filter(x=>x.id!==id))}catch(e){}}).catch(()=>{});return _logChain}
function queueLog(ev){ev.t=ev.t||new Date().toISOString();const id=Date.now().toString(36)+Math.random().toString(36).slice(2,7);
  const a=_obRead();a.push({id,ev});_obWrite(a);return _flushOne(id)}
function flushOutbox(){_obRead().forEach(x=>_flushOne(x.id))}
// Read all events (for the owner dashboard). onUpdate(list,info) is called with partial results as they arrive.
const RCACHE='bd_events_cache_v1';
async function readAllEvents(onUpdate){
  let cache={};try{cache=JSON.parse(localStorage.getItem(RCACHE)||'{}')}catch(e){}
  const list=()=>Object.keys(cache).map(Number).sort((a,b)=>a-b).map(k=>cache[k]);
  if(onUpdate&&Object.keys(cache).length)onUpdate(list(),{cached:true});
  const n=parseInt(await kvGet('count',8))||0;
  const todo=[];for(let i=1;i<=n;i++)if(!cache[i])todo.push(i);
  let failed=0,idx=0;
  async function worker(){while(idx<todo.length){const i=todo[idx++];
    try{const c=parseInt(await kvGet(`e${i}c`))||0;if(!c)continue;
      const ps=await Promise.all(Array.from({length:c},(_,j)=>kvGet(`e${i}p${j}`)));
      cache[i]=JSON.parse(b64uDecode(ps.join('')));try{localStorage.setItem(RCACHE,JSON.stringify(cache))}catch(e){}
      if(onUpdate)onUpdate(list(),{done:false});
    }catch(e){failed++}}}
  await Promise.all(Array.from({length:Math.min(8,todo.length)},worker));
  return {events:list(),failed,total:n};
}
// Gift lock: key 'gift' holds 'none' or 'WISH-COUPON-YYYYMMDD-HHMM' / 'CARD-n-WANGWANG-YYYYMMDD-HHMM'.
async function getGift(){return kvGet('gift');}
async function setGift(token){return kvSet('gift',token);}
