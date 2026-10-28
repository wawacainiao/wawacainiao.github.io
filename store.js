// Remote log store (keyvalue.immanuel.co). Values must be URL-safe; we store base64url(UTF-8 JSON) split in 180-char chunks.
const KV_BASE = 'https://keyvalue.immanuel.co/api/KeyVal';
const KV_APP = 'wrck6xdx';
function b64uEncode(str){const bytes=new TextEncoder().encode(str);let bin='';bytes.forEach(b=>bin+=String.fromCharCode(b));return btoa(bin).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');}
function b64uDecode(s){s=s.replace(/-/g,'+').replace(/_/g,'/');while(s.length%4)s+='=';const bin=atob(s);const bytes=new Uint8Array(bin.length);for(let i=0;i<bin.length;i++)bytes[i]=bin.charCodeAt(i);return new TextDecoder().decode(bytes);}
async function kvGet(key){const r=await fetch(`${KV_BASE}/GetValue/${KV_APP}/${key}?_=${Date.now()}`);if(!r.ok)throw new Error('get '+r.status);const v=await r.json();return (v===null||v===undefined)?'':String(v);}
async function kvSet(key,val,tries=3){for(let i=0;i<tries;i++){try{const r=await fetch(`${KV_BASE}/UpdateValue/${KV_APP}/${key}/${val}`,{method:'POST'});if(r.ok)return true;}catch(e){}await new Promise(r=>setTimeout(r,600*(i+1)));}return false;}
// Append an event {type:'visit'|'wish'|'gift', ...}. Adds time automatically.
async function logEvent(ev){
  ev.t=new Date().toISOString();
  let n=parseInt(await kvGet('count'))||0; n+=1;
  const enc=b64uEncode(JSON.stringify(ev)); const parts=[];
  for(let i=0;i<enc.length;i+=180)parts.push(enc.slice(i,i+180));
  for(let j=0;j<parts.length;j++)await kvSet(`e${n}p${j}`,parts[j]);
  await kvSet(`e${n}c`,String(parts.length));
  await kvSet('count',String(n));
  return n;
}
// Read all events (for the owner dashboard).
async function readAllEvents(){
  const n=parseInt(await kvGet('count'))||0; const out=[];
  for(let i=1;i<=n;i++){try{const c=parseInt(await kvGet(`e${i}c`))||0;let s='';for(let j=0;j<c;j++)s+=await kvGet(`e${i}p${j}`);if(s)out.push(JSON.parse(b64uDecode(s)));}catch(e){}}
  return out;
}
// Gift lock: key 'gift' holds 'none' or 'WISH-COUPON-YYYYMMDD-HHMM' / 'CARD-n-WANGWANG-YYYYMMDD-HHMM'.
async function getGift(){return kvGet('gift');}
async function setGift(token){return kvSet('gift',token);}
// Serialize writes so events never collide: use queueLog(ev) everywhere instead of logEvent directly.
let _logChain=Promise.resolve();
function queueLog(ev){_logChain=_logChain.then(()=>logEvent(ev)).catch(()=>{});return _logChain;}
