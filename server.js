// Rundown — send YouTube videos from your phone to Meta Ray-Ban Display glasses.
//
// One file. No dependencies. No build step. Just: node server.js
//
// Storage is picked automatically:
//   REDIS_URL set  -> Redis, survives restarts
//   nothing set    -> memory, clears on restart (fine for a first test)

import http from 'node:http';
import net from 'node:net';
import tls from 'node:tls';

const PORT = process.env.PORT || 3000;
const REDIS_URL = process.env.REDIS_URL || process.env.REDIS_PRIVATE_URL || '';
const MAX_ITEMS = 50;

/* ══════════════════════════════════════════════════════════════════════════
   Redis, spoken directly over a socket so there is nothing to install
   ══════════════════════════════════════════════════════════════════════════ */

function encodeCommand(args) {
  let out = `*${args.length}\r\n`;
  for (const arg of args) {
    const value = String(arg);
    out += `$${Buffer.byteLength(value)}\r\n${value}\r\n`;
  }
  return out;
}

// Returns [value, bytesConsumed] or null when the buffer holds a partial reply.
function parseReply(buffer, offset = 0) {
  if (offset >= buffer.length) return null;
  const lineEnd = buffer.indexOf('\r\n', offset);
  if (lineEnd === -1) return null;

  const type = buffer.charCodeAt(offset);
  const head = buffer.slice(offset + 1, lineEnd);

  if (type === 43) return [head, lineEnd + 2];                       // + simple
  if (type === 45) return [new Error(String(head)), lineEnd + 2];    // - error
  if (type === 58) return [Number(head), lineEnd + 2];               // : integer

  if (type === 36) {                                                  // $ bulk
    const length = Number(head);
    if (length === -1) return [null, lineEnd + 2];
    const start = lineEnd + 2;
    if (buffer.length < start + length + 2) return null;
    return [buffer.slice(start, start + length), start + length + 2];
  }

  if (type === 42) {                                                  // * array
    const count = Number(head);
    if (count === -1) return [null, lineEnd + 2];
    const values = [];
    let cursor = lineEnd + 2;
    for (let i = 0; i < count; i++) {
      const next = parseReply(buffer, cursor);
      if (!next) return null;
      values.push(next[0]);
      cursor = next[1];
    }
    return [values, cursor];
  }

  throw new Error(`Unexpected reply from Redis: ${String.fromCharCode(type)}`);
}

class Redis {
  constructor(url) {
    const parsed = new URL(url);
    this.host = parsed.hostname;
    this.port = Number(parsed.port || 6379);
    this.secure = parsed.protocol === 'rediss:';
    this.username = decodeURIComponent(parsed.username || '');
    this.password = decodeURIComponent(parsed.password || '');
    this.socket = null;
    this.pending = [];
    this.buffer = '';
    this.ready = null;
  }

  connect() {
    if (this.socket && !this.socket.destroyed) return this.ready;

    this.ready = new Promise((resolve, reject) => {
      const options = {host: this.host, port: this.port};
      this.socket = this.secure
        ? tls.connect({...options, servername: this.host})
        : net.connect(options);

      this.socket.setKeepAlive(true, 30000);
      this.socket.setTimeout(0);

      this.socket.on('data', (chunk) => {
        this.buffer += chunk.toString('binary');
        for (;;) {
          let result;
          try {
            result = parseReply(this.buffer);
          } catch (error) {
            const waiting = this.pending.shift();
            if (waiting) waiting.reject(error);
            this.buffer = '';
            break;
          }
          if (!result) break;
          const [value, consumed] = result;
          this.buffer = this.buffer.slice(consumed);
          const waiting = this.pending.shift();
          if (!waiting) continue;
          if (value instanceof Error) waiting.reject(value);
          else waiting.resolve(value);
        }
      });

      const fail = (error) => {
        while (this.pending.length) this.pending.shift().reject(error);
        this.socket = null;
        reject(error);
      };

      this.socket.on('error', fail);
      this.socket.on('close', () => {
        while (this.pending.length) {
          this.pending.shift().reject(new Error('Redis connection closed'));
        }
        this.socket = null;
      });

      this.socket.on(this.secure ? 'secureConnect' : 'connect', async () => {
        try {
          if (this.password) {
            await this.send(
              this.username && this.username !== 'default'
                ? ['AUTH', this.username, this.password]
                : ['AUTH', this.password],
            );
          }
          resolve();
        } catch (error) {
          fail(error);
        }
      });
    });

    return this.ready;
  }

  send(args) {
    return new Promise((resolve, reject) => {
      if (!this.socket || this.socket.destroyed) {
        return reject(new Error('Redis is not connected'));
      }
      this.pending.push({resolve, reject});
      this.socket.write(encodeCommand(args));
    });
  }

  async command(args) {
    await this.connect();
    return this.send(args);
  }

  async get(key) {
    const value = await this.command(['GET', key]);
    return value == null ? null : Buffer.from(value, 'binary').toString('utf8');
  }

  async set(key, value) {
    return this.command(['SET', key, value]);
  }
}

/* ══════════════════════════════════════════════════════════════════════════
   Storage
   ══════════════════════════════════════════════════════════════════════════ */

const redis = REDIS_URL ? new Redis(REDIS_URL) : null;
const memory = new Map();

const storageName = redis ? 'Redis' : 'memory (queue clears on restart)';

function decodeItems(raw) {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

async function readQueue(token) {
  const key = `rundown:${token}`;
  if (redis) return decodeItems(await redis.get(key));
  return memory.get(key) || [];
}

async function writeQueue(token, items) {
  const key = `rundown:${token}`;
  if (redis) return void (await redis.set(key, JSON.stringify(items)));
  memory.set(key, items);
}

/* ══════════════════════════════════════════════════════════════════════════
   YouTube links
   ══════════════════════════════════════════════════════════════════════════ */

function parseVideoId(input) {
  if (!input) return null;
  const raw = String(input).trim();
  if (/^[\w-]{11}$/.test(raw)) return raw;

  let url;
  try {
    url = new URL(raw.startsWith('http') ? raw : `https://${raw}`);
  } catch {
    return null;
  }

  const host = url.hostname.replace(/^www\.|^m\./, '');
  if (host === 'youtu.be') return url.pathname.slice(1).split('/')[0] || null;
  if (host === 'youtube.com' || host === 'music.youtube.com') {
    const v = url.searchParams.get('v');
    if (v) return v;
    const match = url.pathname.match(/\/(shorts|embed|live|v)\/([\w-]{11})/);
    if (match) return match[2];
  }
  return null;
}

function parseStartSeconds(input) {
  try {
    const url = new URL(String(input).startsWith('http') ? input : `https://${input}`);
    const t = url.searchParams.get('t') || url.searchParams.get('start');
    if (!t) return 0;
    const compound = t.match(/^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s?)?$/);
    if (compound && (compound[1] || compound[2] || compound[3])) {
      return Number(compound[1] || 0) * 3600 + Number(compound[2] || 0) * 60 + Number(compound[3] || 0);
    }
    return Number.parseInt(t, 10) || 0;
  } catch {
    return 0;
  }
}

async function fetchMeta(videoId) {
  try {
    const res = await fetch(
      `https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v=${videoId}&format=json`,
      {signal: AbortSignal.timeout(6000)},
    );
    if (!res.ok) return {};
    const data = await res.json();
    return {title: data.title, channel: data.author_name};
  } catch {
    return {};
  }
}

/* ══════════════════════════════════════════════════════════════════════════
   The two pages
   ══════════════════════════════════════════════════════════════════════════ */

const GLASSES_PAGE = String.raw`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=600, height=600, initial-scale=1.0, user-scalable=no">
<meta name="description" content="Watch the YouTube videos you sent from your phone.">
<meta name="mrbd-web-app-capable" content="yes">
<link rel="icon" href="/icon.png" sizes="192x192">
<title>Rundown</title>
<style>
  :root { --ink:#000; --cue:#FFB000; --live:#FF4436; --text:#FFF; --muted:#8E9A97; --rail:#2A312F; }
  * { margin:0; padding:0; box-sizing:border-box; }
  html, body { width:600px; height:600px; overflow:hidden; background:var(--ink); color:var(--text);
    font-family:"Helvetica Neue",Helvetica,Arial,sans-serif; -webkit-font-smoothing:antialiased; }
  .mono { font-family:"SF Mono","Roboto Mono",Menlo,Consolas,monospace; font-variant-numeric:tabular-nums; letter-spacing:.04em; }
  .screen { display:none; width:600px; height:600px; flex-direction:column; position:relative; }
  .screen.active { display:flex; }

  /* The focus sink. Keyboard focus lives here whenever the player is up, so
     pinches reach this page instead of disappearing into the YouTube iframe. */
  #sink { position:absolute; top:0; left:0; width:1px; height:1px; opacity:0; border:none; background:none; }

  header { display:flex; align-items:baseline; justify-content:space-between; padding:24px 28px 16px; border-bottom:2px solid var(--rail); }
  .slug { font-size:26px; font-weight:700; letter-spacing:.14em; }
  .count { font-size:20px; color:var(--cue); }
  .rows { flex:1; padding:8px 0; }
  .row { display:flex; align-items:center; gap:20px; width:100%; min-height:88px; padding:14px 28px 14px 20px;
    background:transparent; border:none; border-left:8px solid transparent; color:var(--text); text-align:left; cursor:pointer;
    transition:border-color 120ms ease, background 120ms ease; }
  .row:focus, .row.focused { outline:none; border-left-color:var(--cue); background:rgba(255,176,0,.12); }
  .row.armed { border-left-color:var(--live); background:rgba(255,68,54,.16); }
  .row.armed .row-num { color:var(--live); }
  .row-remove { display:block; font-size:17px; color:var(--live); margin-top:4px; letter-spacing:.1em; }
  .row-num { font-size:22px; color:var(--muted); width:44px; flex-shrink:0; }
  .row:focus .row-num, .row.focused .row-num { color:var(--cue); }
  .row-body { min-width:0; }
  .row-title { font-size:24px; line-height:1.25; font-weight:600; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; }
  .row-channel { font-size:17px; color:var(--muted); margin-top:4px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  .notice { flex:1; display:flex; flex-direction:column; justify-content:center; padding:0 40px; gap:14px; }
  .notice-head { font-size:28px; font-weight:700; }
  .notice-body { font-size:20px; line-height:1.45; color:var(--muted); }
  .notice-body strong { color:var(--cue); font-weight:600; }
  footer { padding:14px 28px 22px; font-size:16px; color:var(--muted); border-top:2px solid var(--rail); }

  /* ---- Player: video fills the display, everything else floats over it ---- */
  #playerScreen { display:none; }
  #playerScreen.active { display:block; }

  .stage { position:absolute; inset:0; display:flex; align-items:center; justify-content:center; overflow:hidden; background:var(--ink); }
  /* Nothing inside the iframe can be clicked or focused. All control goes
     through the player API instead, so YouTube's own UI is unreachable. */
  .stage iframe { border:0; display:block; pointer-events:none; }

  .chrome { position:absolute; inset:0; pointer-events:none; opacity:1; transition:opacity 400ms ease; }
  .chrome.hidden { opacity:0; }
  .chrome-top { position:absolute; top:0; left:0; right:0; padding:18px 26px 26px;
    background:linear-gradient(to bottom, rgba(0,0,0,.85), rgba(0,0,0,0)); }
  .chrome-bottom { position:absolute; bottom:0; left:0; right:0; padding:26px 26px 18px;
    background:linear-gradient(to top, rgba(0,0,0,.85), rgba(0,0,0,0)); }
  .now-label { font-size:15px; letter-spacing:.18em; color:var(--live); }
  .now-title { font-size:22px; font-weight:600; line-height:1.2; margin-top:6px;
    display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden;
    text-shadow:0 2px 8px rgba(0,0,0,.9); }
  .track { height:5px; background:rgba(255,255,255,.22); }
  .track-fill { height:5px; width:0%; background:var(--cue); transition:width 220ms linear; }
  .readout { display:flex; justify-content:space-between; align-items:center; margin-top:10px; font-size:18px;
    text-shadow:0 2px 8px rgba(0,0,0,.9); }
  .state { color:var(--live); letter-spacing:.12em; }
  .state.paused { color:var(--muted); }
  .rate { color:var(--cue); letter-spacing:.06em; }

  .stage-veil { position:absolute; inset:0; display:none; align-items:center; justify-content:center;
    background:var(--ink); font-size:21px; color:var(--cue); text-align:center; padding:0 40px; line-height:1.4; }
  .stage-veil.show { display:flex; }

  /* ---- Gesture diagnostics, on with ?debug=1 ---- */
  .debug { position:absolute; top:0; right:0; display:none; padding:10px 14px; background:rgba(0,0,0,.85);
    border-left:3px solid var(--cue); font-size:15px; line-height:1.5; color:var(--cue); text-align:right; z-index:99; }
  .debug.on { display:block; }
  .debug .miss { color:var(--live); }

  @media (prefers-reduced-motion:reduce) { * { transition:none !important; } }
</style>
</head>
<body>

<section id="listScreen" class="screen active">
  <header><span class="slug mono">RUNDOWN</span><span id="count" class="count mono">--</span></header>
  <div id="rows" class="rows"></div>
  <footer id="listHint" class="mono">SWIPE TO MOVE &nbsp;&middot;&nbsp; PINCH TO PLAY &nbsp;&middot;&nbsp; RIGHT TO REMOVE</footer>
</section>

<section id="playerScreen" class="screen">
  <button id="sink" aria-label="Player controls"></button>
  <div class="stage"><div id="player"></div></div>
  <div id="chrome" class="chrome">
    <div class="chrome-top">
      <div id="nowLabel" class="now-label mono">ON AIR</div>
      <div id="nowTitle" class="now-title">&nbsp;</div>
    </div>
    <div class="chrome-bottom">
      <div class="track"><div id="trackFill" class="track-fill"></div></div>
      <div class="readout mono"><span id="state" class="state">PLAYING</span><span id="rate" class="rate">1.0x</span><span id="time">0:00 / 0:00</span></div>
    </div>
  </div>
  <div id="veil" class="stage-veil"></div>
</section>

<div id="debug" class="debug mono"></div>

<script>
(function () {
  'use strict';
  var POLL_MS = 8000, WINDOW = 5, SEEK_STEP = 10, CHROME_MS = 2500;

  var params = new URLSearchParams(location.search);
  var DEBUG = params.get('debug') === '1';
  var ZOOM = params.get('zoom') === 'fill' ? 'fill' : 'fit';
  // Up/down swipes drive playback speed by default. Pass ?updown=volume to
  // use them for volume instead.
  var UPDOWN = params.get('updown') === 'volume' ? 'volume' : 'speed';

  var SPEEDS = [0.5, 0.75, 1, 1.25, 1.5, 1.75, 2, 2.5, 3];
  var speed = (function(){
    var fromUrl = parseFloat(params.get('speed'));
    if (fromUrl && SPEEDS.indexOf(fromUrl) !== -1) {
      try{ localStorage.setItem('rundown.speed', String(fromUrl)); }catch(e){}
      return fromUrl;
    }
    try {
      var saved = parseFloat(localStorage.getItem('rundown.speed'));
      if (saved && SPEEDS.indexOf(saved) !== -1) return saved;
    } catch(e){}
    return 2;
  })();

  var token=null, items=[], focusIndex=0, windowStart=0;
  var player=null, playerReady=false, current=null;
  var ticker=null, chromeTimer=null, refocusTimer=null, view='list';
  var keyCount=0, lastKey='none';

  var el={};
  ['listScreen','playerScreen','rows','count','nowTitle','nowLabel','veil','listHint',
   'trackFill','state','time','rate','chrome','sink','debug']
    .forEach(function(id){ el[id]=document.getElementById(id); });

  /* ---------- diagnostics ---------- */
  function paintDebug(note){
    if(!DEBUG) return;
    el.debug.classList.add('on');
    var active = document.activeElement;
    var where = active === el.sink ? 'sink'
      : active && active.tagName === 'IFRAME' ? '<span class="miss">IFRAME</span>'
      : active && active.className ? String(active.className).split(' ')[0]
      : active ? active.tagName.toLowerCase() : 'none';
    el.debug.innerHTML =
      'view ' + view + '<br>keys ' + keyCount + '<br>last ' + lastKey +
      '<br>focus ' + where + (note ? '<br>' + note : '');
  }

  /* ---------- token ---------- */
  function resolveToken(){
    var fromUrl = params.get('token');
    if(fromUrl){ try{localStorage.setItem('rundown.token',fromUrl);}catch(e){} return fromUrl; }
    try{ return localStorage.getItem('rundown.token'); }catch(e){ return null; }
  }
  function esc(v){ return String(v==null?'':v).replace(/[&<>"']/g,function(c){
    return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }
  function clockFace(s){
    if(!isFinite(s)||s<0) s=0; s=Math.floor(s);
    var h=Math.floor(s/3600), m=Math.floor((s%3600)/60), r=s%60;
    var pad=function(n){return n<10?'0'+n:String(n);};
    return h>0 ? h+':'+pad(m)+':'+pad(r) : m+':'+pad(r);
  }

  /* ---------- rundown ---------- */
  function showNotice(head,body){
    el.rows.innerHTML='<div class="notice"><div class="notice-head">'+head+'</div><div class="notice-body">'+body+'</div></div>';
  }
  function renderList(){
    el.count.textContent = items.length ? String(items.length).padStart(2,'0') : '--';
    if(!token){ showNotice('No token yet','Open this app with <strong>?token=yourword</strong> on the end of the address, then use the same word on your phone.'); return; }
    if(!items.length){ showNotice('Nothing queued','Share a YouTube link from your phone and it lands here within a few seconds.'); return; }
    if(focusIndex<windowStart) windowStart=focusIndex;
    if(focusIndex>windowStart+WINDOW-1) windowStart=focusIndex-WINDOW+1;
    el.rows.innerHTML=items.slice(windowStart,windowStart+WINDOW).map(function(item,i){
      var n=windowStart+i;
      var armed=item.id===armedId;
      var sub='';
      if(armed){
        sub='<span class="row-remove">PINCH TO REMOVE &middot; LEFT TO KEEP</span>';
      } else {
        var bits=[];
        if(item.channel) bits.push(esc(item.channel));
        if(item.position>10) bits.push('resume '+clockFace(item.position));
        if(bits.length) sub='<span class="row-channel">'+bits.join(' &middot; ')+'</span>';
      }
      return '<button class="row focusable'+(armed?' armed':'')+'" data-index="'+n+'">'+
        '<span class="row-num mono">'+String(n+1).padStart(2,'0')+'</span>'+
        '<span class="row-body"><span class="row-title">'+esc(item.title)+'</span>'+sub+'</span></button>';
    }).join('');
    Array.prototype.forEach.call(el.rows.querySelectorAll('.row'),function(node){
      node.addEventListener('click',function(){ openPlayer(Number(this.getAttribute('data-index'))); });
    });
    var target=el.rows.querySelector('[data-index="'+focusIndex+'"]');
    if(target){ target.classList.add('focused'); target.focus(); }
    setHint();
    paintDebug();
  }
  function loadQueue(){
    if(!token){ renderList(); return Promise.resolve(); }
    return fetch('/api/queue?token='+encodeURIComponent(token),{cache:'no-store'})
      .then(function(r){return r.json();})
      .then(function(data){
        if(!data||!Array.isArray(data.items)) return;
        var changed=data.items.length!==items.length||data.items.some(function(it,i){return !items[i]||items[i].id!==it.id;});
        items=data.items;
        if(focusIndex>items.length-1) focusIndex=Math.max(0,items.length-1);
        if(view==='list'&&changed) renderList();
        else if(view==='list') el.count.textContent=items.length?String(items.length).padStart(2,'0'):'--';
      })
      .catch(function(){ if(view==='list'&&!items.length) showNotice('Cannot reach the queue','Check the connection, then middle pinch and restart.'); });
  }
  function dropItem(id){
    items=items.filter(function(it){return it.id!==id;});
    try{ localStorage.removeItem('rundown.pos.'+id); }catch(e){}
    fetch('/api/queue?token='+encodeURIComponent(token)+'&id='+encodeURIComponent(id),{method:'DELETE'}).catch(function(){});
  }

  /* ---------- removing from the list ----------
     Right swipe arms the cued row, then a pinch removes it. Two deliberate
     actions, so a stray gesture never deletes anything. */
  var armedId=null, armTimer=null;

  function setHint(){
    if(!el.listHint) return;
    el.listHint.innerHTML = armedId
      ? 'PINCH TO REMOVE &nbsp;&middot;&nbsp; LEFT OR UP TO KEEP'
      : 'SWIPE TO MOVE &nbsp;&middot;&nbsp; PINCH TO PLAY &nbsp;&middot;&nbsp; RIGHT TO REMOVE';
  }
  function disarm(){
    if(armTimer){ clearTimeout(armTimer); armTimer=null; }
    if(!armedId) return;
    armedId=null; renderList();
  }
  function arm(){
    if(!items.length||!items[focusIndex]) return;
    armedId=items[focusIndex].id;
    renderList();
    if(armTimer) clearTimeout(armTimer);
    armTimer=setTimeout(disarm,5000);   // forget about it and it stands down
  }
  function removeArmed(){
    if(!armedId) return;
    var id=armedId;
    armedId=null;
    if(armTimer){ clearTimeout(armTimer); armTimer=null; }
    dropItem(id);
    if(focusIndex>items.length-1) focusIndex=Math.max(0,items.length-1);
    renderList();
  }

  /* ---------- focus lock ----------
     The iframe grabs focus when a video loads. Pinch fires Enter on whatever
     holds focus, so if we let it drift into the iframe, every gesture lands in
     YouTube's own UI instead of here. We pull it back, repeatedly. */
  function holdFocus(){
    if(view!=='player') return;
    if(document.activeElement !== el.sink){
      try{ el.sink.focus({preventScroll:true}); }catch(e){ try{ el.sink.focus(); }catch(e2){} }
    }
  }
  function startFocusGuard(){
    stopFocusGuard();
    holdFocus();
    [80,250,600,1200,2500].forEach(function(delay){ setTimeout(holdFocus,delay); });
    refocusTimer=setInterval(holdFocus,1500);
  }
  function stopFocusGuard(){ if(refocusTimer){ clearInterval(refocusTimer); refocusTimer=null; } }

  /* ---------- chrome ---------- */
  function showChrome(){
    el.chrome.classList.remove('hidden');
    if(chromeTimer) clearTimeout(chromeTimer);
    chromeTimer=setTimeout(function(){ el.chrome.classList.add('hidden'); },CHROME_MS);
  }
  function setState(text,live){ el.state.textContent=text; el.state.className='state'+(live?'':' paused'); }

  /* ---------- captions ----------
     YouTube turns captions back on per video depending on the channel's
     settings, and the module can load a beat after playback starts, so we
     unload it repeatedly rather than once. */
  function killCaptions(){
    if(!player) return;
    ['captions','cc'].forEach(function(module){
      try{ player.unloadModule(module); }catch(e){}
    });
  }
  function killCaptionsHard(){
    killCaptions();
    [200,600,1500,3000].forEach(function(delay){ setTimeout(killCaptions,delay); });
  }

  /* ---------- playback speed ----------
     Rate resets to 1x on every load, so it gets reapplied each time. Some
     videos only offer a subset of rates, so we fall back to the nearest. */
  function paintRate(){
    if(el.rate) el.rate.textContent = speed.toFixed(2).replace(/0$/,'').replace(/\.$/,'') + 'x';
  }
  function applySpeed(){
    if(!player||!playerReady) return;
    try{
      var available = player.getAvailablePlaybackRates() || [];
      var target = speed;
      if(available.length && available.indexOf(target) === -1){
        target = available.reduce(function(best,rate){
          return Math.abs(rate-speed) < Math.abs(best-speed) ? rate : best;
        }, available[0]);
      }
      player.setPlaybackRate(target);
    }catch(e){}
    paintRate();
  }
  function nudgeSpeed(direction){
    var index = SPEEDS.indexOf(speed);
    if(index === -1) index = SPEEDS.indexOf(1);
    index = Math.max(0, Math.min(SPEEDS.length-1, index + direction));
    speed = SPEEDS[index];
    try{ localStorage.setItem('rundown.speed', String(speed)); }catch(e){}
    applySpeed();
    el.nowLabel.textContent = 'SPEED ' + speed + 'x';
    setTimeout(function(){ el.nowLabel.textContent='ON AIR'; },1200);
  }

  function startTicker(){
    stopTicker();
    ticker=setInterval(function(){
      if(!player||!playerReady||view!=='player') return;
      if(pendingSeek!==null) return;   // a scrub preview owns the readout
      try{
        var now=player.getCurrentTime()||0, total=player.getDuration()||0;
        el.time.textContent=clockFace(now)+' / '+clockFace(total);
        el.trackFill.style.width=total?(now/total)*100+'%':'0%';
      }catch(e){}
      saveProgress(false);
    },500);
  }
  function stopTicker(){ if(ticker){ clearInterval(ticker); ticker=null; } }

  function sizeStage(){
    var frame=document.querySelector('.stage iframe');
    if(!frame) return;
    if(ZOOM==='fill'){ frame.style.width='1067px'; frame.style.height='600px'; }
    else { frame.style.width='600px'; frame.style.height='338px'; }
  }

  function openPlayer(index){
    var item=items[index]; if(!item) return;
    current=item; focusIndex=index; view='player';
    el.listScreen.classList.remove('active'); el.playerScreen.classList.add('active');
    el.nowTitle.textContent=item.title; el.veil.classList.remove('show');
    setState('CUEING',false); el.trackFill.style.width='0%'; el.time.textContent='0:00 / 0:00';
    var resumeAt=resumePointFor(item);
    history.pushState({screen:'player',id:item.id},'');
    if(playerReady&&player) player.loadVideoById({videoId:item.videoId,startSeconds:resumeAt});
    sizeStage(); startTicker(); showChrome(); startFocusGuard(); paintDebug();
    if(resumeAt>10){
      el.nowLabel.textContent='RESUMED '+clockFace(resumeAt);
      setTimeout(function(){ el.nowLabel.textContent='ON AIR'; },2000);
    }
  }
  function closePlayer(){
    if(seekCommit){ clearTimeout(seekCommit); seekCommit=null; }
    if(pendingSeek!==null){ try{ player.seekTo(pendingSeek,true); }catch(e){} pendingSeek=null; }
    saveProgress(true);
    view='list'; stopTicker(); stopFocusGuard();
    if(player&&playerReady){ try{player.stopVideo();}catch(e){} }
    el.playerScreen.classList.remove('active'); el.listScreen.classList.add('active');
    renderList();
  }
  function playNext(){
    if(!current) return closePlayer();
    var finished=current.id;
    var position=items.findIndex(function(it){return it.id===finished;});
    dropItem(finished);
    if(position>-1&&position<items.length) openPlayer(position);
    else if(items.length) openPlayer(Math.max(0,items.length-1));
    else history.back();
  }

  /* ---------- YouTube ---------- */
  window.onYouTubeIframeAPIReady=function(){
    player=new YT.Player('player',{
      width:600,height:338,
      playerVars:{
        controls:0, modestbranding:1, rel:0, playsinline:1,
        iv_load_policy:3, disablekb:1, fs:0, cc_load_policy:0,
        origin:location.origin
      },
      events:{
        onReady:function(){
          playerReady=true; sizeStage(); killCaptionsHard(); paintRate();
          if(view==='player'&&current){
            player.loadVideoById({videoId:current.videoId,startSeconds:current.start||0});
            startFocusGuard();
          }
        },
        onStateChange:function(e){
          if(e.data===YT.PlayerState.PLAYING){
            setState('ON AIR',true);
            applySpeed();       // rate resets on each load
            killCaptionsHard(); // so does the caption track
          }
          else if(e.data===YT.PlayerState.PAUSED) setState('HELD',false);
          else if(e.data===YT.PlayerState.BUFFERING){ setState('LOADING',false); killCaptions(); }
          else if(e.data===YT.PlayerState.ENDED){ setState('DONE',false); playNext(); }
          holdFocus(); paintDebug();
        },
        onPlaybackRateChange:function(){ paintRate(); },
        onError:function(){
          el.veil.textContent="This video won't play outside YouTube. Skipping in a moment.";
          el.veil.classList.add('show'); setState('BLOCKED',false); showChrome();
          setTimeout(function(){ if(view==='player') playNext(); },4000);
        }
      }
    });
  };

  /* ---------- input ---------- */
  function moveFocus(delta){ if(!items.length) return; focusIndex=(focusIndex+delta+items.length)%items.length; renderList(); }
  function nudgeVolume(delta){
    if(!player||!playerReady) return;
    try{
      var next=Math.max(0,Math.min(100,(player.getVolume()||0)+delta));
      player.setVolume(next); if(next===0) player.mute(); else player.unMute();
      el.nowLabel.textContent='VOLUME '+next;
      setTimeout(function(){ el.nowLabel.textContent='ON AIR'; },1200);
    }catch(e){}
  }
  function seek(delta){
    if(!player||!playerReady) return;
    try{ player.seekTo(Math.max(0,(player.getCurrentTime()||0)+delta),true); }catch(e){}
  }

  /* ---------- scrubbing ----------
     Keep swiping the same way and the step grows: 10s, then 30s, a minute,
     five, ten. Nothing is sent to the player until you stop for a moment, so
     crossing twenty minutes costs one seek instead of a hundred. */
  var SEEK_STEPS=[10,10,30,30,60,60,300,300,600];
  var pendingSeek=null, seekCommit=null, seekStreak=-1, lastSeekAt=0;

  function scrub(direction){
    if(!player||!playerReady) return;
    var now=Date.now();
    seekStreak = (now-lastSeekAt < 1200) ? seekStreak+1 : 0;
    lastSeekAt=now;

    var step=SEEK_STEPS[Math.min(seekStreak,SEEK_STEPS.length-1)];
    var total=0, base=0;
    try{ total=player.getDuration()||0; base=(pendingSeek===null)?(player.getCurrentTime()||0):pendingSeek; }catch(e){ return; }

    var target=base+(step*direction);
    if(target<0) target=0;
    if(total && target>total-2) target=Math.max(0,total-2);
    pendingSeek=target;

    // Preview only. The ticker leaves these alone while a scrub is pending.
    el.time.textContent=clockFace(target)+' / '+clockFace(total);
    el.trackFill.style.width=total?(target/total)*100+'%':'0%';
    el.nowLabel.textContent=(direction>0?'AHEAD ':'BACK ')+clockFace(step);
    showChrome();

    if(seekCommit) clearTimeout(seekCommit);
    seekCommit=setTimeout(commitSeek,450);
  }

  function commitSeek(){
    if(pendingSeek===null) return;
    var target=pendingSeek;
    pendingSeek=null; seekStreak=-1;
    try{ player.seekTo(target,true); }catch(e){}
    el.nowLabel.textContent='ON AIR';
    saveProgress(true);
  }

  /* ---------- resume ----------
     Position is written to the queue every few seconds, and again on the way
     out, so closing mid-podcast picks up where you left off. */
  var lastSaved=0;
  function saveProgress(force){
    if(!current||!player||!playerReady||!token) return;
    var at=0;
    try{ at=player.getCurrentTime()||0; }catch(e){ return; }
    if(at<5) return;
    var now=Date.now();
    if(!force && now-lastSaved<5000) return;
    lastSaved=now;

    var position=Math.floor(at);
    current.position=position;
    try{ localStorage.setItem('rundown.pos.'+current.id,String(position)); }catch(e){}

    var payload=JSON.stringify({id:current.id,position:position});
    var endpoint='/api/queue?token='+encodeURIComponent(token);
    if(force && navigator.sendBeacon){
      try{ navigator.sendBeacon(endpoint,new Blob([payload],{type:'application/json'})); return; }catch(e){}
    }
    fetch(endpoint,{method:'POST',headers:{'Content-Type':'application/json'},body:payload}).catch(function(){});
  }

  function resumePointFor(item){
    var saved=item.position||0;
    try{
      var local=parseInt(localStorage.getItem('rundown.pos.'+item.id),10);
      if(local>saved) saved=local;
    }catch(e){}
    return saved>10 ? saved : (item.start||0);
  }
  function togglePlay(){
    if(!player||!playerReady) return;
    try{ var s=player.getPlayerState(); if(s===YT.PlayerState.PLAYING) player.pauseVideo(); else player.playVideo(); }catch(e){}
  }

  // Capture phase, on window, so we see the event before anything else can.
  window.addEventListener('keydown',function(e){
    keyCount++; lastKey=e.key;
    var handled=true;
    if(view==='list'){
      switch(e.key){
        case 'ArrowUp': disarm(); moveFocus(-1); break;
        case 'ArrowDown': disarm(); moveFocus(1); break;
        case 'ArrowRight': arm(); break;
        case 'ArrowLeft': disarm(); break;
        case 'Enter':
          if(armedId) removeArmed();
          else if(items.length) openPlayer(focusIndex);
          break;
        default: handled=false;
      }
    } else {
      showChrome();
      switch(e.key){
        case 'Enter': togglePlay(); break;
        case 'ArrowLeft': scrub(-1); break;
        case 'ArrowRight': scrub(1); break;
        case 'ArrowUp':
          if(UPDOWN==='volume') nudgeVolume(10); else nudgeSpeed(1);
          break;
        case 'ArrowDown':
          if(UPDOWN==='volume') nudgeVolume(-10); else nudgeSpeed(-1);
          break;
        case 'Escape': case 'Backspace': history.back(); break;
        default: handled=false;
      }
      holdFocus();
    }
    paintDebug();
    if(handled){ e.preventDefault(); e.stopPropagation(); }
  },true);

  // If focus escapes into the iframe, snatch it straight back.
  window.addEventListener('blur',function(){ setTimeout(holdFocus,50); });
  document.addEventListener('focusout',function(){ setTimeout(holdFocus,50); });

  window.addEventListener('pagehide',function(){ if(view==='player') saveProgress(true); });
  document.addEventListener('visibilitychange',function(){
    if(document.hidden){ if(view==='player') saveProgress(true); }
    else holdFocus();
  });

  window.addEventListener('popstate',function(event){
    var screen=event.state&&event.state.screen;
    if(screen==='player') return;
    if(view==='player') closePlayer();
  });

  /* ---------- boot ---------- */
  token=resolveToken();
  history.replaceState({screen:'list'},'');
  renderList();
  if(DEBUG) paintDebug();
  var tag=document.createElement('script');
  tag.src='https://www.youtube.com/iframe_api';
  document.head.appendChild(tag);
  if(token){ loadQueue(); setInterval(function(){ if(view==='list') loadQueue(); },POLL_MS); }
})();
</script>
</body>
</html>`;

const PHONE_PAGE = String.raw`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#0B0D0C">
<meta name="apple-mobile-web-app-capable" content="yes">
<link rel="icon" href="/icon.png" sizes="192x192">
<link rel="apple-touch-icon" href="/icon.png">
<link rel="manifest" href="/manifest.webmanifest">
<title>Rundown &middot; Send to glasses</title>
<style>
  :root { --ink:#0B0D0C; --panel:#141817; --rail:#262E2C; --cue:#FFB000; --live:#FF4436; --text:#ECEAE4; --muted:#7F8C89; }
  * { margin:0; padding:0; box-sizing:border-box; -webkit-tap-highlight-color:transparent; }
  body { background:var(--ink); color:var(--text); font-family:ui-sans-serif,-apple-system,"Segoe UI",Roboto,sans-serif;
    min-height:100dvh; padding:max(20px,env(safe-area-inset-top)) 20px 40px; }
  .mono { font-family:ui-monospace,"SF Mono",Menlo,Consolas,monospace; font-variant-numeric:tabular-nums; letter-spacing:.05em; }
  .wrap { max-width:560px; margin:0 auto; }
  header { display:flex; align-items:baseline; justify-content:space-between; padding-bottom:16px; border-bottom:2px solid var(--rail); }
  h1 { font-size:20px; letter-spacing:.16em; font-weight:700; }
  .count { color:var(--cue); font-size:16px; }
  .compose { margin:24px 0 8px; }
  .paste { width:100%; padding:20px; font-size:17px; margin-bottom:20px; }
  label { display:block; font-size:13px; letter-spacing:.1em; color:var(--muted); margin-bottom:10px; }
  .field { display:flex; gap:10px; }
  input[type="url"] { flex:1; min-width:0; background:var(--panel); border:2px solid var(--rail); color:var(--text);
    font-size:17px; padding:15px 14px; border-radius:2px; }
  input:focus { outline:none; border-color:var(--cue); }
  button { background:var(--cue); color:#10120F; border:none; font-size:16px; font-weight:700; letter-spacing:.08em;
    padding:15px 22px; border-radius:2px; cursor:pointer; }
  button:disabled { opacity:.45; }
  button.ghost { background:transparent; color:var(--muted); border:2px solid var(--rail); font-weight:600; padding:11px 16px; font-size:14px; }
  .status { margin-top:14px; font-size:15px; min-height:22px; }
  .status.ok { color:var(--cue); } .status.bad { color:var(--live); }
  .list { margin-top:32px; }
  .list-head { display:flex; justify-content:space-between; align-items:center; font-size:13px; letter-spacing:.1em;
    color:var(--muted); padding-bottom:12px; border-bottom:2px solid var(--rail); }
  .item { display:flex; gap:14px; align-items:flex-start; padding:16px 0; border-bottom:1px solid var(--rail); }
  .item-num { color:var(--muted); font-size:14px; padding-top:3px; width:26px; flex-shrink:0; }
  .item-body { flex:1; min-width:0; }
  .item-title { font-size:16px; line-height:1.35; font-weight:600; }
  .item-channel { font-size:13px; color:var(--muted); margin-top:3px; }
  .item-drop { background:none; border:none; color:var(--muted); font-size:22px; line-height:1; padding:4px 6px; cursor:pointer; }
  .empty { padding:30px 0; color:var(--muted); font-size:15px; line-height:1.5; }
  .setup { margin-top:36px; padding:18px; background:var(--panel); border-left:4px solid var(--cue);
    font-size:14px; line-height:1.6; color:var(--muted); }
  .setup b { color:var(--text); }
  .setup code { font-family:ui-monospace,Menlo,monospace; color:var(--cue); word-break:break-all; }
</style>
</head>
<body>
<div class="wrap">
  <header><h1 class="mono">SEND TO GLASSES</h1><span id="count" class="count mono">--</span></header>
  <div class="compose">
    <button id="paste" class="paste mono">PASTE LINK AND QUEUE</button>
    <label class="mono" for="url">OR TYPE IT IN</label>
    <div class="field">
      <input type="url" id="url" inputmode="url" autocomplete="off" autocapitalize="off" spellcheck="false" placeholder="youtube.com/watch?v=...">
      <button id="send" class="mono">QUEUE</button>
    </div>
    <div id="status" class="status"></div>
  </div>
  <div class="list">
    <div class="list-head mono"><span>IN THE RUNDOWN</span><button id="clear" class="ghost mono">CLEAR ALL</button></div>
    <div id="items"></div>
  </div>
  <div id="setup" class="setup"></div>
</div>
<script>
(function () {
  'use strict';
  var token=null, items=[];
  var el={};
  ['url','send','status','items','count','clear','setup','paste'].forEach(function(id){ el[id]=document.getElementById(id); });

  function say(message,kind){ el.status.textContent=message||''; el.status.className='status'+(kind?' '+kind:''); }
  function esc(v){ return String(v==null?'':v).replace(/[&<>"']/g,function(c){
    return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }

  function render(){
    el.count.textContent=items.length?String(items.length).padStart(2,'0'):'--';
    if(!items.length){ el.items.innerHTML='<div class="empty">Empty. Anything you queue shows up on the glasses within a few seconds.</div>'; return; }
    el.items.innerHTML=items.map(function(item,i){
      var mark='';
      if(item.position>10){
        var s=Math.floor(item.position), m=Math.floor(s/60), r=s%60;
        var h=Math.floor(m/60);
        mark=' &middot; resume '+(h>0?h+':'+String(m%60).padStart(2,'0'):String(m))+':'+String(r).padStart(2,'0');
      }
      return '<div class="item"><span class="item-num mono">'+String(i+1).padStart(2,'0')+
        '</span><div class="item-body"><div class="item-title">'+esc(item.title)+'</div>'+
        ((item.channel||mark)?'<div class="item-channel">'+esc(item.channel)+mark+'</div>':'')+
        '</div><button class="item-drop" data-id="'+item.id+'" aria-label="Remove">&times;</button></div>';
    }).join('');
    Array.prototype.forEach.call(el.items.querySelectorAll('.item-drop'),function(node){
      node.addEventListener('click',function(){ drop(this.getAttribute('data-id')); });
    });
  }
  function api(path,options){
    return fetch('/api/queue?token='+encodeURIComponent(token)+(path||''),Object.assign({cache:'no-store'},options||{}))
      .then(function(r){ return r.json().then(function(body){ if(!r.ok) throw new Error(body.error||'Something went wrong.'); return body; }); });
  }
  function refresh(){
    return api('').then(function(data){
      items=data.items||[]; render();
      if(data.persistent===false) say('Storage is not connected, so the queue empties when the server restarts.','bad');
    }).catch(function(e){ say(e.message,'bad'); });
  }
  function add(value){
    if(!value||!value.trim()) return Promise.resolve();
    el.send.disabled=true; say('Queueing...');
    return api('',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url:value.trim()})})
      .then(function(data){
        items=data.items||[]; render(); el.url.value='';
        say(data.duplicate?'Already in the rundown.':'Queued. Look at your glasses.','ok');
      })
      .catch(function(e){ say(e.message,'bad'); })
      .then(function(){ el.send.disabled=false; });
  }
  function drop(id){
    api('&id='+encodeURIComponent(id),{method:'DELETE'})
      .then(function(data){ items=data.items||[]; render(); })
      .catch(function(e){ say(e.message,'bad'); });
  }
  el.send.addEventListener('click',function(){ add(el.url.value); });

  // Reading the clipboard needs a real tap, and iOS shows its own Paste
  // confirmation. If it is blocked, fall back to the text field.
  el.paste.addEventListener('click',function(){
    if(!navigator.clipboard || !navigator.clipboard.readText){
      say('This browser will not hand over the clipboard. Long press the box below and choose Paste.','bad');
      el.url.focus(); return;
    }
    say('Reading clipboard...');
    navigator.clipboard.readText().then(function(text){
      var link=(text||'').trim();
      if(!link){ say('Clipboard is empty. Copy a YouTube link first.','bad'); return; }
      el.url.value=link; add(link);
    }).catch(function(){
      say('Clipboard access was declined. Long press the box below and choose Paste.','bad');
      el.url.focus();
    });
  });
  el.url.addEventListener('keydown',function(e){ if(e.key==='Enter'){ e.preventDefault(); add(el.url.value); } });
  el.clear.addEventListener('click',function(){
    if(!items.length) return;
    api('&all=1',{method:'DELETE'}).then(function(){ items=[]; render(); say('Rundown cleared.','ok'); })
      .catch(function(e){ say(e.message,'bad'); });
  });

  var params=new URLSearchParams(location.search);
  token=params.get('token');
  if(token){ try{localStorage.setItem('rundown.token',token);}catch(e){} }
  else { try{ token=localStorage.getItem('rundown.token'); }catch(e){} }

  if(!token){ el.send.disabled=true; el.items.innerHTML=''; say('Add ?token=yourword to the address to get started.','bad'); }

  el.setup.innerHTML = token
    ? '<b>Your glasses address</b><br><code>'+esc(location.origin)+'/?token='+esc(token)+'</code><br><br>'+
      'Add that to the Meta AI app under App Connections, Web Apps, Add a Web App.'
    : '<b>Pick a token</b><br>Any word, 4 letters or more. Use the same one here and on the glasses so they share a queue.';

  var shared=params.get('url')||params.get('text');
  if(token&&shared){ add(shared).then(function(){ history.replaceState({},'',location.pathname+'?token='+encodeURIComponent(token)); }); }
  else if(token){ refresh(); }
})();
</script>
</body>
</html>`;

const PROBE_PAGE = String.raw`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=600, height=600, initial-scale=1.0, user-scalable=no">
<meta name="mrbd-web-app-capable" content="yes">
<title>Probe</title>
<style>
  :root { --ink:#000; --cue:#FFB000; --pass:#3DDC84; --fail:#FF4436; --text:#FFF; --muted:#8E9A97; --rail:#2A312F; }
  * { margin:0; padding:0; box-sizing:border-box; }
  html,body { width:600px; min-height:600px; background:var(--ink); color:var(--text);
    font-family:"Helvetica Neue",Helvetica,Arial,sans-serif; }
  body { position:relative; padding-bottom:30px; }
  .mono { font-family:"SF Mono","Roboto Mono",Menlo,Consolas,monospace; letter-spacing:.04em; }
  header { padding:24px 24px 16px; border-bottom:2px solid var(--rail); }
  h1 { font-size:26px; letter-spacing:.14em; font-weight:700; }
  .sub { font-size:16px; color:var(--muted); margin-top:8px; line-height:1.4; }
  .stage { padding:40px 24px; }
  .big { font-size:22px; line-height:1.5; }
  .code { font-size:56px; letter-spacing:.2em; color:var(--cue); font-weight:700; margin:24px 0 16px; }
  .ok { color:var(--pass); }
  .bad { color:var(--fail); }
  .step { font-size:19px; color:var(--muted); line-height:1.5; margin-top:20px; }
  .step b { color:var(--text); }
  .bar { height:6px; background:var(--rail); margin-top:28px; }
  .bar-fill { height:6px; width:0%; background:var(--cue); transition:width 200ms linear; }
  .tally { font-size:18px; color:var(--muted); margin-top:12px; }
</style>
</head>
<body>
<header>
  <h1 class="mono">PROBE</h1>
  <div class="sub">Measuring what this device can do, then sending it up.</div>
</header>
<div class="stage">
  <div id="status" class="big">Running tests...</div>
  <div class="bar"><div id="bar" class="bar-fill"></div></div>
  <div id="tally" class="tally mono"></div>
  <div id="code" class="code mono"></div>
  <div id="step" class="step"></div>
</div>

<script>
(function(){
  'use strict';
  var params=new URLSearchParams(location.search);
  var token=(params.get('token')||'probe').trim();
  var report={ takenAt:new Date().toISOString(), sections:{} };
  var el={};
  ['status','bar','tally','code','step'].forEach(function(id){ el[id]=document.getElementById(id); });

  function put(section,key,value){
    if(!report.sections[section]) report.sections[section]={};
    report.sections[section][key]=value;
  }
  function progress(pct,note){
    el.bar.style.width=pct+'%';
    if(note) el.tally.textContent=note;
  }

  /* ---------- identity ---------- */
  put('device','userAgent',navigator.userAgent);
  put('device','platform',navigator.platform||'');
  put('device','vendor',navigator.vendor||'');
  put('device','languages',(navigator.languages||[]).join(','));
  put('device','cores',navigator.hardwareConcurrency||null);
  put('device','memoryGB',navigator.deviceMemory||null);
  put('device','touchPoints',navigator.maxTouchPoints||0);
  put('device','screen',screen.width+'x'+screen.height);
  put('device','viewport',window.innerWidth+'x'+window.innerHeight);
  put('device','pixelRatio',window.devicePixelRatio||1);
  put('device','colorDepth',screen.colorDepth||null);
  put('device','timezone',(Intl.DateTimeFormat().resolvedOptions().timeZone)||'');
  put('device','online',navigator.onLine);

  var conn=navigator.connection||navigator.mozConnection||navigator.webkitConnection;
  if(conn){
    put('network','effectiveType',conn.effectiveType||'');
    put('network','downlinkMbps',conn.downlink||null);
    put('network','rttMs',conn.rtt||null);
    put('network','saveData',!!conn.saveData);
  } else {
    put('network','info','Network Information API unavailable');
  }

  /* ---------- platform features ---------- */
  put('features','MediaSource',!!window.MediaSource);
  put('features','ManagedMediaSource',!!window.ManagedMediaSource);
  put('features','EME',!!navigator.requestMediaKeySystemAccess);
  put('features','MediaCapabilities',!!(navigator.mediaCapabilities&&navigator.mediaCapabilities.decodingInfo));
  put('features','WebRTC',!!window.RTCPeerConnection);
  put('features','WebSocket',!!window.WebSocket);
  put('features','ServiceWorker',!!navigator.serviceWorker);
  put('features','WebAssembly',typeof WebAssembly!=='undefined');
  put('features','WebGL',(function(){ try{ return !!document.createElement('canvas').getContext('webgl'); }catch(e){ return false; } })());
  put('features','WebGL2',(function(){ try{ return !!document.createElement('canvas').getContext('webgl2'); }catch(e){ return false; } })());
  put('features','WebCodecs',!!(window.VideoDecoder&&window.VideoEncoder));
  put('features','MediaRecorder',!!window.MediaRecorder);
  put('features','WakeLock',!!(navigator.wakeLock));
  put('features','MediaSession',!!navigator.mediaSession);
  put('features','Fullscreen',!!(document.documentElement.requestFullscreen));
  put('features','PictureInPicture',!!document.pictureInPictureEnabled);
  put('features','DeviceMotion',typeof DeviceMotionEvent!=='undefined');
  put('features','DeviceOrientation',typeof DeviceOrientationEvent!=='undefined');
  put('features','Geolocation',!!navigator.geolocation);
  put('features','Vibration',!!navigator.vibrate);
  put('features','Battery',!!navigator.getBattery);
  put('features','Clipboard',!!(navigator.clipboard&&navigator.clipboard.readText));
  put('features','SharedWorker',typeof SharedWorker!=='undefined');
  put('features','OffscreenCanvas',typeof OffscreenCanvas!=='undefined');
  put('features','localStorage',(function(){ try{ localStorage.setItem('_p','1'); localStorage.removeItem('_p'); return true; }catch(e){ return false; } })());

  progress(15,'device and features');

  /* ---------- codecs ---------- */
  var probeVideo=document.createElement('video');
  var codecs={
    'H.264 baseline':'video/mp4; codecs="avc1.42E01E"',
    'H.264 main':'video/mp4; codecs="avc1.4D401F"',
    'H.264 high':'video/mp4; codecs="avc1.640028"',
    'HEVC main':'video/mp4; codecs="hvc1.1.6.L93.B0"',
    'HEVC hev1':'video/mp4; codecs="hev1.1.6.L93.B0"',
    'VP8':'video/webm; codecs="vp8"',
    'VP9':'video/webm; codecs="vp9"',
    'VP9 mp4':'video/mp4; codecs="vp09.00.10.08"',
    'AV1':'video/mp4; codecs="av01.0.05M.08"',
    'MPEG-2 TS':'video/mp2t; codecs="avc1.42E01E,mp4a.40.2"',
    'AAC-LC':'audio/mp4; codecs="mp4a.40.2"',
    'AAC-HE':'audio/mp4; codecs="mp4a.40.5"',
    'MP3':'audio/mpeg',
    'Opus':'audio/webm; codecs="opus"',
    'FLAC':'audio/flac',
    'HLS manifest':'application/vnd.apple.mpegurl',
    'DASH manifest':'application/dash+xml'
  };
  Object.keys(codecs).forEach(function(name){
    var type=codecs[name];
    var tag=probeVideo.canPlayType(type);
    var mse=(window.MediaSource&&MediaSource.isTypeSupported)?MediaSource.isTypeSupported(type):false;
    put('codecs',name,{videoTag:tag||'no',mse:mse});
  });

  progress(35,'codecs');

  /* ---------- DRM, at several robustness levels ---------- */
  var drmSystems=[
    ['Widevine','com.widevine.alpha'],
    ['Widevine L1 hint','com.widevine.alpha.experiment'],
    ['FairPlay','com.apple.fps'],
    ['FairPlay 1.0','com.apple.fps.1_0'],
    ['FairPlay 2.0','com.apple.fps.2_0'],
    ['PlayReady','com.microsoft.playready'],
    ['PlayReady recommendation','com.microsoft.playready.recommendation'],
    ['ClearKey','org.w3.clearkey'],
    ['WisePlay','com.huawei.wiseplay']
  ];
  var robustness=['','SW_SECURE_CRYPTO','SW_SECURE_DECODE','HW_SECURE_CRYPTO','HW_SECURE_DECODE','HW_SECURE_ALL'];

  function drmConfig(rob){
    return [{
      initDataTypes:['cenc','keyids','webm'],
      videoCapabilities:[{contentType:'video/mp4; codecs="avc1.42E01E"',robustness:rob}],
      audioCapabilities:[{contentType:'audio/mp4; codecs="mp4a.40.2"'}]
    }];
  }

  function testDrm(index,done){
    if(index>=drmSystems.length) return done();
    var name=drmSystems[index][0], id=drmSystems[index][1];
    if(!navigator.requestMediaKeySystemAccess){
      put('drm',name,'no EME');
      return testDrm(index+1,done);
    }
    var levels=[];
    var r=0;
    function nextLevel(){
      if(r>=robustness.length){
        put('drm',name, levels.length?levels.join(', '):'refused');
        return testDrm(index+1,done);
      }
      var rob=robustness[r++];
      navigator.requestMediaKeySystemAccess(id, drmConfig(rob))
        .then(function(){ levels.push(rob||'default'); nextLevel(); })
        .catch(function(){ nextLevel(); });
    }
    nextLevel();
  }

  /* ---------- decoding performance hints ---------- */
  function testMediaCapabilities(done){
    if(!(navigator.mediaCapabilities&&navigator.mediaCapabilities.decodingInfo)){
      put('decoding','info','MediaCapabilities unavailable');
      return done();
    }
    var trials=[
      ['H.264 720p30','video/mp4; codecs="avc1.640028"',1280,720,30,3000000],
      ['H.264 1080p60','video/mp4; codecs="avc1.640028"',1920,1080,60,6000000],
      ['HEVC 1080p30','video/mp4; codecs="hvc1.1.6.L93.B0"',1920,1080,30,4000000],
      ['AV1 1080p30','video/mp4; codecs="av01.0.05M.08"',1920,1080,30,3000000]
    ];
    var i=0;
    function next(){
      if(i>=trials.length) return done();
      var t=trials[i++];
      navigator.mediaCapabilities.decodingInfo({
        type:'media-source',
        video:{contentType:t[1],width:t[2],height:t[3],framerate:t[4],bitrate:t[5]}
      }).then(function(r){
        put('decoding',t[0],{supported:r.supported,smooth:r.smooth,powerEfficient:r.powerEfficient});
        next();
      }).catch(function(){ put('decoding',t[0],'query failed'); next(); });
    }
    next();
  }

  /* ---------- upload ---------- */
  function shortCode(){
    return Math.random().toString(36).slice(2,6).toUpperCase();
  }

  function upload(){
    progress(95,'uploading');
    report.code=shortCode();
    fetch('/api/probe?token='+encodeURIComponent(token),{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify(report)
    }).then(function(r){ return r.json(); }).then(function(){
      progress(100,'done');
      el.status.innerHTML='<span class="ok">Report sent.</span>';
      el.code.textContent=report.code;
      el.step.innerHTML='Open this on your computer:<br><b>/report?token='+token+'</b>'+
        '<br><br>The code above should match what you see there.';
    }).catch(function(){
      el.status.innerHTML='<span class="bad">Upload failed.</span>';
      el.step.textContent='The tests ran but could not reach the server. Check the connection and restart.';
    });
  }

  progress(50,'checking DRM');
  testDrm(0,function(){
    progress(75,'checking decode support');
    testMediaCapabilities(upload);
  });
})();
</script>
</body>
</html>`;

const REPORT_PAGE = String.raw`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Probe report</title>
<style>
  :root { --ink:#0B0D0C; --panel:#141817; --rail:#262E2C; --cue:#FFB000; --pass:#3DDC84;
          --fail:#FF4436; --text:#ECEAE4; --muted:#7F8C89; }
  * { margin:0; padding:0; box-sizing:border-box; }
  body { background:var(--ink); color:var(--text); font-family:ui-sans-serif,-apple-system,"Segoe UI",Roboto,sans-serif;
    padding:32px 20px 80px; }
  .mono { font-family:ui-monospace,"SF Mono",Menlo,Consolas,monospace; letter-spacing:.04em; }
  .wrap { max-width:900px; margin:0 auto; }
  header { padding-bottom:18px; border-bottom:2px solid var(--rail); display:flex;
    justify-content:space-between; align-items:baseline; flex-wrap:wrap; gap:12px; }
  h1 { font-size:22px; letter-spacing:.16em; }
  .when { font-size:14px; color:var(--muted); }
  .code { color:var(--cue); font-size:18px; }
  h2 { font-size:13px; letter-spacing:.18em; color:var(--cue); margin:34px 0 12px;
    padding-bottom:8px; border-bottom:1px solid var(--rail); }
  table { width:100%; border-collapse:collapse; }
  td { padding:9px 0; vertical-align:top; border-bottom:1px solid rgba(38,46,44,.6); font-size:15px; }
  td.k { width:38%; color:var(--muted); padding-right:16px; word-break:break-word; }
  td.v { word-break:break-word; }
  .yes { color:var(--pass); font-weight:600; }
  .no { color:var(--fail); }
  .warn { color:var(--cue); }
  .empty { padding:60px 0; color:var(--muted); font-size:16px; line-height:1.6; }
  .raw { margin-top:44px; }
  .raw summary { cursor:pointer; color:var(--muted); font-size:14px; letter-spacing:.1em; }
  pre { margin-top:14px; padding:16px; background:var(--panel); border-left:4px solid var(--cue);
    font-family:ui-monospace,Menlo,monospace; font-size:12px; line-height:1.5; overflow-x:auto;
    white-space:pre-wrap; word-break:break-word; }
  .verdict { margin-top:28px; padding:18px; background:var(--panel); border-left:5px solid var(--cue);
    font-size:16px; line-height:1.55; }
  .verdict b { color:var(--text); }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1 class="mono">PROBE REPORT</h1>
    <span id="meta" class="when mono"></span>
  </header>
  <div id="out"></div>
  <div id="verdict" class="verdict" style="display:none"></div>
  <details class="raw"><summary class="mono">RAW JSON</summary><pre id="raw"></pre></details>
</div>

<script>
(function(){
  'use strict';
  var params=new URLSearchParams(location.search);
  var token=(params.get('token')||'probe').trim();
  var out=document.getElementById('out');

  function esc(v){
    return String(v==null?'':v).replace(/[&<>"']/g,function(c){
      return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; });
  }

  function renderValue(v){
    if(v===true) return '<span class="yes">YES</span>';
    if(v===false) return '<span class="no">NO</span>';
    if(v===null||v==='') return '<span class="warn">&mdash;</span>';
    if(typeof v==='object'){
      return Object.keys(v).map(function(k){
        var inner=v[k];
        var mark = inner===true?'<span class="yes">yes</span>'
                 : inner===false?'<span class="no">no</span>'
                 : inner==='no'?'<span class="no">no</span>'
                 : inner==='probably'?'<span class="yes">probably</span>'
                 : inner==='maybe'?'<span class="warn">maybe</span>'
                 : esc(inner);
        return esc(k)+': '+mark;
      }).join(' &nbsp;·&nbsp; ');
    }
    if(v==='refused'||v==='no EME') return '<span class="no">'+esc(v)+'</span>';
    return esc(v);
  }

  function section(title,data){
    var h=document.createElement('h2');
    h.className='mono'; h.textContent=title;
    out.appendChild(h);
    var t=document.createElement('table');
    t.innerHTML=Object.keys(data).map(function(k){
      return '<tr><td class="k">'+esc(k)+'</td><td class="v">'+renderValue(data[k])+'</td></tr>';
    }).join('');
    out.appendChild(t);
  }

  function verdictFor(r){
    var drm=r.sections.drm||{};
    var f=r.sections.features||{};
    var parts=[];
    var wv=drm['Widevine'];
    if(wv && wv!=='refused' && wv!=='no EME'){
      parts.push('<b>Widevine is available</b> at: '+esc(wv)+'. Protected streams could technically decrypt here.');
    } else {
      parts.push('<b>No Widevine.</b> No commercial subscription service can play on this device, '+
                 'regardless of which one you subscribe to. This is the decisive line.');
    }
    if(f.MediaSource===true){
      parts.push('<b>MSE works</b>, so any unencrypted HLS or DASH source will play.');
    }
    if(f.WebRTC===true){
      parts.push('<b>WebRTC is present</b> — a sub-second path from a machine on your own network, '+
                 'if you ever want live low-latency video from something you control.');
    }
    if(f.WebCodecs===true){
      parts.push('<b>WebCodecs is present</b>, which allows frame-level decoding under your own control.');
    }
    return parts.join('<br><br>');
  }

  fetch('/api/probe?token='+encodeURIComponent(token),{cache:'no-store'})
    .then(function(r){ return r.json(); })
    .then(function(data){
      var r=data.report;
      if(!r){
        out.innerHTML='<div class="empty">Nothing uploaded yet for this token.<br><br>'+
          'Open <b>/probe?token='+esc(token)+'</b> on the glasses first, wait for the code to appear, then reload this page.</div>';
        return;
      }
      document.getElementById('meta').innerHTML=
        'CODE <span class="code">'+esc(r.code||'')+'</span> &nbsp;·&nbsp; '+esc(new Date(r.takenAt).toLocaleString());
      var order=['device','network','features','codecs','drm','decoding'];
      order.forEach(function(name){
        if(r.sections[name]) section(name.toUpperCase(),r.sections[name]);
      });
      Object.keys(r.sections).forEach(function(name){
        if(order.indexOf(name)===-1) section(name.toUpperCase(),r.sections[name]);
      });
      var v=document.getElementById('verdict');
      v.style.display='block';
      v.innerHTML=verdictFor(r);
      document.getElementById('raw').textContent=JSON.stringify(r,null,2);
    })
    .catch(function(){
      out.innerHTML='<div class="empty">Could not load the report.</div>';
    });
})();
</script>
</body>
</html>`;

const LIVE_PAGE = String.raw`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=600, height=600, initial-scale=1.0, user-scalable=no">
<meta name="mrbd-web-app-capable" content="yes">
<title>Live</title>
<style>
  :root { --ink:#000; --cue:#FFB000; --live:#FF4436; --text:#FFF; --muted:#8E9A97; --rail:#2A312F; }
  * { margin:0; padding:0; box-sizing:border-box; }
  html,body { width:600px; height:600px; overflow:hidden; background:var(--ink); color:var(--text);
    font-family:"Helvetica Neue",Helvetica,Arial,sans-serif; }
  body { position:relative; }
  .mono { font-family:"SF Mono","Roboto Mono",Menlo,Consolas,monospace; letter-spacing:.04em; font-variant-numeric:tabular-nums; }
  #sink { position:absolute; top:0; left:0; width:1px; height:1px; opacity:0; border:none; background:none; }
  .stage { position:absolute; top:0; left:0; width:600px; height:600px;
    display:flex; align-items:center; justify-content:center; overflow:hidden; }
  video { display:block; width:600px; height:auto; max-height:600px;
    object-fit:contain; background:var(--ink); pointer-events:none; }
  video.fill { width:1067px; height:600px; max-height:600px; object-fit:cover; }
  .chrome { position:absolute; inset:0; pointer-events:none; opacity:1; transition:opacity 400ms ease; }
  .chrome.hidden { opacity:0; }
  .top { position:absolute; top:0; left:0; right:0; padding:18px 26px 26px;
    background:linear-gradient(to bottom,rgba(0,0,0,.85),rgba(0,0,0,0)); }
  .bottom { position:absolute; bottom:0; left:0; right:0; padding:26px 26px 18px;
    background:linear-gradient(to top,rgba(0,0,0,.85),rgba(0,0,0,0)); }
  .tag { font-size:15px; letter-spacing:.18em; color:var(--live); }
  .name { font-size:22px; font-weight:600; margin-top:6px; text-shadow:0 2px 8px rgba(0,0,0,.9);
    display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; }
  .readout { display:flex; justify-content:space-between; font-size:18px; text-shadow:0 2px 8px rgba(0,0,0,.9); }
  .state { color:var(--live); letter-spacing:.12em; }
  .state.paused { color:var(--muted); }
  .quality { color:var(--cue); }
  .panel { position:absolute; inset:0; display:flex; flex-direction:column; justify-content:center;
    padding:0 40px; gap:14px; background:var(--ink); }
  .panel.gone { display:none; }
  .panel h1 { font-size:26px; letter-spacing:.12em; }
  .panel p { font-size:19px; line-height:1.45; color:var(--muted); }
  .panel code { color:var(--cue); font-family:"SF Mono",Menlo,monospace; font-size:17px; word-break:break-all; }
  .pinned { position:absolute; left:0; right:0; bottom:0; padding:16px 26px; background:rgba(0,0,0,.9);
    border-top:3px solid var(--cue); font-size:18px; line-height:1.4; color:var(--cue); }
  .pinned.gone { display:none; }
  .pinned .why { display:block; font-size:15px; color:var(--muted); margin-top:4px; }
</style>
</head>
<body>
<button id="sink" aria-label="Player"></button>
<div class="stage"><video id="v" playsinline muted autoplay></video></div>
<div id="chrome" class="chrome">
  <div class="top"><div id="tag" class="tag mono">LIVE</div><div id="name" class="name">&nbsp;</div></div>
  <div class="bottom"><div class="readout mono">
    <span id="state" class="state">CONNECTING</span><span id="quality" class="quality">&nbsp;</span>
  </div></div>
</div>
<div id="pinned" class="pinned gone"></div>
<div id="panel" class="panel">
  <h1 class="mono">LIVE</h1>
  <p>Add a stream to the address, like<br><code>/live?src=http://192.168.1.20:5004/auto/v5.1</code></p>
  <p>Any unencrypted HLS, DASH or progressive source works. Protected services do not.</p>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/hls.js/1.5.17/hls.min.js"></script>
<script>
(function(){
  'use strict';
  var params=new URLSearchParams(location.search);
  var src=params.get('src');
  var label=params.get('name')||'';
  var ZOOM=params.get('zoom')==='fill'?'fill':'fit';

  var v=document.getElementById('v');
  var el={};
  ['sink','chrome','tag','name','state','quality','panel','pinned'].forEach(function(id){ el[id]=document.getElementById(id); });

  if(!src) return;
  el.panel.classList.add('gone');
  if(ZOOM==='fill') v.classList.add('fill');
  el.name.textContent=label||src.replace(/^https?:\/\//,'').slice(0,60);

  var chromeTimer=null;
  function showChrome(){
    el.chrome.classList.remove('hidden');
    if(chromeTimer) clearTimeout(chromeTimer);
    // Only fade the overlay once video is genuinely running. Otherwise an
    // error message would vanish before it could be read.
    chromeTimer=setTimeout(function(){
      if(!v.paused && v.readyState>2) el.chrome.classList.add('hidden');
    },2500);
  }
  function pin(message,why){
    el.pinned.classList.remove('gone');
    el.pinned.innerHTML=message+(why?'<span class="why">'+why+'</span>':'');
  }
  function unpin(){ el.pinned.classList.add('gone'); }
  function setState(text,live){ el.state.textContent=text; el.state.className='state'+(live?'':' paused'); }

  // Focus stays here so pinches reach this page, not the video element.
  function holdFocus(){
    if(document.activeElement!==el.sink){
      try{ el.sink.focus({preventScroll:true}); }catch(e){ try{ el.sink.focus(); }catch(e2){} }
    }
  }
  setInterval(holdFocus,1500);
  window.addEventListener('blur',function(){ setTimeout(holdFocus,50); });
  document.addEventListener('focusout',function(){ setTimeout(holdFocus,50); });
  holdFocus();

  var hls=null;
  var isHls=/\.m3u8(\?|$)/i.test(src);

  function attach(){
    if(isHls && !window.Hls){
      pin('Player script did not load.',
          'hls.js is blocked or unreachable from this device, so .m3u8 cannot be demuxed here.');
      setState('NO PLAYER',false);
      return;
    }
    if(isHls && window.Hls && Hls.isSupported()){
      // hls.js drives MSE directly, which the probe confirmed works here.
      hls=new Hls({lowLatencyMode:true, backBufferLength:30, maxBufferLength:20});
      hls.loadSource(src);
      hls.attachMedia(v);
      hls.on(Hls.Events.MANIFEST_PARSED,function(){ v.play().catch(function(){}); });
      hls.on(Hls.Events.LEVEL_SWITCHED,function(e,d){
        var lvl=hls.levels[d.level];
        if(lvl) el.quality.textContent=lvl.height+'p';
      });
      hls.on(Hls.Events.ERROR,function(e,data){
        if(!data.fatal) return;
        if(data.type===Hls.ErrorTypes.NETWORK_ERROR){ setState('RECONNECTING',false); hls.startLoad(); }
        else if(data.type===Hls.ErrorTypes.MEDIA_ERROR){ setState('RECOVERING',false); hls.recoverMediaError(); }
        else { setState('FAILED',false); el.name.textContent='Cannot play this stream.'; }
      });
    } else {
      // Native path: progressive files, or HLS where the runtime handles it.
      v.src=src;
      v.play().catch(function(){ setState('TAP TO START',false); });
    }
  }

  v.addEventListener('playing',function(){
    setState('LIVE',true);
    if(!el.quality.textContent.trim() && v.videoHeight) el.quality.textContent=v.videoHeight+'p';
    // Autoplay is only permitted while muted, so sound waits for a gesture.
    if(v.muted) pin('Playing, muted.','Pinch once for sound.'); else unpin();
    showChrome();
  });
  v.addEventListener('waiting',function(){ setState('BUFFERING',false); });
  v.addEventListener('pause',function(){ setState('PAUSED',false); });
  v.addEventListener('error',function(){
    var code=v.error?v.error.code:0;
    var why={1:'Loading was aborted.',2:'Network failed.',
             3:'Decode failed — codec present but the stream would not decode.',
             4:'Source refused. Wrong format, blocked by CORS, or unreachable.'}[code]||'Unknown failure.';
    setState('FAILED',false);
    pin('Cannot play this stream. Error '+code+'.',why);
    showChrome();
  });

  function jumpToLive(){
    try{
      if(hls && hls.liveSyncPosition){ v.currentTime=hls.liveSyncPosition; }
      else if(v.seekable && v.seekable.length){ v.currentTime=v.seekable.end(v.seekable.length-1); }
      el.tag.textContent='JUMPED TO LIVE';
      setTimeout(function(){ el.tag.textContent='LIVE'; },1500);
    }catch(e){}
  }
  function nudgeVolume(d){
    v.volume=Math.max(0,Math.min(1,v.volume+d));
    el.tag.textContent='VOLUME '+Math.round(v.volume*100);
    setTimeout(function(){ el.tag.textContent='LIVE'; },1200);
  }

  window.addEventListener('keydown',function(e){
    var handled=true;
    showChrome();
    switch(e.key){
      case 'Enter':
        if(v.muted){ v.muted=false; v.volume=0.8; unpin(); v.play().catch(function(){}); }
        else if(v.paused) v.play().catch(function(){});
        else v.pause();
        break;
      case 'ArrowLeft': try{ v.currentTime=Math.max(0,v.currentTime-15); }catch(err){} break;
      case 'ArrowRight': jumpToLive(); break;
      case 'ArrowUp': nudgeVolume(0.1); break;
      case 'ArrowDown': nudgeVolume(-0.1); break;
      default: handled=false;
    }
    holdFocus();
    if(handled){ e.preventDefault(); e.stopPropagation(); }
  },true);

  setState('CONNECTING',false);
  pin('Connecting to the stream...','');
  attach();
  showChrome();
  // If nothing has started after ten seconds, say so rather than sit black.
  setTimeout(function(){
    if(v.readyState<2 && el.state.textContent!=='FAILED' && el.state.textContent!=='NO PLAYER'){
      pin('No video after 10 seconds.',
          'The stream may be unreachable from this device, or blocked by CORS.');
      showChrome();
    }
  },10000);
})();
</script>
</body>
</html>`;

const MANIFEST = JSON.stringify({
  name: 'Rundown',
  short_name: 'Rundown',
  start_url: '/phone',
  display: 'standalone',
  background_color: '#0B0D0C',
  theme_color: '#0B0D0C',
  icons: [{src: '/icon.png', sizes: '192x192', type: 'image/png'}],
  share_target: {action: '/phone', method: 'GET', params: {title: 'title', text: 'text', url: 'url'}},
});

// A 192x192 amber-on-black rundown mark, inlined so there are no other files.
const ICON_PNG = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAMAAAADACAYAAABS3GwHAAAAt0lEQVR4nO3ZsQ2AMAxE0TMSGzACJ' +
  'YUXYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARG' +
  'YARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYAR' +
  'GYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYARGYA' +
  'RGYARGYARGYARGYARGYARGYAT8ADwOGAB1oPRlQAAAABJRU5ErkJggg==',
  'base64',
);

/* ══════════════════════════════════════════════════════════════════════════
   The queue API
   ══════════════════════════════════════════════════════════════════════════ */

function sendJson(res, status, body) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    'Content-Type': 'application/json',
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET,POST,DELETE,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  });
  res.end(payload);
}

function readBody(req) {
  return new Promise((resolve) => {
    let raw = '';
    req.on('data', (chunk) => {
      raw += chunk;
      if (raw.length > 16384) req.destroy();
    });
    req.on('end', () => {
      try {
        resolve(raw ? JSON.parse(raw) : {});
      } catch {
        resolve({});
      }
    });
    req.on('error', () => resolve({}));
  });
}

// Adds one link to the queue. Shared by POST (JSON body) and GET (query
// string), so a Shortcut can just append the link to the URL.
async function addToQueue(token, source, rawQuery) {
  let videoId = parseVideoId(source);

  // A link appended to a URL gets its own query string chopped up by the
  // parser, so as a fallback we scan the whole raw query for a video id.
  if (!videoId && rawQuery) {
    const match = decodeURIComponent(rawQuery).match(
      /(?:youtu\.be\/|watch\?v=|\/shorts\/|\/embed\/|\/live\/|[?&]v=)([\w-]{11})/,
    );
    if (match) videoId = match[1];
  }

  if (!videoId) return {status: 422, body: {error: "That link doesn't contain a YouTube video."}};

  const items = await readQueue(token);
  if (items.some((item) => item.videoId === videoId)) {
    return {status: 200, body: {items, duplicate: true}};
  }

  // The timestamp may have been split off into its own query param.
  let start = parseStartSeconds(source);
  if (!start && rawQuery) {
    const stray = decodeURIComponent(rawQuery).match(/[?&](?:t|start)=(\d+)/);
    if (stray) start = Number(stray[1]);
  }

  const meta = await fetchMeta(videoId);
  const entry = {
    id: `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`,
    videoId,
    start,
    title: meta.title || 'Untitled video',
    channel: meta.channel || '',
    position: 0,
    addedAt: Date.now(),
  };
  const next = [...items, entry].slice(-MAX_ITEMS);
  await writeQueue(token, next);
  return {status: 201, body: {items: next, added: entry}};
}

// Stores and returns a device capability report. Reuses the queue storage,
// keyed separately, so there is nothing new to configure.
async function handleProbe(req, res, url) {
  const token = (url.searchParams.get('token') || 'probe').trim();
  if (!/^[\w-]{1,64}$/.test(token)) {
    return sendJson(res, 400, {error: 'Bad token.'});
  }
  const key = `probe-${token}`;
  try {
    if (req.method === 'POST') {
      const body = await readBody(req);
      if (!body || typeof body !== 'object') {
        return sendJson(res, 422, {error: 'Expected a report.'});
      }
      await writeQueue(key, [body]);
      return sendJson(res, 201, {ok: true, code: body.code || null});
    }
    if (req.method === 'GET') {
      const rows = await readQueue(key);
      return sendJson(res, 200, {report: rows[0] || null});
    }
    return sendJson(res, 405, {error: 'Method not allowed.'});
  } catch (error) {
    return sendJson(res, 500, {error: 'Storage is unreachable.'});
  }
}

async function handleQueue(req, res, url) {
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET,POST,DELETE,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    });
    return res.end();
  }

  const token = (url.searchParams.get('token') || '').trim();
  if (!/^[\w-]{4,64}$/.test(token)) {
    return sendJson(res, 400, {error: 'Add a token to the address, 4 to 64 letters or numbers.'});
  }

  try {
    if (req.method === 'GET') {
      // A link on the query string means "add this", so an iOS Shortcut can be
      // a single action with no request body to configure.
      const shared =
        url.searchParams.get('add') ||
        url.searchParams.get('url') ||
        url.searchParams.get('text');

      if (shared) {
        const result = await addToQueue(token, shared, url.search);
        const added = result.body.added;
        const line = added
          ? `Queued: ${added.title}`
          : result.body.duplicate
            ? 'Already in the rundown.'
            : result.body.error || 'Could not queue that.';
        // Plain text so the Shortcut banner shows something readable.
        res.writeHead(result.status, {
          'Content-Type': 'text/plain; charset=utf-8',
          'Cache-Control': 'no-store',
          'Access-Control-Allow-Origin': '*',
        });
        return res.end(line);
      }

      return sendJson(res, 200, {items: await readQueue(token), persistent: Boolean(redis)});
    }

    if (req.method === 'POST') {
      const body = await readBody(req);

      // A position update, sent while a video plays, rather than a new link.
      if (body.id && typeof body.position === 'number') {
        const items = await readQueue(token);
        let touched = false;
        const next = items.map((item) => {
          if (item.id !== body.id) return item;
          touched = true;
          return {...item, position: Math.max(0, Math.floor(body.position))};
        });
        if (touched) await writeQueue(token, next);
        return sendJson(res, 200, {ok: touched});
      }

      const result = await addToQueue(token, body.url || body.text || '', url.search);
      return sendJson(res, result.status, result.body);
    }

    if (req.method === 'DELETE') {
      if (url.searchParams.get('all')) {
        await writeQueue(token, []);
        return sendJson(res, 200, {items: []});
      }
      const id = url.searchParams.get('id') || '';
      const next = (await readQueue(token)).filter((item) => item.id !== id);
      await writeQueue(token, next);
      return sendJson(res, 200, {items: next});
    }

    return sendJson(res, 405, {error: 'Method not allowed.'});
  } catch (error) {
    console.error('Queue error:', error.message);
    return sendJson(res, 500, {error: 'Storage is unreachable. Try again in a moment.'});
  }
}

/* ══════════════════════════════════════════════════════════════════════════
   Routes
   ══════════════════════════════════════════════════════════════════════════ */

function sendPage(res, body, type, cache) {
  res.writeHead(200, {
    'Content-Type': type,
    'Content-Length': Buffer.byteLength(body),
    'Cache-Control': cache || 'no-cache',
  });
  res.end(body);
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const route = url.pathname.replace(/\/+$/, '') || '/';

  if (route === '/api/queue') return handleQueue(req, res, url);
  if (route === '/api/probe') return handleProbe(req, res, url);

  if (route === '/healthz') {
    let reachable = true;
    if (redis) {
      try {
        await redis.command(['PING']);
      } catch {
        reachable = false;
      }
    }
    return sendJson(res, 200, {ok: true, storage: storageName, reachable});
  }

  if (route === '/') return sendPage(res, GLASSES_PAGE, 'text/html; charset=utf-8');
  if (route === '/phone' || route === '/phone.html') {
    return sendPage(res, PHONE_PAGE, 'text/html; charset=utf-8');
  }
  if (route === '/probe') return sendPage(res, PROBE_PAGE, 'text/html; charset=utf-8');
  if (route === '/live') return sendPage(res, LIVE_PAGE, 'text/html; charset=utf-8');
  if (route === '/report') return sendPage(res, REPORT_PAGE, 'text/html; charset=utf-8');
  if (route === '/manifest.webmanifest') {
    return sendPage(res, MANIFEST, 'application/manifest+json');
  }
  if (route === '/icon.png') {
    res.writeHead(200, {'Content-Type': 'image/png', 'Cache-Control': 'public, max-age=86400'});
    return res.end(ICON_PNG);
  }

  res.writeHead(404, {'Content-Type': 'text/plain'});
  res.end('Not found. Try / for the glasses, /phone for your phone.');
});

server.listen(PORT, () => {
  console.log(`Rundown listening on ${PORT} — storage: ${storageName}`);
});
