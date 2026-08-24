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
  <footer class="mono">SWIPE TO MOVE &nbsp;&middot;&nbsp; PINCH TO PLAY</footer>
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
  ['listScreen','playerScreen','rows','count','nowTitle','nowLabel','veil',
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
      return '<button class="row focusable" data-index="'+n+'"><span class="row-num mono">'+String(n+1).padStart(2,'0')+
        '</span><span class="row-body"><span class="row-title">'+esc(item.title)+'</span>'+
        (item.channel?'<span class="row-channel">'+esc(item.channel)+'</span>':'')+'</span></button>';
    }).join('');
    Array.prototype.forEach.call(el.rows.querySelectorAll('.row'),function(node){
      node.addEventListener('click',function(){ openPlayer(Number(this.getAttribute('data-index'))); });
    });
    var target=el.rows.querySelector('[data-index="'+focusIndex+'"]');
    if(target){ target.classList.add('focused'); target.focus(); }
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
    fetch('/api/queue?token='+encodeURIComponent(token)+'&id='+encodeURIComponent(id),{method:'DELETE'}).catch(function(){});
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
      try{
        var now=player.getCurrentTime()||0, total=player.getDuration()||0;
        el.time.textContent=clockFace(now)+' / '+clockFace(total);
        el.trackFill.style.width=total?(now/total)*100+'%':'0%';
      }catch(e){}
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
    history.pushState({screen:'player',id:item.id},'');
    if(playerReady&&player) player.loadVideoById({videoId:item.videoId,startSeconds:item.start||0});
    sizeStage(); startTicker(); showChrome(); startFocusGuard(); paintDebug();
  }
  function closePlayer(){
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
        case 'ArrowUp': case 'ArrowLeft': moveFocus(-1); break;
        case 'ArrowDown': case 'ArrowRight': moveFocus(1); break;
        case 'Enter': if(items.length) openPlayer(focusIndex); break;
        default: handled=false;
      }
    } else {
      showChrome();
      switch(e.key){
        case 'Enter': togglePlay(); break;
        case 'ArrowLeft': seek(-SEEK_STEP); break;
        case 'ArrowRight': seek(SEEK_STEP); break;
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
  document.addEventListener('visibilitychange',function(){ if(!document.hidden) holdFocus(); });

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
      return '<div class="item"><span class="item-num mono">'+String(i+1).padStart(2,'0')+
        '</span><div class="item-body"><div class="item-title">'+esc(item.title)+'</div>'+
        (item.channel?'<div class="item-channel">'+esc(item.channel)+'</div>':'')+
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
    addedAt: Date.now(),
  };
  const next = [...items, entry].slice(-MAX_ITEMS);
  await writeQueue(token, next);
  return {status: 201, body: {items: next, added: entry}};
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
