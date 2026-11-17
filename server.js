'use strict';
// Moto Courier Simulator — co-op imza (signaling) sunucusu.
//
// Oyun istemcisi (scripts/net/net_relay.gd, net_rooms.gd) WebSocket ile bağlanır:
//   ws(s)://ADRES/?key=ANAHTAR&room=BIKE-KOD&role=host|client
//   ws(s)://ADRES/?key=ANAHTAR&role=lobby
// Sunucu yalnız WebRTC el sıkışmasını (offer/answer/ICE) oda içinde taşır;
// oyun trafiği oyuncular arasında doğrudan (ya da TURN ile) akar.
//
// Sunucu → istemci: welcome, peer-joined, peer-left, signal, room-full,
//   host-taken, room-closed, error{reason}, pong, rooms (lobi)
// İstemci → sunucu: ping{t}, signal{to, data}
//
// Host devri: host düşünce oda HOST_GRACE_MS boyunca açık kalır; kalanlara
// peer-left gider, oyun içinden seçilen devralan aynı oda koduyla role=host
// bağlanır ve odayı devralır. Süre dolarsa kalanlara room-closed gider.

const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

function envInt(name, def) {
  const v = parseInt(process.env[name] || '', 10);
  return Number.isFinite(v) ? v : def;
}

function parseIce() {
  const raw = process.env.ICE_SERVERS;
  if (!raw) return [{ urls: ['stun:stun.l.google.com:19302'] }];
  try {
    const v = JSON.parse(raw);
    return Array.isArray(v) ? v : [{ urls: ['stun:stun.l.google.com:19302'] }];
  } catch (_) {
    console.error('[relay] ICE_SERVERS geçersiz JSON, varsayılan STUN kullanılıyor');
    return [{ urls: ['stun:stun.l.google.com:19302'] }];
  }
}

const CFG = {
  port: envInt('PORT', 8080),
  host: process.env.HOST || '0.0.0.0',
  keys: (process.env.RELAY_APP_KEYS || process.env.RELAY_APP_KEY || '')
    .split(',').map((s) => s.trim()).filter(Boolean),
  maxPeers: envInt('MAX_PEERS', 4),
  maxRooms: envInt('MAX_ROOMS', 500),
  maxConnections: envInt('MAX_CONNECTIONS', 3000),
  hostGraceMs: envInt('HOST_GRACE_MS', 30000),
  waitHostMs: envInt('WAIT_HOST_MS', 60000),
  rateWindowMs: envInt('RATE_WINDOW_MS', 60000),
  rateMax: envInt('RATE_MAX', 60),
  heartbeatMs: envInt('HEARTBEAT_MS', 15000),
  maxMessageBytes: envInt('MAX_MESSAGE_BYTES', 64 * 1024),
  trustProxy: process.env.TRUST_PROXY === '1',
  forceRelay: process.env.FORCE_RELAY === '1',
  iceServers: parseIce(),
  quiet: process.env.QUIET === '1',
};

// Oyunun MpConfig.room_ok ile aynı kural: BIKE- önekli tel adı.
const ROOM_RE = /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,31}$/;

function log(...args) {
  if (!CFG.quiet) console.log(new Date().toISOString(), '[relay]', ...args);
}

/** @type {Map<string, {id: string, hostId: string|null, peers: Map<string, any>, graceTimer: any, waitTimer: any}>} */
const rooms = new Map();
const lobbies = new Set();
const rate = new Map();
let connections = 0;

function send(ws, msg) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg));
}

function fail(ws, reason, code = 4000) {
  send(ws, { type: 'error', reason });
  ws.close(code, reason);
}

function clientIp(req) {
  if (CFG.trustProxy) {
    // En sağdaki değeri güvenilir proxy ekler; soldakiler istemcinin yazabildiği değerler.
    const parts = String(req.headers['x-forwarded-for'] || '').split(',')
      .map((s) => s.trim()).filter(Boolean);
    const fwd = parts[parts.length - 1];
    if (fwd) return fwd.replace(/^::ffff:/, '');
  }
  return String(req.socket.remoteAddress || '').replace(/^::ffff:/, '');
}

function rateLimited(ip) {
  const now = Date.now();
  let rec = rate.get(ip);
  if (!rec || now - rec.start > CFG.rateWindowMs) {
    rec = { start: now, count: 0 };
    rate.set(ip, rec);
  }
  rec.count += 1;
  return rec.count > CFG.rateMax;
}

function pruneRate(now = Date.now()) {
  for (const [ip, rec] of rate) {
    if (now - rec.start > CFG.rateWindowMs) rate.delete(ip);
  }
}

function sameNat(a, b) {
  return !!(a && b && a._ip && a._ip === b._ip);
}

function roomList() {
  const out = [];
  for (const room of rooms.values()) {
    if (!room.hostId) continue;
    out.push({ id: room.id, playerCount: room.peers.size });
  }
  return out;
}

let lobbyTimer = null;
function lobbyChanged() {
  if (lobbyTimer) return;
  lobbyTimer = setTimeout(() => {
    lobbyTimer = null;
    const msg = { type: 'rooms', rooms: roomList() };
    for (const ws of lobbies) send(ws, msg);
  }, 200);
}

function dropRoom(room, notice) {
  if (room.graceTimer) clearTimeout(room.graceTimer);
  if (room.waitTimer) clearTimeout(room.waitTimer);
  for (const ws of room.peers.values()) {
    if (notice) send(ws, { type: notice });
    ws.close(4004, notice || 'room-closed');
  }
  if (rooms.get(room.id) === room) rooms.delete(room.id);
  lobbyChanged();
  log('oda kapandı', room.id, notice || '');
}

function armWaitTimer(room) {
  if (room.waitTimer || room.hostId) return;
  room.waitTimer = setTimeout(() => {
    room.waitTimer = null;
    if (!room.hostId) {
      for (const ws of room.peers.values()) fail(ws, 'no-host', 4003);
    }
  }, CFG.waitHostMs);
}

function joinRoom(ws, roomId, role) {
  let room = rooms.get(roomId);
  if (!room) {
    if (rooms.size >= CFG.maxRooms) return fail(ws, 'server-full', 4029);
    room = { id: roomId, hostId: null, peers: new Map(), graceTimer: null, waitTimer: null };
    rooms.set(roomId, room);
  }
  if (role === 'host' && room.hostId) {
    send(ws, { type: 'host-taken' });
    return ws.close(4009, 'host-taken');
  }
  if (room.peers.size >= CFG.maxPeers) {
    send(ws, { type: 'room-full' });
    return ws.close(4008, 'room-full');
  }

  const peerId = crypto.randomUUID().replace(/-/g, '').slice(0, 16);
  ws._peerId = peerId;
  ws._room = room;
  ws._role = role;
  const others = [...room.peers.entries()];
  room.peers.set(peerId, ws);

  if (role === 'host') {
    room.hostId = peerId;
    if (room.graceTimer) { clearTimeout(room.graceTimer); room.graceTimer = null; }
    if (room.waitTimer) { clearTimeout(room.waitTimer); room.waitTimer = null; }
    const anySame = others.some(([, o]) => sameNat(ws, o));
    send(ws, {
      type: 'welcome', peerId, hostId: peerId, peers: others.map(([id]) => id),
      iceServers: CFG.iceServers, sameNat: anySame, forceRelay: CFG.forceRelay,
    });
    for (const [, o] of others) {
      send(o, { type: 'peer-joined', peerId, hostId: peerId,
        sameNat: sameNat(ws, o), forceRelay: CFG.forceRelay });
    }
    log('host', roomId, peerId, 'bekleyen', others.length);
  } else {
    const hostWs = room.hostId ? room.peers.get(room.hostId) : null;
    send(ws, {
      type: 'welcome', peerId, hostId: room.hostId || '',
      iceServers: CFG.iceServers, sameNat: sameNat(ws, hostWs), forceRelay: CFG.forceRelay,
    });
    for (const [, o] of others) {
      send(o, { type: 'peer-joined', peerId, hostId: room.hostId || '',
        sameNat: sameNat(ws, o), forceRelay: CFG.forceRelay });
    }
    if (!room.hostId) armWaitTimer(room);
    log('istemci', roomId, peerId, room.hostId ? '' : '(host bekleniyor)');
  }
  lobbyChanged();
}

function leaveRoom(ws) {
  const room = ws._room;
  if (!room || !room.peers.has(ws._peerId)) return;
  room.peers.delete(ws._peerId);
  for (const o of room.peers.values()) send(o, { type: 'peer-left', peerId: ws._peerId });
  if (room.hostId === ws._peerId) {
    room.hostId = null;
    if (room.peers.size > 0) {
      // Host devri penceresi: oyun içinden seçilen devralan aynı kodla gelir.
      room.graceTimer = setTimeout(() => {
        room.graceTimer = null;
        if (!room.hostId && rooms.get(room.id) === room) dropRoom(room, 'room-closed');
      }, CFG.hostGraceMs);
    }
  }
  if (room.peers.size === 0) {
    if (room.graceTimer) clearTimeout(room.graceTimer);
    if (room.waitTimer) clearTimeout(room.waitTimer);
    // dropRoom sonrası gecikmeli close olayları aynı kodla açılmış yeni odayı silmesin.
    if (rooms.get(room.id) === room) rooms.delete(room.id);
  }
  lobbyChanged();
  log('ayrıldı', room.id, ws._peerId);
}

function onMessage(ws, data) {
  if (data.length > CFG.maxMessageBytes) return;
  let msg;
  try { msg = JSON.parse(data.toString('utf8')); } catch (_) { return; }
  if (!msg || typeof msg !== 'object') return;
  switch (msg.type) {
    case 'ping':
      send(ws, { type: 'pong', t: typeof msg.t === 'number' ? msg.t : 0 });
      return;
    case 'signal': {
      const room = ws._room;
      if (!room || typeof msg.to !== 'string') return;
      const target = room.peers.get(msg.to);
      if (!target || target === ws) return;
      send(target, { type: 'signal', from: ws._peerId, data: msg.data });
      return;
    }
    default:
  }
}

function handleConnection(ws, req) {
  const ip = clientIp(req);
  ws._ip = ip;
  ws._alive = true;
  ws.on('pong', () => { ws._alive = true; });

  if (connections >= CFG.maxConnections) return fail(ws, 'server-full', 4029);
  if (rateLimited(ip)) return fail(ws, 'rate-limited', 4029);

  let url;
  try { url = new URL(req.url, 'http://localhost'); } catch (_) { return fail(ws, 'invalid-room'); }
  const key = url.searchParams.get('key') || '';
  if (CFG.keys.length > 0 && !CFG.keys.includes(key)) return fail(ws, 'unauthorized', 4001);

  connections += 1;
  ws.on('close', () => {
    connections -= 1;
    lobbies.delete(ws);
    leaveRoom(ws);
  });
  ws.on('error', () => {});
  ws.on('message', (data) => onMessage(ws, data));

  const role = url.searchParams.get('role') || '';
  if (role === 'lobby') {
    lobbies.add(ws);
    send(ws, { type: 'rooms', rooms: roomList() });
    return;
  }
  if (role !== 'host' && role !== 'client') return fail(ws, 'invalid-role');
  const roomId = url.searchParams.get('room') || '';
  if (!ROOM_RE.test(roomId)) return fail(ws, 'invalid-room');
  joinRoom(ws, roomId, role);
}

function createServer() {
  const server = http.createServer((req, res) => {
    if (req.url === '/health') {
      let peers = 0;
      for (const r of rooms.values()) peers += r.peers.size;
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ ok: true, rooms: rooms.size, peers, lobbies: lobbies.size }));
      return;
    }
    res.writeHead(426, { 'content-type': 'text/plain' });
    res.end('WebSocket gerekli');
  });
  const wss = new WebSocketServer({ server, maxPayload: CFG.maxMessageBytes });
  wss.on('connection', handleConnection);
  const beat = setInterval(() => {
    pruneRate();
    for (const ws of wss.clients) {
      if (!ws._alive) { ws.terminate(); continue; }
      ws._alive = false;
      try { ws.ping(); } catch (_) { /* kapanıyor */ }
    }
  }, CFG.heartbeatMs);
  server.on('close', () => clearInterval(beat));
  server.wss = wss;
  return server;
}

/** Açık WebSocket'leri kapatıp sunucuyu durdurur; takılan soketler için süre sınırı var. */
function shutdown(server, timeoutMs = 5000) {
  return new Promise((resolve) => {
    const force = setTimeout(() => {
      for (const ws of server.wss.clients) ws.terminate();
    }, timeoutMs);
    force.unref();
    for (const ws of server.wss.clients) ws.close(1001, 'shutdown');
    server.wss.close();
    server.close(() => { clearTimeout(force); resolve(); });
  });
}

if (require.main === module) {
  if (CFG.keys.length === 0) {
    console.warn('[relay] UYARI: RELAY_APP_KEY ayarlı değil — her istemci kabul edilir.');
  }
  const server = createServer();
  server.listen(CFG.port, CFG.host, () => log(`dinleniyor ${CFG.host}:${CFG.port}`));
  let stopping = false;
  const stop = () => {
    if (stopping) return;
    stopping = true;
    log('kapanıyor');
    shutdown(server).then(() => process.exit(0));
  };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
}

module.exports = { createServer, shutdown, CFG, rooms, rate, pruneRate, clientIp };
