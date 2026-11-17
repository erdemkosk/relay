'use strict';
// Protokol testleri: `npm test` (node test/protocol.test.js)
const assert = require('assert');
const WebSocket = require('ws');
const relay = require('../server.js');

const { CFG, rooms } = relay;
CFG.quiet = true;
CFG.keys = ['test-key'];
CFG.rateMax = 1000;

let server;
let base;

function connect(params) {
  const qs = new URLSearchParams({ key: 'test-key', ...params }).toString();
  const ws = new WebSocket(`${base}/?${qs}`);
  ws.inbox = [];
  ws.waiters = [];
  ws.on('message', (raw) => {
    const msg = JSON.parse(raw.toString());
    const i = ws.waiters.findIndex((w) => w.type === msg.type);
    if (i >= 0) ws.waiters.splice(i, 1)[0].resolve(msg);
    else ws.inbox.push(msg);
  });
  return ws;
}

function next(ws, type, timeoutMs = 2000) {
  const i = ws.inbox.findIndex((m) => m.type === type);
  if (i >= 0) return Promise.resolve(ws.inbox.splice(i, 1)[0]);
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`zaman aşımı: ${type}`)), timeoutMs);
    ws.waiters.push({ type, resolve: (m) => { clearTimeout(t); resolve(m); } });
  });
}

function closed(ws) {
  if (ws.readyState === WebSocket.CLOSED) return Promise.resolve();
  return new Promise((resolve) => ws.once('close', resolve));
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const tests = [];
function test(name, fn) { tests.push({ name, fn }); }

test('yanlış anahtar reddedilir', async () => {
  const ws = new WebSocket(`${base}/?key=nope&role=host&room=BIKE-A`);
  const code = await new Promise((resolve) => ws.on('close', (c) => resolve(c)));
  assert.strictEqual(code, 4001);
});

test('host ve istemci arasında signal taşınır', async () => {
  const host = connect({ role: 'host', room: 'BIKE-SIG' });
  const hw = await next(host, 'welcome');
  const client = connect({ role: 'client', room: 'BIKE-SIG' });
  const cw = await next(client, 'welcome');
  assert.strictEqual(cw.hostId, hw.peerId);
  const joined = await next(host, 'peer-joined');
  assert.strictEqual(joined.peerId, cw.peerId);

  client.send(JSON.stringify({ type: 'signal', to: hw.peerId, data: { sdp: 'x' } }));
  const sig = await next(host, 'signal');
  assert.strictEqual(sig.from, cw.peerId);
  assert.deepStrictEqual(sig.data, { sdp: 'x' });

  client.close(); host.close();
  await Promise.all([closed(client), closed(host)]);
});

test('ikinci host host-taken alır', async () => {
  const a = connect({ role: 'host', room: 'BIKE-HT' });
  await next(a, 'welcome');
  const b = connect({ role: 'host', room: 'BIKE-HT' });
  await next(b, 'host-taken');
  await closed(b);
  a.close(); await closed(a);
});

test('kapanan odanın gecikmeli close olayı aynı kodlu yeni odayı silmez', async () => {
  const oldGrace = CFG.hostGraceMs;
  CFG.hostGraceMs = 50;
  try {
    const host = connect({ role: 'host', room: 'BIKE-RACE' });
    await next(host, 'welcome');
    const client = connect({ role: 'client', room: 'BIKE-RACE' });
    await next(client, 'welcome');
    const oldRoom = rooms.get('BIKE-RACE');
    assert.ok(oldRoom);
    host.close(); await closed(host);
    await next(client, 'peer-left');

    // İstemci soketini okumayı durdur: sunucunun close çerçevesine cevap
    // veremez, sunucu tarafında close olayı gecikir. Bu arada grace dolar,
    // dropRoom eski odayı siler ve aynı kodla yeni oda açılır.
    client.pause();
    await sleep(CFG.hostGraceMs + 100);
    assert.notStrictEqual(rooms.get('BIKE-RACE'), oldRoom);
    const host2 = connect({ role: 'host', room: 'BIKE-RACE' });
    await next(host2, 'welcome');
    client.resume();
    await closed(client);
    await sleep(50);
    const newRoom = rooms.get('BIKE-RACE');
    assert.ok(newRoom, 'yeni oda silinmemeli');
    assert.notStrictEqual(newRoom, oldRoom);
    host2.close(); await closed(host2);
  } finally {
    CFG.hostGraceMs = oldGrace;
  }
});

test('lobi oda listesini alır', async () => {
  const host = connect({ role: 'host', room: 'BIKE-LOBBY' });
  await next(host, 'welcome');
  const lobby = connect({ role: 'lobby' });
  const msg = await next(lobby, 'rooms');
  assert.ok(msg.rooms.some((r) => r.id === 'BIKE-LOBBY'));
  lobby.close(); host.close();
  await Promise.all([closed(lobby), closed(host)]);
});

test('pruneRate süresi dolan kayıtları siler', async () => {
  relay.rate.set('1.2.3.4', { start: Date.now() - CFG.rateWindowMs - 1, count: 5 });
  relay.rate.set('5.6.7.8', { start: Date.now(), count: 1 });
  relay.pruneRate();
  assert.ok(!relay.rate.has('1.2.3.4'));
  assert.ok(relay.rate.has('5.6.7.8'));
  relay.rate.delete('5.6.7.8');
});

test('TRUST_PROXY açıkken en sağdaki X-Forwarded-For kullanılır', async () => {
  const req = { headers: { 'x-forwarded-for': '6.6.6.6, 10.0.0.1, 203.0.113.9' },
    socket: { remoteAddress: '127.0.0.1' } };
  CFG.trustProxy = true;
  try {
    assert.strictEqual(relay.clientIp(req), '203.0.113.9');
  } finally {
    CFG.trustProxy = false;
  }
  assert.strictEqual(relay.clientIp(req), '127.0.0.1');
});

test('shutdown açık soketleri kapatıp sunucuyu durdurur', async () => {
  const s = relay.createServer();
  await new Promise((r) => s.listen(0, '127.0.0.1', r));
  const ws = new WebSocket(`ws://127.0.0.1:${s.address().port}/?key=test-key&role=lobby`);
  await new Promise((r) => ws.once('open', r));
  const code = new Promise((resolve) => ws.on('close', (c) => resolve(c)));
  await relay.shutdown(s, 2000);
  assert.strictEqual(await code, 1001);
});

(async () => {
  server = relay.createServer();
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  base = `ws://127.0.0.1:${server.address().port}`;
  let failed = 0;
  for (const t of tests) {
    try {
      await t.fn();
      console.log(`ok   - ${t.name}`);
    } catch (e) {
      failed += 1;
      console.log(`FAIL - ${t.name}\n       ${e && e.stack}`);
    }
  }
  await relay.shutdown(server, 1000);
  console.log(failed ? `\n${failed} test başarısız` : `\n${tests.length} test geçti`);
  process.exit(failed ? 1 : 0);
})();
