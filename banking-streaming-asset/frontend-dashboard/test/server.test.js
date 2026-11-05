import { test } from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { EventEmitter } from 'node:events';
import { createApp, parseLimit, resolveHost, resolvePort, run } from '../src/server.js';
import { createDecisionStore } from '../src/store.js';
import { createDecisionConsumer } from '../src/consumer.js';
import { createDecisionDecoder } from '../src/decoder.js';
import { encodeValue, fakeRegistry, goodEnv, rawKey, registryCfg, sampleRecord, SECRETS } from './helpers.js';

const dec = (id, over = {}) => ({
  request_id: id, customer_id: 1001, amount: '1000.00', term_months: 12, purpose: 'PERSONAL',
  requested_at: '2026-11-04T10:00:00.000Z', decided_at: '2026-11-04T10:05:00.000Z',
  score: 100, decision: 'APPROVED', reasons: 'MEETS_CRITERIA', model: 'RULES_V1', ...over,
});

async function withApp(store, fn) {
  const server = createApp({ store, logger: () => {} }).listen(0, '127.0.0.1');
  await once(server, 'listening');
  const base = `http://127.0.0.1:${server.address().port}`;
  try {
    await fn(base);
  } finally {
    server.close();
    server.closeAllConnections();
  }
}

test('GET /health -> 200 {status:ok} sin tocar Kafka', async () => {
  await withApp(createDecisionStore(), async (base) => {
    const res = await fetch(`${base}/health`);
    assert.equal(res.status, 200);
    assert.deepEqual(await res.json(), { status: 'ok' });
  });
});

test('GET /api/summary con estado vacío: ceros reales, sin datos inventados', async () => {
  await withApp(createDecisionStore(), async (base) => {
    const res = await fetch(`${base}/api/summary`);
    assert.equal(res.status, 200);
    assert.equal(res.headers.get('cache-control'), 'no-store');
    const body = await res.json();
    assert.equal(body.total, 0);
    assert.equal(body.approval_rate, null);
    assert.equal(body.average_score, null);
  });
});

test('GET /api/summary y /api/decisions reflejan el estado en memoria', async () => {
  const store = createDecisionStore();
  store.add(dec('a'));
  store.add(dec('b', { decision: 'REJECTED', score: 20 }));
  await withApp(store, async (base) => {
    const s = await (await fetch(`${base}/api/summary`)).json();
    assert.equal(s.total, 2);
    assert.equal(s.approved, 1);
    assert.equal(s.rejected, 1);

    const body = await (await fetch(`${base}/api/decisions?limit=1`)).json();
    assert.deepEqual(body.decisions.map((d) => d.request_id), ['b']);
    const all = await (await fetch(`${base}/api/decisions`)).json(); // limit por defecto
    assert.equal(all.decisions.length, 2);
  });
});

test('GET /api/decisions: limit inválido -> 400 genérico', async () => {
  await withApp(createDecisionStore(), async (base) => {
    for (const q of ['0', '-1', '201', 'abc', '1.5', '', '1&limit=2']) {
      const res = await fetch(`${base}/api/decisions?limit=${q}`);
      assert.equal(res.status, 400, `limit=${q}`);
      assert.deepEqual(await res.json(), { error: 'Invalid request' });
    }
  });
});

test('parseLimit: por defecto 50, máximo 200', () => {
  assert.equal(parseLimit(undefined), 50);
  assert.equal(parseLimit('200'), 200);
  assert.equal(parseLimit('201'), null);
  assert.equal(parseLimit(['1', '2']), null);
});

test('solo existen /health, /api/summary y /api/decisions (sin /api/source ni /api/stream)', async () => {
  await withApp(createDecisionStore(), async (base) => {
    for (const p of ['/api/source', '/api/stream', '/api', '/api/otra']) {
      const res = await fetch(`${base}${p}`);
      assert.equal(res.status, 404, p);
      assert.deepEqual(await res.json(), { error: 'Not found' });
    }
    const post = await fetch(`${base}/api/summary`, { method: 'POST' });
    assert.equal(post.status, 404);
  });
});

test('resolveHost / resolvePort', () => {
  assert.equal(resolveHost(undefined), '127.0.0.1');
  assert.equal(resolveHost('0.0.0.0'), '0.0.0.0');
  assert.throws(() => resolveHost(''));
  assert.throws(() => resolveHost('10.0.0.1'));
  assert.equal(resolvePort(undefined), 3001);
  assert.equal(resolvePort('8081'), 8081);
  assert.throws(() => resolvePort('0'));
  assert.throws(() => resolvePort('abc'));
});

// --- consumer con stream simulado ---------------------------------------------------------------------

function fakeStream(messages) {
  return {
    async *[Symbol.asyncIterator]() {
      for (const m of messages) yield m;
    },
    closed: false,
    async close() {
      this.closed = true;
    },
  };
}

test('consumer: decodifica, deduplica, omite mensajes ilegibles y sigue', async () => {
  const { fetchImpl } = fakeRegistry();
  const decoder = createDecisionDecoder(registryCfg(), { fetchImpl });
  const store = createDecisionStore();
  const logs = [];
  const stream = fakeStream([
    { key: rawKey('r1'), value: encodeValue(sampleRecord({ score: 50, decision: 'REJECTED' })) },
    { key: rawKey('r2'), value: encodeValue(sampleRecord()) },
    { key: rawKey('bad'), value: Buffer.from([1, 2, 3]) }, // ilegible
    { key: rawKey('r2'), value: null }, // tombstone: se ignora
    { key: rawKey('r1'), value: encodeValue(sampleRecord({ score: 90 })) }, // reemisión
  ]);
  const calls = [];
  const consumer = {
    consume: async (opts) => { calls.push(opts); return stream; },
    close: async () => {},
  };
  const dc = createDecisionConsumer({ topic: 'loan-decisions' }, { consumer, decoder, store, logger: (e) => logs.push(e) });
  await dc.start();
  await new Promise((r) => setImmediate(r));
  await new Promise((r) => setImmediate(r));

  assert.equal(calls[0].mode, 'earliest');
  assert.deepEqual(calls[0].topics, ['loan-decisions']);
  assert.equal(store.size, 2);
  assert.equal(store.summary().approved, 2);
  assert.equal(store.recent(1)[0].request_id, 'r1');
  assert.deepEqual(dc.stats(), { received: 3, skipped: 1 });
  assert.ok(logs.some((l) => l.event === 'decision_skipped'));
  await dc.stop();
  assert.equal(stream.closed, true);
});

// --- ciclo de vida de run() -----------------------------------------------------------------------------

test('run(): configuración inválida falla antes de cualquier conexión, sin valores', async () => {
  const env = goodEnv();
  delete env.DASHBOARD_API_SR_API_SECRET;
  let touched = false;
  await assert.rejects(
    run({ env, port: 0, proc: new EventEmitter(), logger: () => {}, decoderFactory: () => { touched = true; } }),
    (err) => {
      assert.equal(err.name, 'ConfigError');
      for (const s of Object.values(SECRETS)) assert.ok(!err.message.includes(s));
      return true;
    },
  );
  assert.equal(touched, false);
});

test('run(): arranca, sirve la API y se apaga limpiamente con SIGTERM', async () => {
  const { fetchImpl } = fakeRegistry();
  const proc = new EventEmitter();
  proc.exit = () => {};
  const logs = [];
  const stream = fakeStream([{ key: rawKey('r1'), value: encodeValue(sampleRecord()) }]);
  let consumerClosed = false;
  const { server, shutdown, store } = await run({
    env: goodEnv(),
    port: 0,
    proc,
    logger: (e) => logs.push(e),
    decoderFactory: (cfg) => createDecisionDecoder(cfg, { fetchImpl }),
    consumerFactory: () => ({ consume: async () => stream, close: async () => { consumerClosed = true; } }),
  });
  await new Promise((r) => setImmediate(r));
  await new Promise((r) => setImmediate(r));

  const base = `http://127.0.0.1:${server.address().port}`;
  const summary = await (await fetch(`${base}/api/summary`)).json();
  assert.equal(summary.total, 1);
  assert.equal(store.size, 1);

  proc.emit('SIGTERM');
  await shutdown('SIGTERM');
  assert.equal(consumerClosed, true);
  assert.ok(logs.some((l) => l.event === 'schema_resolved' && l.schema_id === 100011));
  assert.ok(logs.some((l) => l.event === 'shutdown'));
  const text = JSON.stringify(logs);
  for (const s of Object.values(SECRETS)) assert.ok(!text.includes(s));
});
