import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { createApp, HOST } from '../src/server.js';

const body = { customer_id: 1001, amount: 10000, term_months: 24, purpose: 'PERSONAL' };

function listen(deps) {
  return new Promise((resolve) => {
    const server = createApp(deps).listen(0, HOST, () => resolve(server));
  });
}

const post = (server, payload, raw = false) =>
  fetch(`http://${HOST}:${server.address().port}/api/loan-requests`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: raw ? payload : JSON.stringify(payload),
  });

const published = [];
const publisher = { publish: async (msg) => void published.push(msg) };
let server;
let failingServer;
let noPublisherServer;

before(async () => {
  server = await listen({
    publisher,
    uuid: () => 'req-xyz',
    now: () => new Date('2026-01-02T03:04:05.678Z'),
  });
  failingServer = await listen({
    publisher: { publish: async () => { throw new Error('broker secret-host:9092 SASL failure'); } },
    logger: () => {},
  });
  noPublisherServer = await listen({});
});

after(() => {
  for (const s of [server, failingServer, noPublisherServer]) s.close();
});

test('el servidor solo escucha en 127.0.0.1', () => {
  assert.equal(HOST, '127.0.0.1');
  assert.equal(server.address().address, '127.0.0.1');
});

test('POST válido: 202 con request_id/requested_at del servidor y sin score ni decision', async () => {
  const res = await post(server, body);
  assert.equal(res.status, 202);
  const json = await res.json();
  assert.deepEqual(json, { request_id: 'req-xyz', requested_at: '2026-01-02T03:04:05.678Z' });
  for (const forbidden of ['score', 'decision', 'status', 'approved', 'rejected']) {
    assert.ok(!(forbidden in json), forbidden);
  }
  assert.equal(published.length, 1);
  assert.equal(published[0].key, '1001');
  assert.equal(published[0].loanRequest.amount, '10000.00');
});

test('el cliente no puede fijar request_id ni requested_at', async () => {
  const before = published.length;
  const res = await post(server, { ...body, request_id: 'mine', requested_at: 1 });
  assert.equal(res.status, 400);
  assert.equal(published.length, before);
});

test('entrada inválida: 400 genérico, sin detalles de campos ni esquema', async () => {
  const res = await post(server, { ...body, amount: 10.005 });
  assert.equal(res.status, 400);
  const text = await res.text();
  assert.deepEqual(JSON.parse(text), { error: 'Invalid request' });
  assert.ok(!/amount|schema|avro|kafka/i.test(text));
});

test('JSON malformado: 400 genérico', async () => {
  const res = await post(server, '{not json', true);
  assert.equal(res.status, 400);
  assert.deepEqual(await res.json(), { error: 'Invalid request' });
});

test('fallo del publisher: 503 genérico sin filtrar detalles internos', async () => {
  const res = await post(failingServer, body);
  assert.equal(res.status, 503);
  const text = await res.text();
  assert.deepEqual(JSON.parse(text), { error: 'Service unavailable' });
  assert.ok(!/secret|broker|SASL|9092/i.test(text));
});

test('sin publisher inyectado: 503 y nunca una decisión simulada', async () => {
  const res = await post(noPublisherServer, body);
  assert.equal(res.status, 503);
  const text = await res.text();
  assert.ok(!/APPROVED|REJECTED|score|decision/i.test(text));
});

test('rutas desconocidas: 404 genérico', async () => {
  const res = await fetch(`http://${HOST}:${server.address().port}/api/other`);
  assert.equal(res.status, 404);
  assert.deepEqual(await res.json(), { error: 'Not found' });
});
