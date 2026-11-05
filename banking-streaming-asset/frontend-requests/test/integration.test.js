import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { createApp, HOST, run } from '../src/server.js';
import { createRuntime } from '../src/runtime.js';
import { createLoanRequestType, loadLoanRequestSchema } from '../src/avro.js';
import { SECRETS, goodEnv } from './helpers.js';

const schema = loadLoanRequestSchema();
const avroType = createLoanRequestType(schema);
const body = { customer_id: 1001, amount: '10000.5', term_months: 24, purpose: 'PERSONAL' };
const FIXED_NOW = new Date('2026-01-02T03:04:05.678Z');

const srOk = async () =>
  new Response(JSON.stringify({ id: 100001, schema: JSON.stringify(schema) }), { status: 200 });

function mockProducer({ onSend } = {}) {
  const sent = [];
  return {
    sent,
    closed: false,
    async send(opts) {
      sent.push(opts);
      if (onSend) return onSend(opts);
      return { offsets: [{ topic: 'loan-requests', partition: 0, offset: 1n }] };
    },
    async metadata() { return {}; },
    async close() { this.closed = true; },
  };
}

async function withApp({ producer, fetchImpl = srOk }, fn) {
  const logs = [];
  const rt = createRuntime(goodEnv(), { producer, fetchImpl });
  const server = await new Promise((resolve) => {
    const s = createApp({
      publisher: rt.publisher,
      uuid: () => 'server-uuid',
      now: () => FIXED_NOW,
      logger: (e) => logs.push(JSON.stringify(e)),
    }).listen(0, HOST, () => resolve(s));
  });
  try {
    const post = (payload) =>
      fetch(`http://${HOST}:${server.address().port}/api/loan-requests`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(payload),
      });
    await fn({ post, logs });
  } finally {
    server.close();
  }
}

const assertNoSecrets = (text) => {
  for (const s of Object.values(SECRETS)) assert.ok(!text.includes(s), 'secreto filtrado');
};

test('POST válido publica exactamente una vez y responde 202 solo tras la confirmación', async () => {
  const producer = mockProducer();
  await withApp({ producer }, async ({ post }) => {
    const res = await post(body);
    assert.equal(res.status, 202);
    assert.deepEqual(await res.json(), { request_id: 'server-uuid', requested_at: '2026-01-02T03:04:05.678Z' });
    assert.equal(producer.sent.length, 1);

    const msg = producer.sent[0].messages[0];
    assert.equal(msg.topic, 'loan-requests');
    assert.equal(msg.key.toString('utf8'), '1001'); // String(customer_id)
    assert.equal(msg.value[0], 0); // magic byte
    assert.equal(msg.value.readUInt32BE(1), 100001); // schema ID big-endian
    const decoded = avroType.fromBuffer(msg.value.subarray(5));
    assert.equal(decoded.request_id, 'server-uuid'); // generado por el servidor
    assert.equal(decoded.requested_at.getTime(), FIXED_NOW.getTime()); // generado por el servidor
    assert.equal(decoded.amount, '10000.50');
  });
});

test('la respuesta nunca incluye score ni decision', async () => {
  await withApp({ producer: mockProducer() }, async ({ post }) => {
    const text = await (await post(body)).text();
    assert.ok(!/score|decision|approved|rejected/i.test(text));
  });
});

test('el cliente no puede imponer request_id/requested_at y no se publica nada', async () => {
  const producer = mockProducer();
  await withApp({ producer }, async ({ post }) => {
    const res = await post({ ...body, request_id: 'mine', requested_at: 1 });
    assert.equal(res.status, 400);
    assert.equal(producer.sent.length, 0);
  });
});

test('fallo de Kafka: 503 genérico, sin 202, sin detalles ni secretos en respuesta ni logs', async () => {
  const producer = mockProducer({
    onSend: () => {
      throw new Error(`SASL failed for ${SECRETS.kafkaKey}:${SECRETS.kafkaSecret} at broker-1.internal:9092\n    at stack`);
    },
  });
  await withApp({ producer }, async ({ post, logs }) => {
    const res = await post(body);
    assert.equal(res.status, 503);
    const text = await res.text();
    assert.deepEqual(JSON.parse(text), { error: 'Service unavailable' });
    assert.ok(!/SASL|broker|stack|9092/i.test(text));
    assertNoSecrets(text);
    assertNoSecrets(logs.join('\n'));
    assert.ok(logs.length > 0);
  });
});

test('fallo de Schema Registry: 503 genérico, nada llega a Kafka, sin secretos', async () => {
  const producer = mockProducer();
  const fetchImpl = async () => {
    throw new Error(`ECONNRESET https://psrc-test.example ${SECRETS.srKey}:${SECRETS.srSecret}`);
  };
  await withApp({ producer, fetchImpl }, async ({ post, logs }) => {
    const res = await post(body);
    assert.equal(res.status, 503);
    const text = await res.text();
    assert.deepEqual(JSON.parse(text), { error: 'Service unavailable' });
    assert.ok(!/psrc|ECONNRESET|registry|schema/i.test(text));
    assertNoSecrets(text);
    assertNoSecrets(logs.join('\n'));
    assert.equal(producer.sent.length, 0);
  });
});

test('Schema Registry con esquema distinto al contrato: 503 y no se publica', async () => {
  const producer = mockProducer();
  const fetchImpl = async () =>
    new Response(JSON.stringify({ id: 1, schema: JSON.stringify({ ...schema, name: 'Other' }) }), { status: 200 });
  await withApp({ producer, fetchImpl }, async ({ post }) => {
    assert.equal((await post(body)).status, 503);
    assert.equal(producer.sent.length, 0);
  });
});

test('Kafka sin confirmación (sin offsets): 503, nunca 202', async () => {
  await withApp({ producer: mockProducer({ onSend: () => ({}) }) }, async ({ post }) => {
    assert.equal((await post(body)).status, 503);
  });
});

// ---------- lifecycle ----------

function fakeProc() {
  const proc = new EventEmitter();
  proc.exitCode = null;
  proc.exit = (code) => { proc.exitCode = code; };
  return proc;
}

test('run: valida configuración antes de escuchar (falla sin imprimir valores)', async () => {
  await assert.rejects(
    run({ env: { ...goodEnv(), SCHEMA_REGISTRY_URL: '' }, proc: fakeProc(), logger: () => {} }),
    (err) => {
      assert.equal(err.name, 'ConfigError');
      assertNoSecrets(err.message);
      return true;
    },
  );
});

test('run: conecta, escucha en 127.0.0.1 y SIGTERM/SIGINT cierran servidor y producer', async () => {
  for (const signal of ['SIGTERM', 'SIGINT']) {
    const producer = mockProducer();
    const proc = fakeProc();
    const logs = [];
    const { server, shutdown } = await run({
      env: goodEnv(),
      port: 0,
      runtimeFactory: (env) => createRuntime(env, { producer, fetchImpl: srOk }),
      proc,
      logger: (e) => logs.push(JSON.stringify(e)),
    });
    assert.equal(server.address().address, '127.0.0.1');

    proc.emit(signal);
    await shutdown(signal); // misma promesa: espera el cierre
    assert.equal(server.listening, false);
    assert.equal(producer.closed, true);
    assert.equal(proc.exitCode, null);
    assertNoSecrets(logs.join('\n'));
  }
});

test('run: un segundo signal no vuelve a cerrar (shutdown idempotente)', async () => {
  const producer = mockProducer();
  let closes = 0;
  producer.close = async () => { closes++; };
  const proc = fakeProc();
  const { shutdown } = await run({
    env: goodEnv(),
    port: 0,
    runtimeFactory: (env) => createRuntime(env, { producer, fetchImpl: srOk }),
    proc,
    logger: () => {},
  });
  await Promise.all([shutdown('SIGTERM'), shutdown('SIGINT')]);
  assert.equal(closes, 1);
});
