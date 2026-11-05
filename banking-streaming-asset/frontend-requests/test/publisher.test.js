import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loadConfig, ConfigError, REQUIRED_ENV } from '../src/config.js';
import { createSchemaIdResolver, SchemaRegistryError } from '../src/schemaRegistry.js';
import { createLoanRequestPublisher, PublishError } from '../src/publisher.js';
import { createLoanRequestType, loadLoanRequestSchema } from '../src/avro.js';
import { createRuntime } from '../src/runtime.js';
import { SECRETS, goodEnv } from './helpers.js';

const schema = loadLoanRequestSchema();
const avroType = createLoanRequestType(schema);
const loanRequest = {
  request_id: 'req-1',
  customer_id: 1001,
  amount: '10000.00',
  term_months: 24,
  purpose: 'PERSONAL',
  requested_at: new Date('2026-01-02T03:04:05.678Z'),
};

const srResponse = (id, body = JSON.stringify(schema)) => async () =>
  new Response(JSON.stringify({ subject: 'loan-requests-value', version: 1, id, schema: body }), { status: 200 });

// ---------- config ----------

test('loadConfig usa exclusivamente las 7 variables requeridas', () => {
  assert.equal(REQUIRED_ENV.length, 7);
  const cfg = loadConfig(goodEnv());
  assert.deepEqual(cfg.brokers, ['pkc-test.eu-west-1.aws.confluent.cloud:9092']);
  assert.equal(cfg.topic, 'loan-requests');
  assert.equal(cfg.schemaRegistry.subject, 'loan-requests-value');
});

test('loadConfig falla si falta cualquier variable y no imprime valores', () => {
  for (const name of REQUIRED_ENV) {
    const env = goodEnv();
    delete env[name];
    assert.throws(
      () => loadConfig(env),
      (err) => {
        assert.ok(err instanceof ConfigError);
        assert.ok(err.message.includes(name));
        for (const s of Object.values(SECRETS)) assert.ok(!err.message.includes(s));
        return true;
      },
    );
  }
});

test('loadConfig rechaza valores mal formados sin revelarlos', () => {
  const bad = [
    { KAFKA_BOOTSTRAP_SERVERS: 'sin-puerto' },
    { SCHEMA_REGISTRY_URL: 'http://insecure.example' },
    { SCHEMA_REGISTRY_URL: 'https://user:pw@sr.example' },
    { SCHEMA_REGISTRY_URL: 'no-url' },
    { TOPIC_LOAN_REQUESTS: 'bad topic!' },
  ];
  for (const override of bad) {
    assert.throws(
      () => loadConfig({ ...goodEnv(), ...override }),
      (err) => {
        assert.ok(err instanceof ConfigError);
        for (const v of Object.values(override)) assert.ok(!err.message.includes(v), v);
        return true;
      },
    );
  }
});

// ---------- schema registry ----------

const srCfg = () => ({ ...loadConfig(goodEnv()).schemaRegistry });

test('Schema Registry: obtiene el ID existente con GET versions/latest y lo cachea', async () => {
  const calls = [];
  const fetchImpl = async (url, init) => {
    calls.push({ url, init });
    return srResponse(100001)();
  };
  const r = createSchemaIdResolver(srCfg(), schema, { fetchImpl });
  assert.equal(await r.resolve(), 100001);
  assert.equal(await r.resolve(), 100001);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].init.method, 'GET'); // nunca POST: no se registran esquemas
  assert.equal(
    calls[0].url,
    'https://psrc-test.eu-west-1.aws.confluent.cloud/subjects/loan-requests-value/versions/latest',
  );
  assert.equal(
    calls[0].init.headers.authorization,
    `Basic ${Buffer.from(`${SECRETS.srKey}:${SECRETS.srSecret}`).toString('base64')}`,
  );
});

test('Schema Registry: fallos de red, HTTP, cuerpo o contrato dan error genérico sin secretos', async () => {
  const cases = {
    red: async () => { throw new Error(`connect ECONNREFUSED ${SECRETS.srSecret}`); },
    http401: async () => new Response(`unauthorized ${SECRETS.srKey}`, { status: 401 }),
    jsonInvalido: async () => new Response('<html>', { status: 200 }),
    sinId: async () => new Response(JSON.stringify({ schema: JSON.stringify(schema) }), { status: 200 }),
    esquemaDistinto: srResponse(7, JSON.stringify({ ...schema, name: 'Otro' })),
  };
  for (const [name, fetchImpl] of Object.entries(cases)) {
    const r = createSchemaIdResolver(srCfg(), schema, { fetchImpl });
    await assert.rejects(r.resolve(), (err) => {
      assert.ok(err instanceof SchemaRegistryError, name);
      for (const s of Object.values(SECRETS)) assert.ok(!err.message.includes(s), name);
      assert.ok(!/https?:\/\//.test(err.message), name);
      return true;
    });
  }
});

test('Schema Registry: un fallo no se cachea y un reintento posterior puede tener éxito', async () => {
  let n = 0;
  const fetchImpl = async () => (++n === 1 ? new Response('x', { status: 503 }) : srResponse(5)());
  const r = createSchemaIdResolver(srCfg(), schema, { fetchImpl });
  await assert.rejects(r.resolve(), SchemaRegistryError);
  assert.equal(await r.resolve(), 5);
});

// ---------- publisher ----------

function fakeProducer({ onSend } = {}) {
  const sent = [];
  return {
    sent,
    async send(opts) {
      sent.push(opts);
      if (onSend) return onSend(opts);
      return { offsets: [{ topic: 'loan-requests', partition: 0, offset: 18n }] };
    },
    async metadata() { return {}; },
    async close() { this.closed = true; },
  };
}

const makePublisher = (producer, schemaId = 100001, extra = {}) =>
  createLoanRequestPublisher(
    { topic: 'loan-requests' },
    { avroType, schemaIds: { resolve: async () => schemaId }, producer, ...extra },
  );

test('publisher: un publish envía exactamente un mensaje al topic con key String(customer_id) y acks=all', async () => {
  const producer = fakeProducer();
  await makePublisher(producer).publish({ key: '1001', loanRequest });
  assert.equal(producer.sent.length, 1);
  assert.equal(producer.sent[0].acks, -1);
  assert.equal(producer.sent[0].messages.length, 1);
  const msg = producer.sent[0].messages[0];
  assert.equal(msg.topic, 'loan-requests');
  assert.equal(msg.key.toString('utf8'), '1001');
});

test('publisher: wire format Confluent (magic 0 + schema ID big-endian + Avro)', async () => {
  const producer = fakeProducer();
  await makePublisher(producer, 0x01020304).publish({ key: '1001', loanRequest });
  const { value } = producer.sent[0].messages[0];
  assert.equal(value[0], 0);
  assert.deepEqual([...value.subarray(1, 5)], [0x01, 0x02, 0x03, 0x04]);
  const decoded = avroType.fromBuffer(value.subarray(5));
  assert.equal(decoded.request_id, 'req-1');
  assert.equal(decoded.amount, '10000.00');
  assert.equal(decoded.requested_at.getTime(), loanRequest.requested_at.getTime());
});

test('publisher: sin confirmación del broker o con error no resuelve', async () => {
  await assert.rejects(makePublisher(fakeProducer({ onSend: () => ({ offsets: [] }) })).publish({ key: '1', loanRequest }), PublishError);
  await assert.rejects(makePublisher(fakeProducer({ onSend: () => { throw new Error('boom'); } })).publish({ key: '1', loanRequest }));
});

test('publisher: timeout finito (no espera indefinidamente)', async () => {
  const hanging = fakeProducer({ onSend: () => new Promise(() => {}) });
  const t0 = Date.now();
  await assert.rejects(makePublisher(hanging, 1, { publishTimeoutMs: 50 }).publish({ key: '1', loanRequest }), PublishError);
  assert.ok(Date.now() - t0 < 2000);
});

test('publisher: si Schema Registry falla no se envía nada a Kafka', async () => {
  const producer = fakeProducer();
  const publisher = createLoanRequestPublisher(
    { topic: 'loan-requests' },
    { avroType, schemaIds: { resolve: async () => { throw new SchemaRegistryError('x'); } }, producer },
  );
  await assert.rejects(publisher.publish({ key: '1', loanRequest }), SchemaRegistryError);
  assert.equal(producer.sent.length, 0);
});

test('publisher: un registro que no cumple el contrato Avro no se envía', async () => {
  const producer = fakeProducer();
  await assert.rejects(makePublisher(producer).publish({ key: '1', loanRequest: { ...loanRequest, amount: '10.5' } }));
  assert.equal(producer.sent.length, 0);
});

// ---------- runtime ----------

test('runtime: connect valida el schema ID y disconnect cierra el producer', async () => {
  const producer = fakeProducer();
  const rt = createRuntime(goodEnv(), { producer, fetchImpl: srResponse(9) });
  await rt.connect();
  await rt.publisher.publish({ key: '1001', loanRequest });
  assert.equal(producer.sent.length, 1);
  await rt.disconnect();
  assert.equal(producer.closed, true);
});

test('runtime: configuración inválida falla antes de crear ningún cliente', () => {
  assert.throws(() => createRuntime({}, { producer: fakeProducer() }), ConfigError);
});
