import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createDecisionDecoder, DecodeError, parseKey } from '../src/decoder.js';
import { avroKey, avroUnionKey, encodeValue, fakeRegistry, rawKey, registryCfg, sampleRecord, SECRETS } from './helpers.js';

test('parseKey: raw, Avro con framing y Avro con unión', () => {
  assert.equal(parseKey(rawKey('req-1')), 'req-1');
  assert.equal(parseKey(avroKey('req-2')), 'req-2');
  assert.equal(parseKey(avroUnionKey('req-3')), 'req-3');
});

test('parseKey: claves ilegibles -> null', () => {
  assert.equal(parseKey(null), null);
  assert.equal(parseKey(Buffer.alloc(0)), null);
  assert.equal(parseKey(Buffer.from([0x01, 0xff, 0xfe])), null);
  assert.equal(parseKey(Buffer.concat([Buffer.from([0, 0, 0, 0, 1]), Buffer.from([0x7f])])), null);
});

test('decode: value Avro + key raw -> decisión normalizada', async () => {
  const { fetchImpl } = fakeRegistry();
  const decoder = createDecisionDecoder(registryCfg(), { fetchImpl });
  const d = await decoder.decode(rawKey('abc-123'), encodeValue(sampleRecord()));
  assert.deepEqual(d, {
    request_id: 'abc-123',
    customer_id: 1001,
    amount: '10000.00',
    term_months: 24,
    purpose: 'PERSONAL',
    requested_at: '2026-11-04T17:50:40.680Z',
    decided_at: '2026-11-04T17:55:55.000Z',
    score: 100,
    decision: 'APPROVED',
    reasons: 'MEETS_CRITERIA',
    model: 'RULES_V1',
  });
});

test('decode: key Avro con framing y rechazo con dti_ratio nulo', async () => {
  const { fetchImpl } = fakeRegistry();
  const decoder = createDecisionDecoder(registryCfg(), { fetchImpl });
  const d = await decoder.decode(
    avroKey('req-9'),
    encodeValue(sampleRecord({ decision: 'REJECTED', score: 0, dti_ratio: null, reasons: 'CUSTOMER_NOT_FOUND' })),
  );
  assert.equal(d.request_id, 'req-9');
  assert.equal(d.decision, 'REJECTED');
  assert.equal(d.reasons, 'CUSTOMER_NOT_FOUND');
});

test('decode: importes pequeños y con parte decimal (escala 2)', async () => {
  const { fetchImpl } = fakeRegistry();
  const decoder = createDecisionDecoder(registryCfg(), { fetchImpl });
  const amountOf = async (cents) => {
    const rec = sampleRecord();
    let hex = BigInt(cents).toString(16);
    if (hex.length % 2) hex = `0${hex}`;
    if (Number.parseInt(hex.slice(0, 2), 16) >= 0x80) hex = `00${hex}`;
    rec.amount = Buffer.from(hex, 'hex');
    return (await decoder.decode(rawKey('x'), encodeValue(rec))).amount;
  };
  assert.equal(await amountOf(5), '0.05');
  assert.equal(await amountOf(123456), '1234.56');
  assert.equal(await amountOf(200000000), '2000000.00');
});

test('decode: tombstone (value nulo) -> null; el esquema se pide una vez por ID y solo con GET', async () => {
  const { fetchImpl, calls } = fakeRegistry();
  const decoder = createDecisionDecoder(registryCfg(), { fetchImpl });
  assert.equal(await decoder.decode(rawKey('t'), null), null);
  await decoder.decode(rawKey('a'), encodeValue(sampleRecord()));
  await decoder.decode(rawKey('b'), encodeValue(sampleRecord()));
  assert.equal(calls.length, 1);
  assert.match(calls[0].url, /\/schemas\/ids\/100011\?subject=loan-decisions-value$/);
  assert.equal(calls[0].init.method, 'GET');
});

test('decode: value mal formado -> DecodeError sin filtrar secretos', async () => {
  const { fetchImpl } = fakeRegistry();
  const decoder = createDecisionDecoder(registryCfg(), { fetchImpl });
  const bad = [
    [rawKey('k'), Buffer.from([1, 2, 3])], // sin framing
    [rawKey('k'), Buffer.concat([Buffer.alloc(5), Buffer.from([0xff, 0xff])])], // Avro corrupto
    [Buffer.from([0x01, 0xff]), encodeValue(sampleRecord())], // clave ilegible
    [rawKey('k'), encodeValue(sampleRecord({ decision: 'MAYBE' }))], // decisión inesperada
  ];
  for (const [key, value] of bad) {
    await assert.rejects(decoder.decode(key, value), (err) => {
      assert.ok(err instanceof DecodeError);
      for (const s of Object.values(SECRETS)) assert.ok(!err.message.includes(s));
      return true;
    });
  }
});

test('decode: Schema Registry no disponible -> DecodeError genérico', async () => {
  for (const fetchImpl of [fakeRegistry({ status: 403 }).fetchImpl, async () => { throw new Error(`boom ${SECRETS.srSecret}`); }]) {
    const decoder = createDecisionDecoder(registryCfg(), { fetchImpl });
    await assert.rejects(decoder.decode(rawKey('k'), encodeValue(sampleRecord())), (err) => {
      assert.ok(err instanceof DecodeError);
      assert.ok(!err.message.includes(SECRETS.srSecret));
      return true;
    });
  }
});

test('connect: resuelve la última versión de loan-decisions-value en solo lectura', async () => {
  const { fetchImpl, calls } = fakeRegistry();
  const decoder = createDecisionDecoder(registryCfg(), { fetchImpl });
  assert.deepEqual(await decoder.connect(), { subject: 'loan-decisions-value', schemaId: 100011 });
  assert.match(calls[0].url, /\/subjects\/loan-decisions-value\/versions\/latest$/);
  assert.equal(calls[0].init.method, 'GET');
  assert.match(calls[0].init.headers.authorization, /^Basic /);
});
