import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import {
  createLoanRequestType,
  loadLoanRequestSchema,
  toAvroRecord,
  encodeLoanRequest,
  toConfluentWireFormat,
} from '../src/avro.js';
import { buildLoanRequest } from '../src/loanRequest.js';
import { validateLoanRequestInput } from '../src/validation.js';

const type = createLoanRequestType();
const fixedNow = new Date('2026-01-02T03:04:05.678Z');

function loanRequest(overrides = {}) {
  const v = validateLoanRequestInput({
    customer_id: 1001,
    amount: '12345.6',
    term_months: 24,
    purpose: 'PERSONAL',
    ...overrides,
  }).value;
  return buildLoanRequest(v, { uuid: () => 'req-1', now: () => fixedNow });
}

test('el esquema se carga directamente desde terraform/schemas/loan-requests.avsc', () => {
  const raw = JSON.parse(
    readFileSync(new URL('../../terraform/schemas/loan-requests.avsc', import.meta.url), 'utf8'),
  );
  assert.deepEqual(loadLoanRequestSchema(), raw);
  assert.equal(raw.name, 'LoanRequest');
  assert.equal(raw.namespace, 'io.confluent.banking');
});

test('el registro Avro tiene exactamente los campos y tipos del contrato', () => {
  assert.deepEqual(
    type.fields.map((f) => f.name),
    ['request_id', 'customer_id', 'amount', 'term_months', 'purpose', 'requested_at'],
  );
  assert.deepEqual(Object.keys(toAvroRecord(loanRequest())), type.fields.map((f) => f.name));
  assert.equal(type.fields[1].type.typeName, 'int');
  assert.equal(type.fields[2].type.underlyingType.typeName, 'bytes');
  assert.equal(type.fields[5].type.underlyingType.typeName, 'long');
});

test('el registro es compatible con loan-requests.avsc (valid + round trip)', () => {
  const lr = loanRequest();
  assert.equal(type.isValid(toAvroRecord(lr)), true);
  const decoded = type.fromBuffer(encodeLoanRequest(type, lr));
  assert.equal(decoded.request_id, 'req-1');
  assert.equal(decoded.customer_id, 1001);
  assert.equal(decoded.term_months, 24);
  assert.equal(decoded.purpose, 'PERSONAL');
});

test('decimal(12,2): se conserva scale 2 y la representación binaria es la esperada', () => {
  const raw = loadLoanRequestSchema();
  assert.equal(raw.fields[2].type.logicalType, 'decimal');
  assert.equal(raw.fields[2].type.precision, 12);
  assert.equal(raw.fields[2].type.scale, 2);

  const decoded = type.fromBuffer(encodeLoanRequest(type, loanRequest()));
  assert.equal(decoded.amount, '12345.60');
  for (const [input, expected] of [
    ['0.01', '0.01'],
    ['100', '100.00'],
    ['9999999999.99', '9999999999.99'],
  ]) {
    const out = type.fromBuffer(encodeLoanRequest(type, loanRequest({ amount: input })));
    assert.equal(out.amount, expected);
  }
});

test('decimal: bytes exactos del valor sin escala (complemento a dos, big-endian)', () => {
  const amountType = createLoanRequestType().fields[2].type;
  // 1234560 = 0x12D680 -> length-prefix zigzag(3)=6, luego 12 D6 80
  assert.deepEqual([...amountType.toBuffer('12345.60')], [0x06, 0x12, 0xd6, 0x80]);
  // 0.01 -> 1 -> 0x01
  assert.deepEqual([...amountType.toBuffer('0.01')], [0x02, 0x01]);
  // 128.00 -> 12800 = 0x3200
  assert.deepEqual([...amountType.toBuffer('128.00')], [0x04, 0x32, 0x00]);
});

test('decimal: el encoder no redondea ni acepta otra escala o precisión', () => {
  const lr = loanRequest();
  for (const bad of ['10.005', '10.1', '10', '10000000000.00', 10.5, null]) {
    assert.throws(() => encodeLoanRequest(type, { ...lr, amount: bad }), `amount=${String(bad)}`);
  }
});

test('timestamp-millis: Date <-> epoch millis exacto', () => {
  const tsType = createLoanRequestType().fields[5].type;
  const buf = tsType.toBuffer(fixedNow);
  assert.equal(tsType.fromBuffer(buf).getTime(), fixedNow.getTime());
  assert.equal(tsType.underlyingType.fromBuffer(buf), fixedNow.getTime());

  const decoded = type.fromBuffer(encodeLoanRequest(type, loanRequest()));
  assert.equal(decoded.requested_at.getTime(), 1767323045678);
  assert.throws(() => encodeLoanRequest(type, { ...loanRequest(), requested_at: new Date('x') }));
  assert.throws(() => encodeLoanRequest(type, { ...loanRequest(), requested_at: 1767323045678 }));
});

test('tipos incorrectos no cumplen el contrato', () => {
  const lr = loanRequest();
  assert.throws(() => encodeLoanRequest(type, { ...lr, customer_id: 1.5 }));
  assert.throws(() => encodeLoanRequest(type, { ...lr, customer_id: 2 ** 31 }));
  assert.throws(() => encodeLoanRequest(type, { ...lr, request_id: 123 }));
});

test('wire format de Confluent: magic byte 0 + schema id big-endian + payload', () => {
  const payload = encodeLoanRequest(type, loanRequest());
  const framed = toConfluentWireFormat(100001, payload);
  assert.equal(framed[0], 0);
  assert.equal(framed.readInt32BE(1), 100001);
  assert.deepEqual(framed.subarray(5), payload);
  assert.throws(() => toConfluentWireFormat(-1, payload), TypeError);
  assert.throws(() => toConfluentWireFormat('1', payload), TypeError);
});
