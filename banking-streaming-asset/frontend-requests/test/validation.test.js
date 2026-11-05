import { test } from 'node:test';
import assert from 'node:assert/strict';
import { validateLoanRequestInput, normalizeAmount } from '../src/validation.js';
import { buildLoanRequest, loanRequestKey } from '../src/loanRequest.js';

const valid = () => ({ customer_id: 1001, amount: 10000, term_months: 24, purpose: 'PERSONAL' });

test('request válido se acepta y el importe se normaliza a scale 2', () => {
  const r = validateLoanRequestInput(valid());
  assert.equal(r.ok, true);
  assert.deepEqual(r.value, {
    customer_id: 1001,
    amount: '10000.00',
    term_months: 24,
    purpose: 'PERSONAL',
  });
});

test('amount acepta número o string con hasta 2 decimales y el máximo decimal(12,2)', () => {
  assert.equal(normalizeAmount(1500.5), '1500.50');
  assert.equal(normalizeAmount('1500.5'), '1500.50');
  assert.equal(normalizeAmount('0.01'), '0.01');
  assert.equal(normalizeAmount('9999999999.99'), '9999999999.99');
});

test('customer_id no entero se rechaza', () => {
  for (const bad of [1001.5, '1001', null, undefined, NaN, true, [], 0, -1, 2147483648]) {
    const r = validateLoanRequestInput({ ...valid(), customer_id: bad });
    assert.equal(r.ok, false, `customer_id=${String(bad)}`);
    assert.ok(r.fields.includes('customer_id'));
  }
});

test('amount <= 0 se rechaza', () => {
  for (const bad of [0, -1, -0.01, '0', '0.00', '-5.00']) {
    const r = validateLoanRequestInput({ ...valid(), amount: bad });
    assert.equal(r.ok, false, `amount=${String(bad)}`);
    assert.ok(r.fields.includes('amount'));
  }
});

test('amount con más de 2 decimales se rechaza (no se redondea)', () => {
  for (const bad of [10.005, '10.005', '1000.999', 0.1 + 0.2]) {
    const r = validateLoanRequestInput({ ...valid(), amount: bad });
    assert.equal(r.ok, false, `amount=${String(bad)}`);
    assert.ok(r.fields.includes('amount'));
  }
});

test('amount fuera de precision(12,2) o con formato no decimal se rechaza', () => {
  for (const bad of ['10000000000.00', 12345678901, 1e21, '1e3', '0x10', ' 100', '', null, undefined, {}, Infinity]) {
    const r = validateLoanRequestInput({ ...valid(), amount: bad });
    assert.equal(r.ok, false, `amount=${String(bad)}`);
    assert.ok(r.fields.includes('amount'));
  }
});

test('term_months no permitido se rechaza', () => {
  for (const bad of [0, 6, 13, 360, '24', 24.5, null, undefined]) {
    const r = validateLoanRequestInput({ ...valid(), term_months: bad });
    assert.equal(r.ok, false, `term_months=${String(bad)}`);
    assert.ok(r.fields.includes('term_months'));
  }
  for (const good of [12, 24, 36, 48, 60, 120, 240]) {
    assert.equal(validateLoanRequestInput({ ...valid(), term_months: good }).ok, true);
  }
});

test('purpose no permitido se rechaza', () => {
  for (const bad of ['home', 'TRAVEL', '', null, undefined, 1, ['HOME']]) {
    const r = validateLoanRequestInput({ ...valid(), purpose: bad });
    assert.equal(r.ok, false, `purpose=${String(bad)}`);
    assert.ok(r.fields.includes('purpose'));
  }
  for (const good of ['HOME', 'CAR', 'EDUCATION', 'PERSONAL', 'BUSINESS', 'DEBT_CONSOLIDATION']) {
    assert.equal(validateLoanRequestInput({ ...valid(), purpose: good }).ok, true);
  }
});

test('campos desconocidos se rechazan, incluidos request_id y requested_at del cliente', () => {
  for (const extra of ['score', 'decision', 'request_id', 'requested_at', 'foo']) {
    const r = validateLoanRequestInput({ ...valid(), [extra]: 'x' });
    assert.equal(r.ok, false, extra);
    assert.ok(r.fields.includes(extra));
  }
});

test('body que no es un objeto se rechaza', () => {
  for (const bad of [null, undefined, 'x', 1, []]) {
    assert.equal(validateLoanRequestInput(bad).ok, false);
  }
});

test('request_id y requested_at los genera el servidor', () => {
  const v = validateLoanRequestInput(valid()).value;
  const fixedNow = new Date('2026-01-02T03:04:05.678Z');
  const lr = buildLoanRequest(v, { uuid: () => 'fixed-uuid', now: () => fixedNow });
  assert.equal(lr.request_id, 'fixed-uuid');
  assert.equal(lr.requested_at, fixedNow);

  const real = buildLoanRequest(v);
  assert.match(real.request_id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  assert.ok(real.requested_at instanceof Date);
  assert.notEqual(buildLoanRequest(v).request_id, real.request_id);
});

test('la clave del registro es String(customer_id)', () => {
  const lr = buildLoanRequest(validateLoanRequestInput(valid()).value);
  assert.equal(loanRequestKey(lr), '1001');
});
