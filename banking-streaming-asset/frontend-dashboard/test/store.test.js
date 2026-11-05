import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createDecisionStore } from '../src/store.js';

const dec = (id, over = {}) => ({
  request_id: id,
  customer_id: 1001,
  amount: '1000.00',
  term_months: 12,
  purpose: 'PERSONAL',
  requested_at: '2026-11-04T10:00:00.000Z',
  decided_at: '2026-11-04T10:05:00.000Z',
  score: 100,
  decision: 'APPROVED',
  reasons: 'MEETS_CRITERIA',
  model: 'RULES_V1',
  ...over,
});

test('store vacío: totales a cero y tasa/media nulas (no se inventan valores)', () => {
  const s = createDecisionStore().summary();
  assert.equal(s.total, 0);
  assert.equal(s.approval_rate, null);
  assert.equal(s.average_score, null);
  assert.equal(s.total_approved_amount, '0.00');
  assert.deepEqual(s.score_distribution.map((b) => b.count), [0, 0, 0, 0, 0]);
});

test('summary: agrega aprobadas/rechazadas, media, importe aprobado y distribución de score', () => {
  const store = createDecisionStore();
  store.add(dec('a', { score: 100, amount: '10000.00' }));
  store.add(dec('b', { score: 20, decision: 'REJECTED', amount: '3000.00' }));
  store.add(dec('c', { score: 60, amount: '0.05' }));
  store.add(dec('d', { score: 0, decision: 'REJECTED' }));
  const s = store.summary();
  assert.equal(s.total, 4);
  assert.equal(s.approved, 2);
  assert.equal(s.rejected, 2);
  assert.equal(s.approval_rate, 0.5);
  assert.equal(s.average_score, 45);
  assert.equal(s.total_approved_amount, '10000.05');
  assert.deepEqual(s.score_distribution, [
    { range: '0-19', count: 1 },
    { range: '20-39', count: 1 },
    { range: '40-59', count: 0 },
    { range: '60-79', count: 1 },
    { range: '80-100', count: 1 },
  ]);
});

test('límites de los tramos de score: 19/20, 39/40, 59/60, 79/80', () => {
  const store = createDecisionStore();
  [19, 20, 39, 40, 59, 60, 79, 80].forEach((score, i) => store.add(dec(`s${i}`, { score })));
  assert.deepEqual(store.summary().score_distribution.map((b) => b.count), [1, 2, 2, 2, 1]);
});

test('deduplicación: la última decisión recibida con el mismo request_id gana', () => {
  const store = createDecisionStore();
  store.add(dec('r1', { score: 50, decision: 'REJECTED' }));
  store.add(dec('r2'));
  store.add(dec('r1', { score: 90, decision: 'APPROVED' })); // reemisión
  assert.equal(store.size, 2);
  const [first, second] = store.recent(10);
  assert.equal(first.request_id, 'r1'); // la reemitida pasa a ser la más reciente
  assert.equal(first.score, 90);
  assert.equal(second.request_id, 'r2');
  const s = store.summary();
  assert.equal(s.total, 2);
  assert.equal(s.approved, 2);
});

test('límite de memoria: solo las últimas N decisiones únicas', () => {
  const store = createDecisionStore({ max: 3 });
  for (let i = 1; i <= 5; i += 1) store.add(dec(`r${i}`));
  assert.equal(store.size, 3);
  assert.deepEqual(store.recent(10).map((d) => d.request_id), ['r5', 'r4', 'r3']);
});

test('límite por defecto: 1000 decisiones únicas', () => {
  const store = createDecisionStore();
  for (let i = 0; i < 1200; i += 1) store.add(dec(`r${i}`));
  assert.equal(store.size, 1000);
  assert.equal(store.recent(1)[0].request_id, 'r1199');
});

test('recent(limit): orden de más reciente a más antigua y respeta el límite', () => {
  const store = createDecisionStore();
  for (let i = 1; i <= 5; i += 1) store.add(dec(`r${i}`));
  assert.deepEqual(store.recent(2).map((d) => d.request_id), ['r5', 'r4']);
});
