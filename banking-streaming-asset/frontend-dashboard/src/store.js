// Estado en memoria del dashboard. Sin base de datos ni persistencia: si el proceso se reinicia,
// se reconstruye releyendo loan-decisions desde earliest.
// Deduplicación por request_id: la última decisión recibida gana (hay reemisiones al reemplazar el INSERT
// de Flink). Se conservan como máximo `max` decisiones únicas; al superarse se descarta la más antigua.

import { MAX_DECISIONS } from './config.js';

const SCORE_BUCKETS = Object.freeze(['0-19', '20-39', '40-59', '60-79', '80-100']);

function bucketOf(score) {
  if (score >= 80) return '80-100';
  if (score >= 60) return '60-79';
  if (score >= 40) return '40-59';
  if (score >= 20) return '20-39';
  return '0-19';
}

/** "1234.50" -> 123450n (centésimas). amount siempre llega como decimal con escala 2. */
function toCents(amount) {
  return BigInt(amount.replace('.', ''));
}

function fromCents(cents) {
  const digits = cents.toString().padStart(3, '0');
  return `${digits.slice(0, -2)}.${digits.slice(-2)}`;
}

export function createDecisionStore({ max = MAX_DECISIONS } = {}) {
  // Map conserva el orden de inserción: re-insertar tras delete deja la decisión como la más reciente.
  const byRequestId = new Map();

  return {
    add(decision) {
      byRequestId.delete(decision.request_id);
      byRequestId.set(decision.request_id, decision);
      while (byRequestId.size > max) {
        byRequestId.delete(byRequestId.keys().next().value);
      }
    },

    get size() {
      return byRequestId.size;
    },

    /** Últimas `limit` decisiones, de la más reciente a la más antigua (orden de llegada). */
    recent(limit) {
      const all = [...byRequestId.values()];
      return all.slice(-limit).reverse();
    },

    summary() {
      let approved = 0;
      let rejected = 0;
      let scoreSum = 0;
      let approvedCents = 0n;
      const buckets = Object.fromEntries(SCORE_BUCKETS.map((b) => [b, 0]));

      for (const d of byRequestId.values()) {
        if (d.decision === 'APPROVED') {
          approved += 1;
          approvedCents += toCents(d.amount);
        } else {
          rejected += 1;
        }
        scoreSum += d.score;
        buckets[bucketOf(d.score)] += 1;
      }

      const total = approved + rejected;
      return {
        total,
        approved,
        rejected,
        approval_rate: total === 0 ? null : approved / total,
        average_score: total === 0 ? null : Math.round((scoreSum / total) * 10) / 10,
        total_approved_amount: fromCents(approvedCents),
        score_distribution: SCORE_BUCKETS.map((range) => ({ range, count: buckets[range] })),
      };
    },
  };
}
