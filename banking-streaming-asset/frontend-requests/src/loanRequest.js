import { randomUUID } from 'node:crypto';

/**
 * Construye la solicitud interna a partir de la entrada ya validada.
 * request_id y requested_at los genera siempre el servidor.
 *
 * @param {{customer_id: number, amount: string, term_months: number, purpose: string}} validated
 * @param {{uuid?: () => string, now?: () => Date}} [deps] inyectables para tests
 */
export function buildLoanRequest(validated, { uuid = randomUUID, now = () => new Date() } = {}) {
  return {
    request_id: uuid(),
    customer_id: validated.customer_id,
    amount: validated.amount,
    term_months: validated.term_months,
    purpose: validated.purpose,
    requested_at: now(),
  };
}

/** Clave del registro Kafka: String(customer_id). Flink la lee como `raw`. */
export function loanRequestKey(loanRequest) {
  return String(loanRequest.customer_id);
}
