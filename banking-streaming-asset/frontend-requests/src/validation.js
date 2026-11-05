// Validación estricta del body de POST /api/loan-requests.
// Solo se aceptan los cuatro campos del cliente; request_id y requested_at los genera el servidor.

export const ALLOWED_FIELDS = Object.freeze(['customer_id', 'amount', 'term_months', 'purpose']);
export const ALLOWED_TERMS = Object.freeze([12, 24, 36, 48, 60, 120, 240]);
export const ALLOWED_PURPOSES = Object.freeze([
  'HOME',
  'CAR',
  'EDUCATION',
  'PERSONAL',
  'BUSINESS',
  'DEBT_CONSOLIDATION',
]);

const INT32_MAX = 2147483647;
// decimal(12,2): hasta 10 dígitos enteros y hasta 2 decimales; sin signo, sin exponente, sin ceros a la izquierda.
const AMOUNT_PATTERN = /^(0|[1-9]\d{0,9})(\.\d{1,2})?$/;

/**
 * Devuelve el importe normalizado a exactamente 2 decimales ("1000" -> "1000.00"),
 * o null si no es válido. Nunca redondea.
 */
export function normalizeAmount(raw) {
  let text;
  if (typeof raw === 'number' && Number.isFinite(raw)) {
    text = String(raw);
  } else if (typeof raw === 'string') {
    text = raw;
  } else {
    return null;
  }
  if (!AMOUNT_PATTERN.test(text)) return null;
  const [integerPart, fraction = ''] = text.split('.');
  const normalizedFraction = fraction.padEnd(2, '0');
  if (/^0+$/.test(integerPart + normalizedFraction)) return null; // importe <= 0
  return `${integerPart}.${normalizedFraction}`;
}

/**
 * @returns {{ok: true, value: {customer_id: number, amount: string, term_months: number, purpose: string}}
 *          | {ok: false, fields: string[]}}
 * `fields` es solo para uso interno/tests; nunca se envía al cliente.
 */
export function validateLoanRequestInput(body) {
  if (typeof body !== 'object' || body === null || Array.isArray(body)) {
    return { ok: false, fields: ['body'] };
  }

  const invalid = [];

  for (const key of Object.keys(body)) {
    if (!ALLOWED_FIELDS.includes(key)) invalid.push(key);
  }

  const { customer_id, amount, term_months, purpose } = body;

  if (!Number.isInteger(customer_id) || customer_id < 1 || customer_id > INT32_MAX) {
    invalid.push('customer_id');
  }

  const normalizedAmount = normalizeAmount(amount);
  if (normalizedAmount === null) invalid.push('amount');

  if (!Number.isInteger(term_months) || !ALLOWED_TERMS.includes(term_months)) {
    invalid.push('term_months');
  }

  if (typeof purpose !== 'string' || !ALLOWED_PURPOSES.includes(purpose)) {
    invalid.push('purpose');
  }

  if (invalid.length > 0) return { ok: false, fields: invalid };

  return {
    ok: true,
    value: { customer_id, amount: normalizedAmount, term_months, purpose },
  };
}
