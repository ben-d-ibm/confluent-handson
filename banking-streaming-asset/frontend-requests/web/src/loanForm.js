// Valores aceptados por la API. Etiquetas amigables en la UI; se envían los valores exactos.
// El backend sigue siendo la autoridad de validación.

export const TERM_OPTIONS = [12, 24, 36, 48, 60, 120, 240].map((months) => ({
  value: String(months),
  label: `${months} months`,
}));

export const PURPOSE_OPTIONS = [
  { value: 'HOME', label: 'Home' },
  { value: 'CAR', label: 'Car' },
  { value: 'EDUCATION', label: 'Education' },
  { value: 'PERSONAL', label: 'Personal' },
  { value: 'BUSINESS', label: 'Business' },
  { value: 'DEBT_CONSOLIDATION', label: 'Debt consolidation' },
];

export const DEFAULT_VALUES = Object.freeze({
  customerId: '1001',
  amount: '10000.00',
  term: '24',
  purpose: 'PERSONAL',
});

const CUSTOMER_ID_MAX = 2147483647;
const AMOUNT_PATTERN = /^(0|[1-9]\d{0,9})(\.\d{1,2})?$/;

/** Validación solo para UX. Devuelve mensajes de campo (vacío si todo es plausible). */
export function validateForm(values) {
  const errors = {};

  const id = values.customerId.trim();
  if (!/^\d+$/.test(id) || Number(id) < 1 || Number(id) > CUSTOMER_ID_MAX) {
    errors.customerId = 'Enter a whole number greater than 0.';
  }

  const amount = values.amount.trim();
  if (!AMOUNT_PATTERN.test(amount) || Number(amount) <= 0) {
    errors.amount = 'Enter an amount greater than 0 with up to 2 decimals.';
  }

  return errors;
}

/** Payload exacto de POST /api/loan-requests (request_id y requested_at los genera el servidor). */
export function toPayload(values) {
  return {
    customer_id: Number(values.customerId.trim()),
    amount: values.amount.trim(),
    term_months: Number(values.term),
    purpose: values.purpose,
  };
}
