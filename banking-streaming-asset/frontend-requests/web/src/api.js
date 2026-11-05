// Cliente de la API. Ruta relativa: sin hosts, puertos ni URLs externas.
const ENDPOINT = '/api/loan-requests';

/** Tipos de fallo que la UI sabe mostrar. Nunca transportan detalles del backend. */
export class SubmitError extends Error {
  constructor(kind) {
    super(kind);
    this.name = 'SubmitError';
    this.kind = kind; // 'validation' | 'unavailable' | 'unexpected'
  }
}

/**
 * Envía la solicitud. Éxito = el backend confirmó la publicación (HTTP 202).
 * No es una decisión: solo se devuelven request_id y requested_at.
 */
export async function submitLoanRequest(payload, fetchImpl = fetch) {
  let response;
  try {
    response = await fetchImpl(ENDPOINT, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
    });
  } catch {
    throw new SubmitError('unexpected');
  }

  if (response.status === 400) throw new SubmitError('validation');
  if (response.status === 503) throw new SubmitError('unavailable');
  if (response.status !== 202) throw new SubmitError('unexpected');

  let body;
  try {
    body = await response.json();
  } catch {
    throw new SubmitError('unexpected');
  }
  if (typeof body?.request_id !== 'string' || typeof body?.requested_at !== 'string') {
    throw new SubmitError('unexpected');
  }
  return { requestId: body.request_id, requestedAt: body.requested_at };
}

export const ERROR_MESSAGES = Object.freeze({
  validation: 'Please check the loan request details.',
  unavailable: 'The request could not be submitted. Please try again.',
  unexpected: 'Something went wrong. Please try again later.',
});
