// Decodificación de loan-decisions (solo lectura de Schema Registry).
//  - value: wire format de Confluent (0x00 + schema id int32 BE + Avro). El esquema se obtiene por ID
//    desde Schema Registry (GET) y se cachea; nunca se registra ni se modifica nada.
//  - key: el value NO incluye request_id (Flink lo deja solo en la clave/PK), así que se lee de la clave.
//    No se necesita el schema de la clave: se extrae el string tanto si es raw como si es Avro con framing.

import avsc from 'avsc';

const DEFAULT_TIMEOUT_MS = 5000;

export class DecodeError extends Error {
  constructor(reason) {
    super(reason); // mensaje fijo; nunca incluye payloads, URL ni cabeceras
    this.name = 'DecodeError';
  }
}

function bufferToUnscaled(buf) {
  if (buf.length === 0) return 0n;
  return BigInt.asIntN(buf.length * 8, BigInt(`0x${buf.toString('hex')}`));
}

/** decimal(p, 2) sobre bytes -> string "1234.50". */
class DecimalType extends avsc.types.LogicalType {
  constructor(schema, opts) {
    super(schema, opts);
    this._scale = schema.scale;
  }

  _fromValue(buf) {
    const unscaled = bufferToUnscaled(buf);
    const negative = unscaled < 0n;
    const digits = (negative ? -unscaled : unscaled).toString().padStart(this._scale + 1, '0');
    const cut = digits.length - this._scale;
    return `${negative ? '-' : ''}${digits.slice(0, cut)}.${digits.slice(cut)}`;
  }

  _toValue() {
    return undefined; // solo lectura
  }

  _resolve(type) {
    return avsc.Type.isType(type, 'bytes', 'logical:decimal') ? (v) => v : undefined;
  }
}

/** timestamp-millis sobre long -> Date. */
class TimestampMillisType extends avsc.types.LogicalType {
  _fromValue(ms) {
    return new Date(ms);
  }

  _toValue() {
    return undefined; // solo lectura
  }

  _resolve(type) {
    return avsc.Type.isType(type, 'long', 'logical:timestamp-millis') ? (v) => v : undefined;
  }
}

const LOGICAL_TYPES = { decimal: DecimalType, 'timestamp-millis': TimestampMillisType };

function readVarint(buf, offset) {
  let result = 0n;
  let shift = 0n;
  let pos = offset;
  while (pos < buf.length && shift < 35n) {
    const b = buf[pos++];
    result |= BigInt(b & 0x7f) << shift;
    if ((b & 0x80) === 0) {
      const value = Number((result >> 1n) ^ -(result & 1n)); // zigzag
      return { value, next: pos };
    }
    shift += 7n;
  }
  return null;
}

function stringAt(buf, offset) {
  const len = readVarint(buf, offset);
  if (!len || len.value < 0 || len.next + len.value !== buf.length) return null;
  return buf.toString('utf8', len.next, buf.length);
}

/**
 * Extrae request_id de la clave Kafka. Acepta:
 *  - string UTF-8 sin framing (key.format raw);
 *  - Avro con framing de Confluent cuyo único campo es un string (opcionalmente unión [null, string]).
 * Devuelve null si no se puede interpretar.
 */
export function parseKey(key) {
  if (!Buffer.isBuffer(key) || key.length === 0) return null;

  if (key[0] === 0x00 && key.length > 5) {
    const direct = stringAt(key, 5); // record { request_id: string }
    if (direct) return direct;
    const branch = readVarint(key, 5); // record { request_id: union [null, string] }
    if (branch && branch.value === 1) return stringAt(key, branch.next);
    return null;
  }

  const text = key.toString('utf8');
  return /^[\x20-\x7e]+$/.test(text) ? text : null;
}

/**
 * @param {{url: string, username: string, password: string, subject: string}} cfg
 * @param {{fetchImpl?: typeof fetch, timeoutMs?: number}} [deps]
 */
export function createDecisionDecoder(cfg, { fetchImpl = fetch, timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  const authorization = `Basic ${Buffer.from(`${cfg.username}:${cfg.password}`).toString('base64')}`;
  const types = new Map(); // schema id -> avsc Type (el ID de un esquema registrado es inmutable)

  async function typeFor(id) {
    const cached = types.get(id);
    if (cached) return cached;

    const endpoint = `${cfg.url}/schemas/ids/${id}?subject=${encodeURIComponent(cfg.subject)}`;
    let response;
    try {
      response = await fetchImpl(endpoint, {
        method: 'GET',
        headers: { authorization, accept: 'application/vnd.schemaregistry.v1+json' },
        redirect: 'error',
        signal: AbortSignal.timeout(timeoutMs),
      });
    } catch {
      throw new DecodeError('schema registry unreachable');
    }
    if (!response.ok) throw new DecodeError('schema registry rejected the request');

    let type;
    try {
      const body = await response.json();
      type = avsc.Type.forSchema(JSON.parse(body.schema), { logicalTypes: LOGICAL_TYPES });
    } catch {
      throw new DecodeError('schema registry returned an invalid schema');
    }
    types.set(id, type);
    return type;
  }

  return {
    /**
     * Calienta la caché con la última versión del subject (falla rápido ante credenciales o permisos
     * incorrectos). Devuelve el ID y el subject.
     */
    async connect() {
      const endpoint = `${cfg.url}/subjects/${encodeURIComponent(cfg.subject)}/versions/latest`;
      let response;
      try {
        response = await fetchImpl(endpoint, {
          method: 'GET',
          headers: { authorization, accept: 'application/vnd.schemaregistry.v1+json' },
          redirect: 'error',
          signal: AbortSignal.timeout(timeoutMs),
        });
      } catch {
        throw new DecodeError('schema registry unreachable');
      }
      if (!response.ok) throw new DecodeError('schema registry rejected the request');
      let id;
      try {
        const body = await response.json();
        if (!Number.isInteger(body.id)) throw new Error('bad');
        id = body.id;
        types.set(id, avsc.Type.forSchema(JSON.parse(body.schema), { logicalTypes: LOGICAL_TYPES }));
      } catch {
        throw new DecodeError('schema registry returned an invalid schema');
      }
      return { subject: cfg.subject, schemaId: id };
    },

    /**
     * @param {Buffer|null} key
     * @param {Buffer|null} value
     * @returns {Promise<object|null>} decisión normalizada, o null si es un tombstone (value nulo)
     */
    async decode(key, value) {
      if (value == null) return null;
      if (!Buffer.isBuffer(value) || value.length < 6 || value[0] !== 0x00) {
        throw new DecodeError('unexpected value framing');
      }
      const requestId = parseKey(key);
      if (!requestId) throw new DecodeError('unreadable key');

      const type = await typeFor(value.readInt32BE(1));
      let record;
      try {
        record = type.fromBuffer(value.subarray(5));
      } catch {
        throw new DecodeError('value does not match its schema');
      }

      const { decision } = record;
      if (decision !== 'APPROVED' && decision !== 'REJECTED') throw new DecodeError('unexpected decision value');

      return {
        request_id: requestId,
        customer_id: record.customer_id,
        amount: record.amount,
        term_months: record.term_months,
        purpose: record.purpose,
        requested_at: record.requested_at.toISOString(),
        decided_at: record.decided_at.toISOString(),
        score: record.score,
        decision,
        reasons: record.reasons,
        model: record.model,
      };
    },
  };
}
