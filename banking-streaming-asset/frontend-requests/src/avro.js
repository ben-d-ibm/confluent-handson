// Preparación y encoding Avro de loan-requests. Totalmente offline: no usa Kafka ni Schema Registry.
// Reutiliza el contrato único de Terraform (no se copia el esquema).

import { readFileSync } from 'node:fs';
import avsc from 'avsc';

const DEFAULT_SCHEMA_URL = new URL('../../terraform/schemas/loan-requests.avsc', import.meta.url);
const DECIMAL_TEXT = /^-?\d+\.\d{2}$/; // scale exactamente 2

export function loadLoanRequestSchema(path = process.env.LOAN_REQUESTS_SCHEMA_PATH) {
  return JSON.parse(readFileSync(path ?? DEFAULT_SCHEMA_URL, 'utf8'));
}

function unscaledToBuffer(n) {
  // Big-endian en complemento a dos, longitud mínima (convención del decimal de Avro).
  const magnitudeBits = (n < 0n ? -n - 1n : n).toString(2).length;
  const bytes = Math.ceil((magnitudeBits + 1) / 8);
  const hex = BigInt.asUintN(bytes * 8, n).toString(16).padStart(bytes * 2, '0');
  return Buffer.from(hex, 'hex');
}

function bufferToUnscaled(buf) {
  if (buf.length === 0) return 0n;
  return BigInt.asIntN(buf.length * 8, BigInt(`0x${buf.toString('hex')}`));
}

/** decimal(precision, scale) sobre bytes. Valor de aplicación: string "1234.50". */
class DecimalType extends avsc.types.LogicalType {
  constructor(schema, opts) {
    super(schema, opts);
    this._precision = schema.precision;
    this._scale = schema.scale;
  }

  _toValue(text) {
    // undefined => avsc lo trata como valor inválido (nunca se redondea ni se ajusta la escala).
    if (typeof text !== 'string' || this._scale !== 2 || !DECIMAL_TEXT.test(text)) return undefined;
    const digits = text.replace('-', '').replace('.', '').replace(/^0+(?=\d)/, '');
    if (digits.length > this._precision) return undefined;
    return unscaledToBuffer(BigInt(text.replace('.', '')));
  }

  _fromValue(buf) {
    const unscaled = bufferToUnscaled(buf);
    const negative = unscaled < 0n;
    const digits = (negative ? -unscaled : unscaled).toString().padStart(this._scale + 1, '0');
    const cut = digits.length - this._scale;
    return `${negative ? '-' : ''}${digits.slice(0, cut)}.${digits.slice(cut)}`;
  }

  _resolve(type) {
    if (avsc.Type.isType(type, 'bytes', 'logical:decimal')) return (v) => v;
    return undefined;
  }
}

/** timestamp-millis sobre long. Valor de aplicación: Date. */
class TimestampMillisType extends avsc.types.LogicalType {
  _toValue(date) {
    if (!(date instanceof Date)) return undefined;
    const ms = date.getTime();
    return Number.isSafeInteger(ms) ? ms : undefined;
  }

  _fromValue(ms) {
    return new Date(ms);
  }

  _resolve(type) {
    if (avsc.Type.isType(type, 'long', 'logical:timestamp-millis')) return (v) => v;
    return undefined;
  }
}

export function createLoanRequestType(schema = loadLoanRequestSchema()) {
  return avsc.Type.forSchema(schema, {
    logicalTypes: { decimal: DecimalType, 'timestamp-millis': TimestampMillisType },
  });
}

/**
 * Convierte la solicitud interna al registro del contrato Avro.
 * Los nombres y tipos coinciden 1:1 con loan-requests.avsc.
 */
export function toAvroRecord(loanRequest) {
  return {
    request_id: loanRequest.request_id,
    customer_id: loanRequest.customer_id,
    amount: loanRequest.amount, // string con scale 2 -> decimal(12,2)
    term_months: loanRequest.term_months,
    purpose: loanRequest.purpose,
    requested_at: loanRequest.requested_at, // Date -> timestamp-millis
  };
}

/** Serializa el valor en Avro binario (sin cabecera de Schema Registry). Lanza si no cumple el contrato. */
export function encodeLoanRequest(type, loanRequest) {
  return type.toBuffer(toAvroRecord(loanRequest));
}

/** Wire format de Confluent: magic byte 0x00 + schema id (int32 big-endian) + payload Avro. */
export function toConfluentWireFormat(schemaId, avroPayload) {
  if (!Number.isInteger(schemaId) || schemaId < 0 || schemaId > 0x7fffffff) {
    throw new TypeError('invalid schema id');
  }
  const header = Buffer.alloc(5);
  header.writeUInt8(0, 0);
  header.writeInt32BE(schemaId, 1);
  return Buffer.concat([header, avroPayload]);
}
