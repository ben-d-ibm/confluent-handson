import avsc from 'avsc';

// Schema real de loan-decisions-value (leído de Schema Registry en modo solo lectura). request_id NO está
// en el value: es la PK de Flink y viaja en la clave.
export const DECISIONS_SCHEMA = {
  type: 'record',
  name: 'loan_decisions_value',
  namespace: 'org.apache.flink.avro.generated.record',
  fields: [
    { name: 'customer_id', type: 'int' },
    { name: 'amount', type: { type: 'bytes', logicalType: 'decimal', precision: 12, scale: 2 } },
    { name: 'term_months', type: 'int' },
    { name: 'purpose', type: 'string' },
    { name: 'requested_at', type: { type: 'long', logicalType: 'timestamp-millis' } },
    { name: 'decided_at', type: { type: 'long', logicalType: 'timestamp-millis' } },
    { name: 'score', type: 'int' },
    { name: 'decision', type: 'string' },
    { name: 'reasons', type: 'string' },
    { name: 'dti_ratio', type: ['null', 'double'], default: null },
    { name: 'model', type: 'string' },
  ],
};

export const SECRETS = {
  kafkaKey: 'KAFKA-KEY-SENTINEL-111',
  kafkaSecret: 'KAFKA-SECRET-SENTINEL-222',
  srKey: 'SR-KEY-SENTINEL-333',
  srSecret: 'SR-SECRET-SENTINEL-444',
};

export const goodEnv = () => ({
  KAFKA_BOOTSTRAP_SERVERS: 'pkc-test.eu-west-1.aws.confluent.cloud:9092',
  SCHEMA_REGISTRY_URL: 'https://psrc-test.eu-west-1.aws.confluent.cloud',
  DASHBOARD_API_KAFKA_API_KEY: SECRETS.kafkaKey,
  DASHBOARD_API_KAFKA_API_SECRET: SECRETS.kafkaSecret,
  DASHBOARD_API_SR_API_KEY: SECRETS.srKey,
  DASHBOARD_API_SR_API_SECRET: SECRETS.srSecret,
  DASHBOARD_API_GROUP_PREFIX: 'banking-streaming-dashboard-api-',
  TOPIC_LOAN_DECISIONS: 'loan-decisions',
});

// Sin logicalTypes registrados avsc usa el tipo base: decimal -> Buffer, timestamp -> number (ms).
const rawType = avsc.Type.forSchema(DECISIONS_SCHEMA);

function unscaledBuffer(cents) {
  // Complemento a dos big-endian de longitud mínima (los importes de test son positivos).
  let hex = BigInt(cents).toString(16);
  if (hex.length % 2) hex = `0${hex}`;
  if (Number.parseInt(hex.slice(0, 2), 16) >= 0x80) hex = `00${hex}`;
  return Buffer.from(hex, "hex");
}

export function sampleRecord(over = {}) {
  return {
    customer_id: 1001,
    amount: unscaledBuffer(1000000), // 10000.00
    term_months: 24,
    purpose: 'PERSONAL',
    requested_at: Date.parse('2026-11-04T17:50:40.680Z'),
    decided_at: Date.parse('2026-11-04T17:55:55.000Z'),
    score: 100,
    decision: 'APPROVED',
    reasons: 'MEETS_CRITERIA',
    dti_ratio: 0.12,
    model: 'RULES_V1',
    ...over,
  };
}

/** Value con wire format de Confluent. */
export function encodeValue(record, schemaId = 100011) {
  const header = Buffer.alloc(5);
  header.writeInt32BE(schemaId, 1);
  return Buffer.concat([header, rawType.toBuffer(record)]);
}

function zigzagVarint(n) {
  let v = BigInt(n << 1) ^ BigInt(n >> 31);
  const out = [];
  do {
    let b = Number(v & 0x7fn);
    v >>= 7n;
    if (v !== 0n) b |= 0x80;
    out.push(b);
  } while (v !== 0n);
  return Buffer.from(out);
}

const avroString = (s) => Buffer.concat([zigzagVarint(Buffer.byteLength(s)), Buffer.from(s)]);
const framing = Buffer.concat([Buffer.from([0]), Buffer.from([0, 0, 0x27, 0x10])]);

/** Clave raw: string UTF-8. */
export const rawKey = (requestId) => Buffer.from(requestId);
/** Clave Avro (record { request_id: string }) con framing de Confluent. */
export const avroKey = (requestId) => Buffer.concat([framing, avroString(requestId)]);
/** Clave Avro con unión [null, string]. */
export const avroUnionKey = (requestId) =>
  Buffer.concat([framing, zigzagVarint(1), avroString(requestId)]);

/** fetch simulado de Schema Registry. */
export function fakeRegistry({ id = 100011, schema = DECISIONS_SCHEMA, status = 200 } = {}) {
  const calls = [];
  const fetchImpl = async (url, init) => {
    calls.push({ url: String(url), init });
    if (status !== 200) return { ok: false, status, json: async () => ({}) };
    return { ok: true, status: 200, json: async () => ({ id, schema: JSON.stringify(schema) }) };
  };
  return { fetchImpl, calls };
}

export const registryCfg = () => ({
  url: 'https://psrc-test.eu-west-1.aws.confluent.cloud',
  username: SECRETS.srKey,
  password: SECRETS.srSecret,
  subject: 'loan-decisions-value',
});
