// Resolución del schema ID existente en Schema Registry (solo lectura).
// Equivalente a auto.register.schemas=false + use.latest.version=true: nunca se registra un esquema;
// únicamente GET /subjects/<subject>/versions/latest. Además se comprueba que el esquema registrado
// coincide (forma canónica de Avro) con terraform/schemas/loan-requests.avsc.

import avsc from 'avsc';

const DEFAULT_TIMEOUT_MS = 5000;

export class SchemaRegistryError extends Error {
  constructor(reason) {
    super(reason); // mensaje fijo; nunca incluye URL, cabeceras ni cuerpo de la respuesta
    this.name = 'SchemaRegistryError';
  }
}

function fingerprint(schema) {
  return avsc.Type.forSchema(schema).fingerprint('sha256').toString('hex');
}

/**
 * @param {{url: string, username: string, password: string, subject: string}} cfg
 * @param {object} localSchema contenido parseado de loan-requests.avsc
 * @param {{fetchImpl?: typeof fetch, timeoutMs?: number}} [deps]
 */
export function createSchemaIdResolver(cfg, localSchema, { fetchImpl = fetch, timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  const expected = fingerprint(localSchema);
  const authorization = `Basic ${Buffer.from(`${cfg.username}:${cfg.password}`).toString('base64')}`;
  const endpoint = `${cfg.url}/subjects/${encodeURIComponent(cfg.subject)}/versions/latest`;
  let cachedId = null;

  return {
    async resolve() {
      if (cachedId !== null) return cachedId;

      let response;
      try {
        response = await fetchImpl(endpoint, {
          method: 'GET',
          headers: { authorization, accept: 'application/vnd.schemaregistry.v1+json' },
          redirect: 'error',
          signal: AbortSignal.timeout(timeoutMs),
        });
      } catch {
        throw new SchemaRegistryError('schema registry unreachable');
      }
      if (!response.ok) throw new SchemaRegistryError('schema registry rejected the request');

      let body;
      try {
        body = await response.json();
      } catch {
        throw new SchemaRegistryError('schema registry returned an invalid response');
      }

      const id = body?.id;
      if (!Number.isInteger(id) || id < 0 || id > 0x7fffffff || typeof body.schema !== 'string') {
        throw new SchemaRegistryError('schema registry returned an invalid response');
      }
      let registered;
      try {
        registered = fingerprint(JSON.parse(body.schema));
      } catch {
        throw new SchemaRegistryError('schema registry returned an invalid response');
      }
      if (registered !== expected) {
        throw new SchemaRegistryError('registered schema does not match the local contract');
      }

      cachedId = id; // el ID de una versión registrada es inmutable
      return cachedId;
    },
  };
}
