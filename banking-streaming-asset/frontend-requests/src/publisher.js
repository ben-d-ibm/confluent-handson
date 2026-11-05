// Publisher de loan-requests: Avro (helpers de avro.js) + wire format de Confluent + Kafka.
// Sin dependencia directa de ninguna librería Kafka: recibe un `producer` con la forma
//   send({ messages: [{ topic, key, value }], acks }) => Promise<{ offsets?: [...] }>
// lo que permite probarlo offline con un doble.

import { encodeLoanRequest, toConfluentWireFormat } from './avro.js';

const ACKS_ALL = -1;
const DEFAULT_PUBLISH_TIMEOUT_MS = 15000;

export class PublishError extends Error {
  constructor(reason) {
    super(reason);
    this.name = 'PublishError';
  }
}

function withTimeout(promise, ms) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new PublishError('publish timed out')), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

/**
 * @param {{topic: string}} cfg
 * @param {{avroType: object, schemaIds: {resolve: () => Promise<number>}, producer: object,
 *          publishTimeoutMs?: number}} deps
 */
export function createLoanRequestPublisher(cfg, { avroType, schemaIds, producer, publishTimeoutMs = DEFAULT_PUBLISH_TIMEOUT_MS }) {
  return {
    /** Publica exactamente un mensaje. Resuelve solo si el broker confirma (acks=all). */
    async publish({ key, loanRequest }) {
      const publishOnce = async () => {
        const schemaId = await schemaIds.resolve();
        const value = toConfluentWireFormat(schemaId, encodeLoanRequest(avroType, loanRequest));
        const result = await producer.send({
          messages: [{ topic: cfg.topic, key: Buffer.from(key, 'utf8'), value }],
          acks: ACKS_ALL,
        });
        if (!result?.offsets?.length) throw new PublishError('publish not acknowledged');
        const { topic, partition, offset } = result.offsets[0];
        return { topic, partition, offset: String(offset) };
      };
      return withTimeout(publishOnce(), publishTimeoutMs);
    },
  };
}
