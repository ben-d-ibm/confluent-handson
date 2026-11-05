// Consumer Kafka de loan-decisions -> decoder -> store. Una sola instancia, un group.id fijo.
// mode EARLIEST: en cada arranque relee el tópico desde el principio para reconstruir el estado en memoria
// (el estado no se persiste). Sin autocommit: los offsets no se usan para nada, solo se necesita el grupo.

import { Consumer } from '@platformatic/kafka';

const CLIENT_ID = 'frontend-dashboard';
const KAFKA_TIMEOUT_MS = 10000;

export function createKafkaConsumer(config) {
  return new Consumer({
    clientId: CLIENT_ID,
    groupId: config.groupId,
    bootstrapBrokers: config.brokers,
    tls: {}, // TLS con verificación de certificados por defecto
    sasl: { mechanism: 'PLAIN', username: config.kafka.username, password: config.kafka.password },
    timeout: KAFKA_TIMEOUT_MS,
    connectTimeout: KAFKA_TIMEOUT_MS,
    requestTimeout: KAFKA_TIMEOUT_MS,
    // Key y value llegan como Buffer; el decoder los interpreta.
    deserializers: {
      key: (data) => data,
      value: (data) => data,
      headerKey: (data) => data,
      headerValue: (data) => data,
    },
  });
}

/**
 * @param {{topic: string}} config
 * @param {{consumer: object, decoder: {decode: Function}, store: {add: Function}, logger: Function}} deps
 */
export function createDecisionConsumer(config, { consumer, decoder, store, logger }) {
  let stream = null;
  let running = null;
  let received = 0;
  let skipped = 0;

  async function handle(message) {
    try {
      const decision = await decoder.decode(message.key, message.value);
      if (decision) {
        store.add(decision);
        received += 1;
      }
    } catch (err) {
      // Un mensaje ilegible no detiene el dashboard: se cuenta y se sigue.
      skipped += 1;
      logger({ event: 'decision_skipped', error: err?.name ?? 'Error', reason: err?.message });
    }
  }

  return {
    async start() {
      stream = await consumer.consume({
        topics: [config.topic],
        mode: 'earliest',
        autocommit: false,
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
      });
      logger({ event: 'consumer_started', topic: config.topic });

      running = (async () => {
        for await (const message of stream) {
          await handle(message);
        }
      })().catch((err) => {
        logger({ event: 'consumer_failed', error: err?.name ?? 'Error' });
      });
    },

    stats() {
      return { received, skipped };
    },

    async stop() {
      try {
        await stream?.close();
        await running;
      } finally {
        await consumer.close();
      }
    },
  };
}
