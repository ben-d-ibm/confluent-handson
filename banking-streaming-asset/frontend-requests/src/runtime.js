// Cableado runtime: Kafka (SASL_SSL) + Schema Registry + publisher.
//
// Elección de librerías (comprobada en el registro npm):
//  - Kafka: @platformatic/kafka 2.14.0 (Apache-2.0, release 2026-10-07, Node >=22.22 / >=24.6,
//    cliente puro JS con SASL PLAIN + TLS, retries/timeouts configurables, producer reutilizable).
//    Descartado: kafkajs (último release 2023-02, sin mantenimiento); @confluentinc/kafka-javascript
//    (mantenido, pero binding nativo librdkafka con node-pre-gyp: peor encaje en imagen UBI mínima).
//  - Schema Registry: llamada REST de solo lectura propia (src/schemaRegistry.js). Evita
//    @confluentinc/schemaregistry (arrastra SDKs de KMS de AWS/Azure/GCP que no se necesitan) y
//    reutiliza avsc, ya presente. No hay protocolo Kafka manual: solo HTTP para obtener el schema ID.
//  - Reintentos: retries=0 en el cliente y un timeout global por publicación. Un reintento interno
//    podría duplicar el mensaje sin idempotencia (no se habilita: exigiría una ACL IDEMPOTENT_WRITE
//    que Terraform no concede). Si falla, la API devuelve 503 y el cliente decide reenviar.

import { Producer } from '@platformatic/kafka';
import { loadConfig } from './config.js';
import { createLoanRequestType, loadLoanRequestSchema } from './avro.js';
import { createSchemaIdResolver } from './schemaRegistry.js';
import { createLoanRequestPublisher } from './publisher.js';
const CLIENT_ID = 'frontend-requests';
const KAFKA_TIMEOUT_MS = 10000;

export function createKafkaProducer(config) {
  return new Producer({
    clientId: CLIENT_ID,
    bootstrapBrokers: config.brokers,
    tls: {}, // TLS con verificación de certificados por defecto
    sasl: { mechanism: 'PLAIN', username: config.kafka.username, password: config.kafka.password },
    timeout: KAFKA_TIMEOUT_MS,
    connectTimeout: KAFKA_TIMEOUT_MS,
    requestTimeout: KAFKA_TIMEOUT_MS,
    retries: 0,
    autocreateTopics: false,
  });
}

/**
 * Crea el runtime a partir del entorno. Lanza ConfigError si falta o es inválida alguna variable.
 * El producer es único y reutilizable; se conecta de forma perezosa en el primer uso o con connect().
 */
export function createRuntime(env = process.env, { producer, fetchImpl } = {}) {
  const config = loadConfig(env);
  const schema = loadLoanRequestSchema();
  const kafkaProducer = producer ?? createKafkaProducer(config);
  const schemaIds = createSchemaIdResolver(config.schemaRegistry, schema, { fetchImpl });
  const publisher = createLoanRequestPublisher(config, {
    avroType: createLoanRequestType(schema),
    schemaIds,
    producer: kafkaProducer,
  });

  return {
    publisher,
    /** Calienta la conexión Kafka y comprueba el schema ID; falla rápido ante mala configuración. */
    async connect() {
      await kafkaProducer.metadata({ topics: [config.topic] });
      const schemaId = await schemaIds.resolve();
      return { topic: config.topic, subject: config.schemaRegistry.subject, schemaId };
    },
    async disconnect() {
      await kafkaProducer.close();
    },
  };
}
