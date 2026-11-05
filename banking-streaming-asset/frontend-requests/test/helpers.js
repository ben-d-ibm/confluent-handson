// Valores centinela para comprobar que ningún secreto se filtra a respuestas, errores ni logs.
export const SECRETS = {
  kafkaKey: 'KAFKA-KEY-SENTINEL-111',
  kafkaSecret: 'KAFKA-SECRET-SENTINEL-222',
  srKey: 'SR-KEY-SENTINEL-333',
  srSecret: 'SR-SECRET-SENTINEL-444',
};

export const goodEnv = () => ({
  KAFKA_BOOTSTRAP_SERVERS: 'pkc-test.eu-west-1.aws.confluent.cloud:9092',
  SCHEMA_REGISTRY_URL: 'https://psrc-test.eu-west-1.aws.confluent.cloud',
  REQUESTS_API_KAFKA_API_KEY: SECRETS.kafkaKey,
  REQUESTS_API_KAFKA_API_SECRET: SECRETS.kafkaSecret,
  REQUESTS_API_SR_API_KEY: SECRETS.srKey,
  REQUESTS_API_SR_API_SECRET: SECRETS.srSecret,
  TOPIC_LOAN_REQUESTS: 'loan-requests',
});
