import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ConfigError, GROUP_SUFFIX, loadConfig, REQUIRED_ENV } from '../src/config.js';
import { goodEnv, SECRETS } from './helpers.js';

test('loadConfig: configuración válida usa DASHBOARD_API_* y el subject loan-decisions-value', () => {
  const cfg = loadConfig(goodEnv());
  assert.equal(cfg.topic, 'loan-decisions');
  assert.equal(cfg.schemaRegistry.subject, 'loan-decisions-value');
  assert.equal(cfg.groupId, `banking-streaming-dashboard-api-${GROUP_SUFFIX}`);
  assert.deepEqual(cfg.brokers, ['pkc-test.eu-west-1.aws.confluent.cloud:9092']);
  assert.equal(cfg.kafka.username, SECRETS.kafkaKey);
  assert.equal(cfg.schemaRegistry.username, SECRETS.srKey);
});

test('loadConfig: falta cada variable requerida -> ConfigError con el nombre, sin valores', () => {
  for (const name of REQUIRED_ENV) {
    const env = goodEnv();
    delete env[name];
    assert.throws(
      () => loadConfig(env),
      (err) => {
        assert.ok(err instanceof ConfigError);
        assert.match(err.message, new RegExp(name));
        for (const s of Object.values(SECRETS)) assert.ok(!err.message.includes(s));
        return true;
      },
    );
  }
});

test('loadConfig: valores inválidos se rechazan sin revelar el valor', () => {
  const cases = [
    ['KAFKA_BOOTSTRAP_SERVERS', 'sin-puerto'],
    ['SCHEMA_REGISTRY_URL', 'http://psrc-test.example.com'],
    ['SCHEMA_REGISTRY_URL', 'https://user:pass@psrc-test.example.com'],
    ['TOPIC_LOAN_DECISIONS', 'topic con espacios'],
    ['DASHBOARD_API_GROUP_PREFIX', 'prefijo inválido!'],
  ];
  for (const [name, value] of cases) {
    const env = { ...goodEnv(), [name]: value };
    assert.throws(
      () => loadConfig(env),
      (err) => {
        assert.ok(err instanceof ConfigError);
        assert.ok(!err.message.includes(value));
        return true;
      },
    );
  }
});

test('loadConfig: no usa credenciales de otras aplicaciones', () => {
  const env = { ...goodEnv(), REQUESTS_API_KAFKA_API_KEY: 'otra-app' };
  assert.notEqual(loadConfig(env).kafka.username, 'otra-app');
});
