// Configuración runtime. Solo estas variables; ningún valor por defecto para endpoints ni credenciales.
// Los errores mencionan nombres de variables, nunca sus valores.

export const REQUIRED_ENV = Object.freeze([
  'KAFKA_BOOTSTRAP_SERVERS',
  'SCHEMA_REGISTRY_URL',
  'DASHBOARD_API_KAFKA_API_KEY',
  'DASHBOARD_API_KAFKA_API_SECRET',
  'DASHBOARD_API_SR_API_KEY',
  'DASHBOARD_API_SR_API_SECRET',
  'DASHBOARD_API_GROUP_PREFIX',
  'TOPIC_LOAN_DECISIONS',
]);

// Una sola instancia del dashboard: un group.id fijo con el prefijo que autoriza la ACL.
export const GROUP_SUFFIX = 'dashboard';
export const MAX_DECISIONS = 1000;

const BROKER_PATTERN = /^[A-Za-z0-9.-]+:\d{1,5}$/;
const TOPIC_PATTERN = /^[A-Za-z0-9._-]{1,249}$/;
const GROUP_PREFIX_PATTERN = /^[A-Za-z0-9._-]{1,200}$/;

export class ConfigError extends Error {
  constructor(problems) {
    super(`Invalid configuration: ${problems.join('; ')}`);
    this.name = 'ConfigError';
    this.problems = problems;
  }
}

export function loadConfig(env = process.env) {
  const problems = [];

  for (const name of REQUIRED_ENV) {
    if (typeof env[name] !== 'string' || env[name].trim() === '') problems.push(`${name} is required`);
  }
  if (problems.length > 0) throw new ConfigError(problems);

  const brokers = env.KAFKA_BOOTSTRAP_SERVERS.split(',').map((b) => b.trim());
  if (!brokers.every((b) => BROKER_PATTERN.test(b))) {
    problems.push('KAFKA_BOOTSTRAP_SERVERS must be a comma-separated list of host:port');
  }

  let registryUrl;
  try {
    const u = new URL(env.SCHEMA_REGISTRY_URL);
    if (u.protocol !== 'https:' || u.username || u.password) throw new Error('bad');
    registryUrl = u.origin + u.pathname.replace(/\/+$/, '');
  } catch {
    problems.push('SCHEMA_REGISTRY_URL must be an https URL without embedded credentials');
  }

  if (!TOPIC_PATTERN.test(env.TOPIC_LOAN_DECISIONS)) {
    problems.push('TOPIC_LOAN_DECISIONS is not a valid topic name');
  }
  if (!GROUP_PREFIX_PATTERN.test(env.DASHBOARD_API_GROUP_PREFIX)) {
    problems.push('DASHBOARD_API_GROUP_PREFIX is not a valid consumer group prefix');
  }

  if (problems.length > 0) throw new ConfigError(problems);

  return Object.freeze({
    brokers,
    topic: env.TOPIC_LOAN_DECISIONS,
    groupId: `${env.DASHBOARD_API_GROUP_PREFIX}${GROUP_SUFFIX}`,
    kafka: Object.freeze({
      username: env.DASHBOARD_API_KAFKA_API_KEY,
      password: env.DASHBOARD_API_KAFKA_API_SECRET,
    }),
    schemaRegistry: Object.freeze({
      url: registryUrl,
      username: env.DASHBOARD_API_SR_API_KEY,
      password: env.DASHBOARD_API_SR_API_SECRET,
      subject: `${env.TOPIC_LOAN_DECISIONS}-value`,
    }),
  });
}
