import { existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { loadConfig } from './config.js';
import { createDecisionDecoder } from './decoder.js';
import { createDecisionStore } from './store.js';
import { createDecisionConsumer, createKafkaConsumer } from './consumer.js';

export const HOST = '127.0.0.1';
const ALLOWED_HOSTS = new Set([HOST, '0.0.0.0']);
const DEFAULT_PORT = 3001;
const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 200;

export const DEFAULT_STATIC_DIR = fileURLToPath(new URL('../web/dist', import.meta.url));
const HAS_FILE_EXTENSION = /\.[A-Za-z0-9]+$/;

function defaultLogger(entry) {
  // JSON por línea en stdout; los eventos nunca contienen credenciales.
  console.log(JSON.stringify({ ts: new Date().toISOString(), ...entry }));
}

/** limit: entero 1..MAX_LIMIT; ausente => DEFAULT_LIMIT. Cualquier otra cosa => null (400). */
export function parseLimit(raw) {
  if (raw === undefined) return DEFAULT_LIMIT;
  if (typeof raw !== 'string' || !/^\d{1,4}$/.test(raw)) return null;
  const n = Number(raw);
  return n >= 1 && n <= MAX_LIMIT ? n : null;
}

/** API de solo lectura sobre el estado en memoria. No calcula score ni decide nada. */
export function createApp({ store, logger = defaultLogger, staticDir = null }) {
  const app = express();
  app.disable('x-powered-by');

  app.get('/health', (_req, res) => {
    res.status(200).json({ status: 'ok' });
  });

  app.get('/api/summary', (_req, res) => {
    res.set('Cache-Control', 'no-store');
    res.status(200).json(store.summary());
  });

  app.get('/api/decisions', (req, res) => {
    const limit = parseLimit(req.query.limit);
    if (limit === null) return res.status(400).json({ error: 'Invalid request' });
    res.set('Cache-Control', 'no-store');
    return res.status(200).json({ decisions: store.recent(limit) });
  });

  app.all(['/api', '/api/*splat'], (_req, res) => {
    res.status(404).json({ error: 'Not found' });
  });

  if (staticDir) {
    const root = path.resolve(staticDir);
    const indexFile = path.join(root, 'index.html');
    const sendIndex = (res, next) =>
      res.sendFile(indexFile, { headers: { 'Cache-Control': 'no-cache' } }, (err) => err && next(err));

    app.use((_req, res, next) => {
      res.set('X-Content-Type-Options', 'nosniff');
      next();
    });
    app.use(
      express.static(root, {
        index: false,
        dotfiles: 'ignore',
        redirect: false,
        setHeaders: (res, filePath) => {
          res.set(
            'Cache-Control',
            filePath.startsWith(path.join(root, 'assets') + path.sep)
              ? 'public, max-age=31536000, immutable'
              : 'no-cache',
          );
        },
      }),
    );
    app.get('/', (_req, res, next) => sendIndex(res, next));
    app.get('/*splat', (req, res, next) => {
      if (HAS_FILE_EXTENSION.test(req.path)) return next();
      return sendIndex(res, next);
    });
  }

  app.use((_req, res) => {
    res.status(404).json({ error: 'Not found' });
  });

  // eslint-disable-next-line no-unused-vars
  app.use((err, _req, res, _next) => {
    logger({ event: 'unhandled_error', error: err?.name ?? 'Error' });
    res.status(500).json({ error: 'Internal error' });
  });

  return app;
}

export function resolveHost(raw) {
  if (raw === undefined) return HOST;
  if (!ALLOWED_HOSTS.has(raw)) throw new Error('HOST must be 127.0.0.1 or 0.0.0.0');
  return raw;
}

export function resolvePort(raw) {
  if (raw === undefined) return DEFAULT_PORT;
  const port = Number(raw);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error('PORT must be an integer between 1 and 65535');
  }
  return port;
}

export function resolveStaticDir(logger = defaultLogger, dir = DEFAULT_STATIC_DIR) {
  if (existsSync(path.join(dir, 'index.html'))) return dir;
  logger({ event: 'static_dir_missing' });
  return null;
}

const SHUTDOWN_TIMEOUT_MS = 10000;
const STARTUP_TIMEOUT_MS = 30000;

/**
 * Arranque: valida la configuración, comprueba Schema Registry (loan-decisions-value), arranca el consumer
 * y escucha. Todas las dependencias son inyectables para probar el ciclo de vida offline.
 */
export async function run({
  env = process.env,
  port = resolvePort(env.PORT),
  host = resolveHost(env.HOST),
  proc = process,
  logger = defaultLogger,
  decoderFactory = createDecisionDecoder,
  consumerFactory = createKafkaConsumer,
} = {}) {
  const config = loadConfig(env); // ConfigError con nombres de variables, nunca valores
  const store = createDecisionStore();
  const decoder = decoderFactory(config.schemaRegistry);

  let startupTimer;
  const info = await Promise.race([
    decoder.connect(),
    new Promise((_, reject) => {
      startupTimer = setTimeout(() => reject(new Error('startup timed out')), STARTUP_TIMEOUT_MS);
    }),
  ]).finally(() => clearTimeout(startupTimer));
  logger({ event: 'schema_resolved', subject: info.subject, schema_id: info.schemaId });

  const consumer = createDecisionConsumer(config, {
    consumer: consumerFactory(config),
    decoder,
    store,
    logger,
  });
  await consumer.start();

  const server = createApp({ store, logger, staticDir: resolveStaticDir(logger) }).listen(port, host);
  await new Promise((resolve, reject) => {
    server.once('listening', resolve);
    server.once('error', reject);
  });
  logger({ event: 'listening', host, port });

  let closing = null;
  const shutdown = (signal) => {
    closing ??= (async () => {
      logger({ event: 'shutdown', signal });
      const timer = setTimeout(() => proc.exit(1), SHUTDOWN_TIMEOUT_MS);
      timer.unref();
      try {
        await new Promise((resolve) => {
          server.close(() => resolve());
          server.closeIdleConnections();
        });
        await consumer.stop();
      } catch (err) {
        logger({ event: 'shutdown_error', error: err?.name ?? 'Error' });
      } finally {
        clearTimeout(timer);
      }
    })();
    return closing;
  };
  proc.once('SIGTERM', () => shutdown('SIGTERM'));
  proc.once('SIGINT', () => shutdown('SIGINT'));

  return { server, shutdown, store, consumer };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  run().catch((err) => {
    const detail = err?.name === 'ConfigError' ? err.problems : undefined;
    defaultLogger({ event: 'startup_failed', error: err?.name ?? 'Error', detail });
    process.exit(1);
  });
}
