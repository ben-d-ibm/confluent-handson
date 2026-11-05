import { existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { validateLoanRequestInput } from './validation.js';
import { buildLoanRequest, loanRequestKey } from './loanRequest.js';
import { createRuntime } from './runtime.js';

// Bind por defecto: loopback. Solo se permite cambiarlo (HOST=0.0.0.0) dentro del namespace de red de un
// contenedor; el puerto se publica en el host exclusivamente en 127.0.0.1 (compose/compose.yaml).
export const HOST = '127.0.0.1';
const ALLOWED_HOSTS = new Set([HOST, '0.0.0.0']);
const DEFAULT_PORT = 3000;

// Build de producción de React (npm run build:web). En la imagen vive en /app/web/dist.
export const DEFAULT_STATIC_DIR = fileURLToPath(new URL('../web/dist', import.meta.url));
const HAS_FILE_EXTENSION = /\.[A-Za-z0-9]+$/;

/**
 * Interfaz del publisher (se inyectará en un checkpoint posterior; aquí no hay Kafka):
 *   publisher.publish({ key: string, loanRequest: object }) => Promise<void>
 * El publisher real se encargará de Avro + Schema Registry + Kafka. La API nunca calcula
 * score ni decisión: la única fuente de decisiones es Flink (topic loan-decisions).
 */
export function createApp({ publisher = null, uuid, now, logger = defaultLogger, staticDir = null } = {}) {
  const app = express();
  app.disable('x-powered-by');
  app.use(express.json({ limit: '4kb' }));

  // Liveness local: solo indica que el proceso HTTP responde. No toca Kafka ni Schema Registry.
  app.get('/health', (_req, res) => {
    res.status(200).json({ status: 'ok' });
  });

  app.post('/api/loan-requests', async (req, res) => {
    const result = validateLoanRequestInput(req.body);
    if (!result.ok) {
      return res.status(400).json({ error: 'Invalid request' });
    }

    if (!publisher) {
      return res.status(503).json({ error: 'Service unavailable' });
    }

    const loanRequest = buildLoanRequest(result.value, { uuid, now });
    try {
      const ack = await publisher.publish({ key: loanRequestKey(loanRequest), loanRequest });
      logger({
        event: 'publish_confirmed',
        request_id: loanRequest.request_id,
        topic: ack?.topic,
        partition: ack?.partition,
        offset: ack?.offset,
      });
    } catch (err) {
      logger({ event: 'publish_failed', error: err?.name ?? 'Error' });
      return res.status(503).json({ error: 'Service unavailable' });
    }

    // Solo acuse de recepción: sin score ni decisión.
    return res.status(202).json({
      request_id: loanRequest.request_id,
      requested_at: loanRequest.requested_at.toISOString(),
    });
  });

  // Las rutas /api/* desconocidas nunca caen en el frontend: siempre JSON.
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
    // Solo sirve ficheros dentro de root (sin dotfiles, sin listado de directorios).
    app.use(
      express.static(root, {
        index: false,
        dotfiles: 'ignore',
        redirect: false,
        setHeaders: (res, filePath) => {
          res.set(
            'Cache-Control',
            filePath.startsWith(path.join(root, 'assets') + path.sep)
              ? 'public, max-age=31536000, immutable' // nombres con hash de contenido
              : 'no-cache',
          );
        },
      }),
    );
    app.get('/', (_req, res, next) => sendIndex(res, next));
    // Fallback SPA: solo navegaciones GET sin extensión. Un asset inexistente (/assets/x.js) da 404 JSON.
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
    if (err?.type === 'entity.parse.failed') {
      return res.status(400).json({ error: 'Invalid request' });
    }
    if (err?.type === 'entity.too.large') {
      return res.status(413).json({ error: 'Invalid request' });
    }
    logger({ event: 'unhandled_error', error: err?.name ?? 'Error' });
    return res.status(500).json({ error: 'Internal error' });
  });

  return app;
}

function defaultLogger(entry) {
  // JSON por línea en stdout; los eventos nunca contienen credenciales ni cuerpos de petición.
  console.log(JSON.stringify({ ts: new Date().toISOString(), ...entry }));
}

export function startServer({ port = DEFAULT_PORT, host = HOST, ...deps } = {}) {
  return createApp(deps).listen(port, host);
}

/** HOST: lista cerrada (127.0.0.1 por defecto o 0.0.0.0). Cualquier otro valor, incluido vacío, se rechaza. */
export function resolveHost(raw) {
  if (raw === undefined) return HOST;
  if (!ALLOWED_HOSTS.has(raw)) {
    throw new Error('HOST must be 127.0.0.1 or 0.0.0.0');
  }
  return raw;
}

/** Directorio estático de producción, o null (con aviso) si no se ha generado el build de React. */
export function resolveStaticDir(logger = defaultLogger, dir = DEFAULT_STATIC_DIR) {
  if (existsSync(path.join(dir, 'index.html'))) return dir;
  logger({ event: 'static_dir_missing' });
  return null;
}

export function resolvePort(raw) {
  if (raw === undefined) return DEFAULT_PORT;
  const port = Number(raw);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error('PORT must be an integer between 1 and 65535');
  }
  return port;
}

const SHUTDOWN_TIMEOUT_MS = 10000;
const STARTUP_TIMEOUT_MS = 30000;

/**
 * Arranque completo: valida la configuración (falla antes de escuchar), conecta el producer,
 * escucha en 127.0.0.1 y registra un shutdown limpio en SIGTERM/SIGINT.
 * Todas las dependencias son inyectables para poder probar el ciclo de vida offline.
 */
export async function run({
  env = process.env,
  port = resolvePort(env.PORT), // el override solo existe para tests (puerto efímero)
  host = resolveHost(env.HOST),
  runtimeFactory = createRuntime,
  proc = process,
  logger = defaultLogger,
} = {}) {
  const runtime = runtimeFactory(env, { logger }); // ConfigError si falta/está mal alguna variable (sin valores)
  let startupTimer;
  const info = await Promise.race([
    runtime.connect(),
    new Promise((_, reject) => {
      startupTimer = setTimeout(() => reject(new Error('startup timed out')), STARTUP_TIMEOUT_MS);
    }),
  ]).finally(() => clearTimeout(startupTimer));
  if (info) {
    logger({ event: 'kafka_connected', topic: info.topic });
    // connect() solo resuelve si el esquema registrado coincide por fingerprint con loan-requests.avsc.
    logger({ event: 'schema_resolved', subject: info.subject, schema_id: info.schemaId, fingerprint_match: true });
  }

  const server = startServer({
    port,
    host,
    publisher: runtime.publisher,
    logger,
    staticDir: resolveStaticDir(logger),
  });
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
        await runtime.disconnect();
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

  return { server, shutdown };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  run().catch((err) => {
    // Solo nombre y, para ConfigError, los problemas (nombres de variables, nunca valores).
    const detail = err?.name === 'ConfigError' ? err.problems : undefined;
    defaultLogger({ event: 'startup_failed', error: err?.name ?? 'Error', detail });
    process.exit(1);
  });
}
