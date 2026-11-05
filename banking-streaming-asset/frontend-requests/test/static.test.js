import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createApp, HOST, resolveStaticDir, DEFAULT_STATIC_DIR } from '../src/server.js';

// Fixture: <tmp>/outside-secret.txt queda FUERA de <tmp>/dist, que es lo único que se sirve.
const INDEX = '<!doctype html><html><body><div id="root"></div></body></html>';
const ASSET = 'console.log("asset");';
const SECRET = 'TOP-SECRET-OUTSIDE-DIST';

const body = { customer_id: 1001, amount: 10000, term_months: 24, purpose: 'PERSONAL' };
const published = [];
const publisher = { publish: async (msg) => void published.push(msg) };

let tmp;
let server;
let noStaticServer;

const listen = (deps) =>
  new Promise((resolve) => {
    const s = createApp(deps).listen(0, HOST, () => resolve(s));
  });

const url = (s, p) => `http://${HOST}:${s.address().port}${p}`;

// fetch() normaliza "../"; http.request permite enviar la ruta tal cual para probar traversal.
const rawGet = (s, rawPath) =>
  new Promise((resolve, reject) => {
    const req = http.request({ host: HOST, port: s.address().port, path: rawPath, method: 'GET' }, (res) => {
      let data = '';
      res.on('data', (c) => (data += c));
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.end();
  });

before(async () => {
  tmp = mkdtempSync(path.join(tmpdir(), 'static-test-'));
  const dist = path.join(tmp, 'dist');
  mkdirSync(path.join(dist, 'assets'), { recursive: true });
  writeFileSync(path.join(dist, 'index.html'), INDEX);
  writeFileSync(path.join(dist, 'assets', 'index-abc123.js'), ASSET);
  writeFileSync(path.join(dist, '.hidden'), 'dotfile');
  writeFileSync(path.join(tmp, 'outside-secret.txt'), SECRET);

  server = await listen({
    publisher,
    uuid: () => 'req-static',
    now: () => new Date('2026-01-02T03:04:05.678Z'),
    staticDir: dist,
    logger: () => {},
  });
  noStaticServer = await listen({ logger: () => {} });
});

after(() => {
  server.close();
  noStaticServer.close();
  rmSync(tmp, { recursive: true, force: true });
});

test('GET / devuelve index.html', async () => {
  const res = await fetch(url(server, '/'));
  assert.equal(res.status, 200);
  assert.match(res.headers.get('content-type'), /text\/html/);
  assert.equal(await res.text(), INDEX);
  assert.equal(res.headers.get('cache-control'), 'no-cache');
  assert.equal(res.headers.get('x-powered-by'), null);
});

test('assets estáticos: se sirven con su content-type y caché inmutable', async () => {
  const res = await fetch(url(server, '/assets/index-abc123.js'));
  assert.equal(res.status, 200);
  assert.match(res.headers.get('content-type'), /javascript/);
  assert.match(res.headers.get('cache-control'), /immutable/);
  assert.equal(res.headers.get('x-content-type-options'), 'nosniff');
  assert.equal(await res.text(), ASSET);
});

test('asset inexistente (con extensión): 404 JSON, no index.html', async () => {
  const res = await fetch(url(server, '/assets/missing.js'));
  assert.equal(res.status, 404);
  assert.match(res.headers.get('content-type'), /application\/json/);
  assert.deepEqual(await res.json(), { error: 'Not found' });
});

test('/api/loan-requests sigue funcionando igual con el frontend estático', async () => {
  const res = await fetch(url(server, '/api/loan-requests'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  assert.equal(res.status, 202);
  assert.deepEqual(await res.json(), { request_id: 'req-static', requested_at: '2026-01-02T03:04:05.678Z' });
  assert.equal(published.length, 1);

  const bad = await fetch(url(server, '/api/loan-requests'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ ...body, purpose: 'NOPE' }),
  });
  assert.equal(bad.status, 400);
  assert.deepEqual(await bad.json(), { error: 'Invalid request' });
});

test('GET /api/loan-requests (método no definido) y /api/* inexistente: JSON, nunca index.html', async () => {
  for (const p of ['/api/ruta-inexistente', '/api/loan-requests', '/api', '/api/a/b/c']) {
    const res = await fetch(url(server, p));
    assert.equal(res.status, 404, p);
    assert.match(res.headers.get('content-type'), /application\/json/, p);
    assert.deepEqual(await res.json(), { error: 'Not found' }, p);
  }
  const post = await fetch(url(server, '/api/ruta-inexistente'), { method: 'POST' });
  assert.equal(post.status, 404);
  assert.match(post.headers.get('content-type'), /application\/json/);
});

test('ruta de frontend desconocida (sin extensión): fallback SPA a index.html', async () => {
  const res = await fetch(url(server, '/requests/new'));
  assert.equal(res.status, 200);
  assert.match(res.headers.get('content-type'), /text\/html/);
  assert.equal(await res.text(), INDEX);
});

test('el fallback SPA solo aplica a GET', async () => {
  const res = await fetch(url(server, '/requests/new'), { method: 'POST' });
  assert.equal(res.status, 404);
  assert.match(res.headers.get('content-type'), /application\/json/);
});

test('no se exponen archivos fuera de web/dist (path traversal)', async () => {
  const attempts = [
    '/../outside-secret.txt',
    '/assets/../../outside-secret.txt',
    '/%2e%2e/outside-secret.txt',
    '/%2e%2e%2foutside-secret.txt',
    '/assets/%2e%2e/%2e%2e/outside-secret.txt',
    '/..%5coutside-secret.txt',
    '/outside-secret.txt',
  ];
  for (const p of attempts) {
    const res = await rawGet(server, p);
    assert.ok(!res.body.includes(SECRET), `filtración con ${p}`);
    assert.notEqual(res.status, 500, p);
  }
});

test('no se sirven dotfiles ni se lista el directorio', async () => {
  const dot = await fetch(url(server, '/.hidden'));
  assert.ok(![200].includes(dot.status));
  assert.ok(!(await dot.text()).includes('dotfile'));

  const dir = await fetch(url(server, '/assets/'));
  assert.ok(!(await dir.text()).includes('index-abc123.js'));
});

test('/health: 200 {"status":"ok"} sin consultar dependencias ni exponer configuración', async () => {
  for (const s of [server, noStaticServer]) {
    const res = await fetch(url(s, '/health'));
    assert.equal(res.status, 200);
    assert.match(res.headers.get('content-type'), /application\/json/);
    assert.deepEqual(await res.json(), { status: 'ok' });
  }
});

test('sin staticDir (dev/tests API): no hay frontend y todo lo desconocido es 404 JSON', async () => {
  const res = await fetch(url(noStaticServer, '/'));
  assert.equal(res.status, 404);
  assert.deepEqual(await res.json(), { error: 'Not found' });
});

test('resolveStaticDir: devuelve el directorio solo si contiene index.html; si no, avisa y devuelve null', () => {
  const events = [];
  assert.equal(resolveStaticDir((e) => events.push(e), path.join(tmp, 'dist')), path.join(tmp, 'dist'));
  assert.equal(events.length, 0);
  assert.equal(resolveStaticDir((e) => events.push(e), path.join(tmp, 'no-existe')), null);
  assert.deepEqual(events, [{ event: 'static_dir_missing' }]);
  assert.ok(DEFAULT_STATIC_DIR.endsWith(path.join('frontend-requests', 'web', 'dist')));
});
