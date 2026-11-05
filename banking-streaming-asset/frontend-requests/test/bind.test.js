import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { HOST, resolveHost, resolvePort, run } from '../src/server.js';
import { goodEnv } from './helpers.js';

test('HOST por defecto: 127.0.0.1', () => {
  assert.equal(HOST, '127.0.0.1');
  assert.equal(resolveHost(undefined), '127.0.0.1');
});

test('HOST permitido: 127.0.0.1 y 0.0.0.0 (este último solo para el namespace de red del contenedor)', () => {
  assert.equal(resolveHost('127.0.0.1'), '127.0.0.1');
  assert.equal(resolveHost('0.0.0.0'), '0.0.0.0');
});

test('HOST: cualquier otro valor se rechaza', () => {
  for (const bad of ['', ' ', 'localhost', '::', '::1', '10.0.0.5', '192.168.1.10', '0.0.0.0 ', '127.0.0.2', '0', 'example.com', null]) {
    assert.throws(() => resolveHost(bad), /HOST must be 127\.0\.0\.1 or 0\.0\.0\.0/, JSON.stringify(bad));
  }
});

test('PORT: valor válido aceptado, por defecto 3000', () => {
  assert.equal(resolvePort(undefined), 3000);
  assert.equal(resolvePort('8080'), 8080);
  assert.equal(resolvePort('1'), 1);
  assert.equal(resolvePort('65535'), 65535);
});

test('PORT: valores inválidos rechazados', () => {
  for (const bad of ['', '0', '-1', '65536', '80.5', 'abc', '8080abc', ' ']) {
    assert.throws(() => resolvePort(bad), /PORT must be an integer between 1 and 65535/, JSON.stringify(bad));
  }
});

test('run: un HOST inválido falla antes de crear el runtime (sin conectar con Kafka)', async () => {
  let runtimeCreated = false;
  const proc = new EventEmitter();
  await assert.rejects(
    run({
      env: { ...goodEnv(), HOST: '10.0.0.5' },
      runtimeFactory: () => {
        runtimeCreated = true;
        throw new Error('no debe llamarse');
      },
      proc,
      logger: () => {},
    }),
    /HOST must be 127\.0\.0\.1 or 0\.0\.0\.0/,
  );
  assert.equal(runtimeCreated, false);
  assert.equal(proc.listenerCount('SIGTERM'), 0);
});

test('run: un PORT inválido falla antes de crear el runtime', async () => {
  let runtimeCreated = false;
  await assert.rejects(
    run({
      env: { ...goodEnv(), PORT: 'abc' },
      runtimeFactory: () => {
        runtimeCreated = true;
        throw new Error('no debe llamarse');
      },
      proc: new EventEmitter(),
      logger: () => {},
    }),
    /PORT must be an integer/,
  );
  assert.equal(runtimeCreated, false);
});
