# frontend-dashboard

Dashboard de solo lectura que muestra las decisiones de préstamo que produce Flink en `loan-decisions`.

```
Kafka · topic loan-decisions  (escrito por Flink)
        ↓  consumer Node (Avro + Schema Registry)
   estado en memoria (dedupe por request_id)
        ↓
   Express  →  GET /api/summary · GET /api/decisions
        ↓  polling cada 3 s
 React + Carbon Charts  (el navegador nunca habla con Kafka)
```

## Qué muestra

- **KPIs**: Total decisions, Approved, Rejected, Approval rate, Average score, Total approved amount.
- **Gráficos** (Carbon Charts): donut APPROVED vs REJECTED y barras con la distribución de score (0-19, 20-39, 40-59, 60-79, 80-100).
- **Tabla** con las últimas 50 decisiones: `request_id`, `customer_id`, `amount`, `purpose`, `score`, `decision`, `reasons`, `decided_at`.
- **Polling**: la UI consulta `/api/summary` y `/api/decisions?limit=50` cada 3 s. Si el backend falla, conserva los últimos datos y muestra un aviso discreto; el siguiente ciclo reintenta.

## Qué NO hace

- **No calcula el score ni genera decisiones.** Solo lee `loan-decisions`; Flink es la única fuente de decisiones.
- **No hay datos simulados.** Sin decisiones muestra «No decisions received yet».
- No tiene base de datos: el estado vive en memoria (máx. 1000 decisiones únicas por `request_id`, gana la última recibida) y se reconstruye leyendo el topic desde el principio en cada arranque.

> Una decisión nueva puede tardar varios minutos en aparecer (latencia conocida del pipeline Flink, ISSUE-1). Es normal.

## Endpoints

| Ruta | Descripción |
|---|---|
| `/` | Dashboard React (build estático servido por Express). |
| `GET /health` | `200 {"status":"ok"}`. Solo comprueba que Express responde; no consulta Kafka ni Schema Registry. |
| `GET /api/summary` | Totales, tasa de aprobación, score medio, importe aprobado y distribución de score. |
| `GET /api/decisions?limit=N` | Últimas decisiones, más recientes primero (`N` de 1 a 200, por defecto 50). |

Cualquier otra ruta `/api/*` devuelve `404` JSON.

## Variables de entorno

Solo nombres; los valores salen de `.env` y `.env.runtime` (generado por `make cloud-up`) y nunca se escriben en el repositorio ni en la imagen.

| Variable | Uso |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Bootstrap de Kafka en Confluent Cloud. |
| `SCHEMA_REGISTRY_URL` | Endpoint de Schema Registry. |
| `DASHBOARD_API_KAFKA_API_KEY` / `DASHBOARD_API_KAFKA_API_SECRET` | Credenciales Kafka de la identidad `dashboard_api` (lectura de `loan-decisions`). |
| `DASHBOARD_API_SR_API_KEY` / `DASHBOARD_API_SR_API_SECRET` | Credenciales de Schema Registry de `dashboard_api` (solo lectura del subject `loan-decisions-value`). |
| `DASHBOARD_API_GROUP_PREFIX` | Prefijo de consumer group autorizado; el grupo usado es `<prefijo>dashboard`. |
| `TOPIC_LOAN_DECISIONS` | Topic a leer (`loan-decisions`). |
| `HOST` | `127.0.0.1` (por defecto) o `0.0.0.0` (contenedor). Cualquier otro valor hace fallar el arranque. |
| `PORT` | Puerto de escucha, por defecto `3001` (Compose usa `8081`). |

Si falta alguna variable obligatoria, el proceso termina al arrancar con un error que lista los nombres (nunca los valores).

## Ejecución local

Requiere Node ≥ 22. Desde `frontend-dashboard/`:

```bash
npm install
npm test        # API (node --test) + UI (vitest), todo offline
npm run build   # genera web/dist
node --env-file=../.env --env-file=../.env.runtime src/server.js   # http://127.0.0.1:3001
```

## Docker

El contexto de build es la raíz del asset (`banking-streaming-asset/`):

```bash
podman build \
  -f frontend-dashboard/Dockerfile \
  -t loan-decision-dashboard:0.1.0 \
  .
```

UBI 9 Node 24 (`registry.access.redhat.com`), multi-stage (el build ejecuta `npm test` y `vite build`), runtime solo con dependencias de producción, usuario non-root (UID 1001). Sin Vite ni nginx en runtime. `Dockerfile.dockerignore` es una allowlist: la imagen no contiene `.env*` ni credenciales.

## Compose

El servicio `frontend-dashboard` está en `compose/compose.yaml` (servicio normal, sin profile ni `depends_on`; su única dependencia es Confluent Cloud). `make up` lo levanta junto al resto. Solo él, desde la raíz del asset:

```bash
podman compose -f compose/compose.yaml \
  --env-file .env --env-file .env.runtime \
  up -d --build --no-deps frontend-dashboard
```

- Dashboard: <http://127.0.0.1:8081>
- Health: <http://127.0.0.1:8081/health>

El puerto se publica solo como `127.0.0.1:8081:8081`; `HOST=0.0.0.0` aplica únicamente dentro del contenedor. El healthcheck hace `GET /health` con timeouts finitos.
