# frontend-requests

Formulario web para crear solicitudes de préstamo y publicarlas en Confluent Cloud.

```
React + Carbon (formulario)
        ↓  POST /api/loan-requests
     Express (validación estricta)
        ↓  Avro (esquema existente)
   Schema Registry
        ↓
 Kafka · topic loan-requests  →  Flink  →  loan-decisions  →  Dashboard (:8081)
```

## Propósito

Este componente **solo genera solicitudes de préstamo**.

- **No calcula el score.**
- **No decide APPROVED/REJECTED ni muestra decisiones.**
- Tras el envío, la UI muestra «Loan request submitted», el `request_id`, el `requested_at`, el aviso «The application is being processed. View real-time decisions in the dashboard.» y los botones «Open Dashboard» (abre `http://127.0.0.1:8081`) y «Submit another request».
- El navegador nunca habla con Kafka. Las decisiones de Flink se ven en el dashboard (:8081).

## Componentes

| Componente | Descripción |
|---|---|
| UI React + Carbon | Formulario en `web/` (Vite en build time). Llama a la URL relativa `/api/loan-requests`. |
| API Express | `src/server.js`. Valida, genera `request_id` y `requested_at`, codifica en Avro y publica. Sirve también el build de React. |
| `POST /api/loan-requests` | Crea una solicitud. Ver [API](#api). |
| `GET /health` | Liveness: devuelve `200 {"status":"ok"}`. Solo indica que el proceso HTTP responde; no consulta Kafka, Schema Registry ni Flink. |
| Avro | Se reutiliza `terraform/schemas/loan-requests.avsc` desde su origen (no se copia). Logical types `decimal(12,2)` y `timestamp-millis`. |
| Schema Registry | Solo lectura: se obtiene el schema ID de `loan-requests-value` y se comprueba por fingerprint que coincide con el `.avsc`. No registra esquemas. |
| `@platformatic/kafka` | Cliente Kafka (JS puro, TLS + SASL PLAIN). Clave del mensaje = `String(customer_id)`. |
| Topic `loan-requests` | Destino de las solicitudes (configurable con `TOPIC_LOAN_REQUESTS`). |

## Formulario

El cliente envía cuatro campos:

| Campo | Descripción |
|---|---|
| `customer_id` | Identificador del cliente. |
| `amount` | Importe solicitado. |
| `term_months` | Plazo en meses. |
| `purpose` | Finalidad del préstamo. |

`request_id` (UUID) y `requested_at` los **genera el servidor**. Si el cliente los envía, la petición se rechaza.

## Validación

Se aplica en el servidor (la UI replica las reglas solo para dar feedback al usuario).

- **`customer_id`**: entero, de 1 a 2147483647 (int32).
- **`amount`**:
  - mayor que 0;
  - compatible con `decimal(12,2)`: hasta 10 dígitos enteros;
  - máximo 2 decimales (nunca se redondea);
  - sin signo, sin exponente y sin ceros a la izquierda;
  - se recomienda enviarlo como cadena (`"10000.00"`); se normaliza siempre a 2 decimales.
- **`term_months`**: `12`, `24`, `36`, `48`, `60`, `120` o `240`.
- **`purpose`**: `HOME`, `CAR`, `EDUCATION`, `PERSONAL`, `BUSINESS` o `DEBT_CONSOLIDATION`.
- Cualquier **campo desconocido** (incluidos `request_id` y `requested_at`) invalida la petición.
- Cuerpo máximo: 4 kB.

## Variables de entorno

Solo se documentan los nombres; los valores salen de `.env` y `.env.runtime` (generado por `make cloud-up`). Nunca se escriben en el repositorio ni en la imagen.

| Variable | Uso |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Bootstrap de Kafka en Confluent Cloud. |
| `SCHEMA_REGISTRY_URL` | Endpoint de Schema Registry. |
| `REQUESTS_API_KAFKA_API_KEY` | API key de Kafka de la identidad `requests-api`. |
| `REQUESTS_API_KAFKA_API_SECRET` | Secret de la anterior. |
| `REQUESTS_API_SR_API_KEY` | API key de Schema Registry de `requests-api`. |
| `REQUESTS_API_SR_API_SECRET` | Secret de la anterior. |
| `TOPIC_LOAN_REQUESTS` | Topic de destino (`loan-requests`). |
| `HOST` | Dirección de bind. Opcional. |
| `PORT` | Puerto de escucha. Opcional, por defecto `3000`; entero de 1 a 65535. |

Las siete primeras son obligatorias: si falta alguna, el proceso termina al arrancar con un error que lista los nombres (nunca los valores).

**`HOST`** solo admite dos valores; cualquier otro hace fallar el arranque:

- **Local** (Node directo en el host): `HOST=127.0.0.1` (valor por defecto).
- **Contenedor**: `HOST=0.0.0.0`. Solo aplica dentro del namespace de red del contenedor. Compose publica el puerto en el host **únicamente** como `127.0.0.1:8080:8080`.

## Desarrollo local

Requiere Node ≥ 22 (el contenedor usa Node 24). Desde `frontend-requests/`:

```bash
npm install          # dependencias
npm test             # API (node --test) + UI (vitest), todo offline
npm run build:web    # genera web/dist (no existe un script "build")
npm start            # node src/server.js; requiere las variables de entorno
```

Scripts disponibles: `start`, `test`, `test:api`, `test:web` y `build:web`.

Los tests no se conectan a Kafka ni a Schema Registry. Para arrancar con `npm start` hay que exportar las variables de la sección anterior (por ejemplo con `node --env-file=../.env --env-file=../.env.runtime src/server.js`). Si existe `web/dist`, Express sirve también la UI; si no, avisa (`static_dir_missing`) y funciona solo como API.

## Docker

El contexto de build es la raíz del asset (`banking-streaming-asset/`), porque el esquema Avro se reutiliza desde `terraform/schemas/`:

```bash
podman build \
  -f frontend-requests/Dockerfile \
  -t loan-request-frontend:0.1.0 \
  .
```

- Imagen base: UBI 9 Node 24 LTS (`registry.access.redhat.com/ubi9/nodejs-24-minimal`, no requiere login de Red Hat).
- Multi-stage: dependencias de producción, build (ejecuta `npm test` y `vite build`) y runtime.
- El runtime corre como **non-root (UID 1001)**.
- React se compila en build time; Express sirve el frontend y la API en un único proceso.
- **No hay Vite ni nginx en runtime.**
- `Dockerfile.dockerignore` es una allowlist: no entran `.env*`, `*.tfstate*`, `node_modules` ni `web/dist` del host. La imagen no contiene credenciales.

## Compose

El servicio `frontend-requests` está en `compose/compose.yaml` como servicio normal (sin profile) y no tiene `depends_on`: su única dependencia es Confluent Cloud. Requiere haber ejecutado antes `make cloud-up` (genera `.env.runtime`). Desde la raíz del asset:

```bash
podman compose -f compose/compose.yaml \
  --env-file .env --env-file .env.runtime \
  up -d --build --no-deps frontend-requests
```

Para pararlo sin tocar el resto de servicios:

```bash
podman compose -f compose/compose.yaml \
  --env-file .env --env-file .env.runtime \
  stop frontend-requests
```

`make local-up` también lo levanta junto a Postgres y Connect. El servicio pasa de forma explícita solo `HOST`, `PORT` y las siete variables del backend (sin `env_file`).

- Aplicación: <http://127.0.0.1:8080>
- Health: <http://127.0.0.1:8080/health>

El healthcheck del contenedor hace `GET /health` (intervalo 15 s, timeout 5 s, 3 reintentos, 40 s de margen de arranque).

## API

### `POST /api/loan-requests`

```json
{
  "customer_id": 1001,
  "amount": "10000.00",
  "term_months": 24,
  "purpose": "PERSONAL"
}
```

Respuesta correcta, `HTTP 202`:

```json
{
  "request_id": "...",
  "requested_at": "..."
}
```

**`202` significa «solicitud aceptada y publicada en `loan-requests` para su procesamiento».** No significa que el préstamo esté aprobado: la decisión la toma Flink. Solo se responde `202` tras la confirmación del broker.

Otras respuestas, siempre JSON y con mensaje genérico:

| Código | Cuerpo | Cuándo |
|---|---|---|
| 400 | `{"error":"Invalid request"}` | Validación fallida, campo desconocido o JSON mal formado. |
| 413 | `{"error":"Invalid request"}` | Cuerpo mayor de 4 kB. |
| 404 | `{"error":"Not found"}` | Ruta `/api/*` inexistente (nunca se devuelve HTML). |
| 503 | `{"error":"Service unavailable"}` | Fallo de Kafka o de Schema Registry. |
| 500 | `{"error":"Internal error"}` | Error inesperado. |

## Seguridad

- Sin secretos hardcodeados: la configuración se inyecta en runtime y no se copia a la imagen.
- En el host, el puerto solo se publica en `127.0.0.1`. En local, `HOST` vale `127.0.0.1` por defecto.
- Contenedor non-root (UID 1001).
- Validación estricta en servidor; los campos desconocidos se rechazan.
- Errores genéricos hacia el cliente; el detalle no sale del servidor.
- Los logs son JSON por línea y no incluyen credenciales ni cuerpos de petición.
- Se usa el esquema ya registrado: la aplicación **no** registra esquemas automáticamente (equivale a `auto.register.schemas=false`) y falla si el esquema registrado no coincide con `loan-requests.avsc`.
- Identidad de mínimo privilegio (`requests-api`): el código solo escribe en `loan-requests`. Sin ACLs ni service accounts nuevos.
- Las credenciales de Kafka y Schema Registry solo existen en el backend; el navegador solo llama a `/api/*`.

## Tests

| Verificación | Estado |
|---|---|
| API/backend | 72 tests PASS |
| UI | 19 tests PASS |
| **Total** | **91 tests PASS** |
| Validación del contenedor en runtime | PASS |
| Fingerprint del esquema en Schema Registry | PASS |
| Publicación en Kafka | PASS |

## Limitaciones conocidas

1. **`retries=0`.** Se desactivan los reintentos automáticos para no duplicar mensajes (la ACL actual no concede escritura idempotente). Un fallo de publicación ambiguo (por ejemplo, un timeout tras escribir) seguido de un reintento del cliente podría generar una nueva solicitud con otro `request_id`. La deduplicación queda fuera del MVP.
2. **Las decisiones no se muestran en :8080.** Se eliminó el polling de `loan-decisions`: la UI solo confirma el envío y enlaza al dashboard (:8081), que es donde se ven las decisiones en tiempo real. Los permisos de `requests-api` sobre `loan-decisions` siguen en Terraform, sin uso.
3. **Filas repetidas en `flink-sql.sh`.** En alguna lectura puntual con `scripts/flink-sql.sh` la misma fila apareció repetida. Eso no implica duplicación en el topic: en la validación integrada hubo un único `POST`, un único `publish_confirmed`, un único offset y un `COUNT(*)` acotado igual a 1. La causa de la repetición en el lector no se ha investigado.
