# Plan: Asset «Banca Streaming» (Confluent Cloud + Flink + Tableflow + watsonx.data opcional)

> Versión 2. Misma arquitectura que la v1, con implementación incremental: primero un vertical slice (MVP) que valida CDC → Flink, y después UX, capa analítica opcional y packaging.

## Top-Level Overview

**Objetivo:** crear un asset reutilizable, orientado a un cliente bancario, que levante con un solo comando un flujo de scoring de préstamos en tiempo real sobre Confluent Cloud (AWS), usando Terraform para la parte cloud y Docker/Podman Compose para la parte local.

**Entregables**
1. Carpeta `banking-streaming-asset/` con código y README.md en castellano. Se distribuye como zip a los participantes.
2. `laboratorio_banca_streaming.html` en la raíz del repo, **fuera del zip**, enlazado desde `index.html`. Incluye la descripción del flujo, cómo descargar el zip, las variables del `.env` y los comandos paso a paso.

**Flujo objetivo (completo)**
- Productor Java y frontend de solicitudes publican `loan-requests` (y `payments` en V2).
- PostgreSQL local (`customers`, `payment_history`) llega a Confluent con Kafka Connect self-managed + Debezium.
- Flink SQL enriquece `loan-requests` con los CDC, calcula un score por reglas hardcodeadas y escribe APPROVED/REJECTED en `loan-decisions`.
- `ENABLE_WATSONXDATA=false` (por defecto): el dashboard consume `loan-decisions` directamente desde Kafka.
- `ENABLE_WATSONXDATA=true`: Tableflow → Iceberg (almacenamiento gestionado por Confluent) → catálogo Iceberg REST → watsonx.data → Presto → dashboard.

**Decisiones confirmadas**
| Tema | Decisión |
|---|---|
| CDC | Kafka Connect self-managed + Debezium en contenedor local |
| BD | PostgreSQL |
| Orquestación | Terraform (`confluentinc/confluent`) + compose + wrapper Makefile/bash |
| Frontends | React + Carbon Design System / Carbon Charts, backend Node/Express |
| Score | Reglas hardcodeadas por rangos. El modo IA (`ML_PREDICT`/`AI_COMPLETE`) solo se menciona en la documentación, no se implementa y no requiere Bedrock |
| Tableflow → watsonx.data | Almacenamiento gestionado por Confluent + catálogo Iceberg REST |
| watsonx.data | Instancia existente (SaaS u on-prem/CP4D); solo se piden host, puerto, credenciales y catálogo como variables |
| Nube | AWS, región parametrizable (por defecto `us-east-1`) |
| Runtime | Compose compatible con Docker y Podman |
| Documentación | Castellano |
| `payments` | **Fuera del MVP.** Extensión V2 (Fase 5) |
| watsonx.data | Opcional. **No debe bloquear** el asset principal. El laboratorio debe poder ejecutarse completo en modo Confluent-only |

**Estructura propuesta**
```
banking-streaming-asset/
  README.md
  .env.example
  .gitignore
  Makefile            (up, down, destroy, check, status)
  scripts/            (check.sh, up.sh, down.sh, registro del conector, generador de loan-requests, smoke test)
  terraform/          (env, cluster, SR, tópicos, API keys, Flink; Tableflow condicional en Fase 3)
  compose/            (postgres, connect-debezium; luego productor, backends y frontends)
  postgres/           (init SQL: esquema + datos sintéticos)
  flink-sql/          (sentencias SQL parametrizadas)
  producer-java/      (Fase 2)
  frontend-requests/  (Fase 2)
  frontend-dashboard/ (Fase 2 modo Kafka, Fase 3 modo Presto)
```

## Reglas transversales
- Imágenes de la familia UBI de Red Hat, mínimas y con usuario no root. Para Connect/Debezium, imagen propia sobre UBI mínima con el plugin. Si no es viable, documentar la excepción.
- Puertos publicados solo en `127.0.0.1`. Nada en `0.0.0.0`.
- Cero secretos en el repo: `.env` y `*.tfvars` en `.gitignore`, solo se versiona `.env.example`. Las API keys las genera Terraform y se vuelcan a un `.env` generado.
- TLS hacia Confluent Cloud, con timeouts explícitos.
- Dependencias en su última versión estable y mantenida, con lockfiles.
- Logging estructurado sin datos sensibles. Validación de entradas en los backends. Errores genéricos al cliente.
- `datagen-users-connector.json` contiene un secreto en claro: no replicar ese patrón.

## Mapa del backlog original → fases
| Sub-tarea original | Dónde se implementa |
|---|---|
| 1. Esqueleto y configuración | Fase 1 (versión mínima) y Fase 4 (completa) |
| 2. Terraform | Fase 1 (mínimo) y Fase 3 (Tableflow) |
| 3. PostgreSQL + Debezium | Fase 1 |
| 4. Productor Java + frontend de solicitudes | Fase 1 (generador mínimo) y Fase 2 (definitivos) |
| 5. Flink SQL | Fase 1 |
| 6. Dashboard dual | Fase 2 (Kafka) y Fase 3 (Presto) |
| 7. Orquestación y wrapper | Fase 1 (parcial) y Fase 4 |
| 8. Documentación | Fase 4 |

---

# FASE 1 — MVP técnico (vertical slice)

**Criterio de éxito de la fase:** *una solicitud de préstamo entra a Confluent, Flink la enriquece con información de PostgreSQL vía CDC, calcula el score y genera un evento en `loan-decisions`.*

```
PostgreSQL (customers, payment_history) → Debezium → Confluent Cloud → Flink SQL → loan-decisions
loan-requests (generador mínimo) ────────────────────────────────────→ Flink SQL
```

### 1.1 Spike CDC → Flink (primer paso técnico, riesgo principal)
**Intent:** validar explícitamente la integración antes de construir nada alrededor. No se asume que funcione.
**Expected Outcomes:** documento corto de hallazgos (`flink-sql/SPIKE.md` o sección del README) con decisiones tomadas y evidencias, y SQL de referencia que funcione.
**Todo List:**
1. Levantar lo mínimo (Terraform mínimo + Postgres + Connect/Debezium) con unos pocos registros.
2. Validar el **formato producido por Debezium** (envelope completo vs. `ExtractNewRecordState`/unwrap) y qué formato espera Flink en Confluent Cloud para cada opción.
3. Validar las **keys de los tópicos CDC** (clave primaria de cada tabla) y que se mapeen a la PK de las tablas Flink.
4. Validar **serialización y Schema Registry** (Avro / JSON Schema / Protobuf), converters del Connect y compatibilidad con el catálogo de Flink.
5. Decidir cómo se **representan `customers` y `payment_history` en Flink** (tabla con PK en modo upsert, o changelog Debezium) y el `changelog.mode` resultante.
6. Validar la **semántica changelog/upsert** de cada tabla (inserts, updates y deletes).
7. Elegir la **estrategia de join** con `loan-requests` (temporal join con versionado, regular join, o agregado previo de `payment_history` por cliente) y documentar el trade-off.
8. Probar el **comportamiento ante updates de un `customer`** (¿se reemite una decisión? ¿se usa el valor vigente al llegar la solicitud?).
9. Probar el **comportamiento ante cambios en `payment_history`** (inserts nuevos, updates, efecto sobre el agregado de impagos).
10. Registrar riesgos residuales y fijar las decisiones que consumirán las tareas 1.3 y 1.4.
**Relevant Context:** `laboratorio_confluent_cloud.html` sección 2 (Flink SQL ya explicado a los participantes); documentación de Flink en Confluent Cloud (changelog mode, temporal joins, formatos Debezium).
**Notas de implementación (1.1):**
- Hallazgos y decisiones en `banking-streaming-asset/flink-sql/SPIKE.md`; SQL de referencia en `flink-sql/01-customer-payment-stats.sql` y `02-spike-enriched-probe.sql`.
- Validado con Confluent Cloud real: envelope Debezium completo en Avro, PK declarada con `ALTER TABLE … ADD PRIMARY KEY … NOT ENFORCED`, agregado de pagos en una tabla `upsert` y dos temporal joins desde `loan-requests`. UPDATE en `customers` e INSERT/DELETE en `payment_history` probados de extremo a extremo.
- **Requisito:** `REPLICA IDENTITY FULL` en ambas tablas (ya añadido a `postgres/init/01-schema.sql`).
- **Riesgo residual resuelto:** el registro CDC previo a `REPLICA IDENTITY FULL` (offset 16 de `customers`) se eliminó recreando solo los tópicos CDC con Terraform (`-replace`), reiniciando offsets del conector, borrando el slot y repitiendo el snapshot. Procedimiento en `SPIKE.md` §14. `make flink-check` OK (50 / 312 filas), sin statements huérfanos y sin drift de Terraform. Los subjects de Schema Registry no pudieron borrarse (403) y se reutilizan.
- `scripts/flink-sql.sh` ahora tiene plazo, límite de filas y limpieza; `make flink-check` hace una comprobación acotada. Statements Flink permanentes: ninguno activo.
**Status:** [x] done

### 1.2 Esqueleto mínimo + Terraform mínimo + `make check` inicial
**Intent:** infraestructura justa para el spike y el MVP, y un chequeo de entorno desde el principio.
**Expected Outcomes:** estructura de carpetas, `.env.example`, `.gitignore` seguro, `terraform apply` crea environment, cluster (tipo compatible con Flink), Schema Registry, service account + RBAC, API keys, tópicos (`loan-requests`, CDC de `customers` y `payment_history`, `loan-decisions`) y la Flink compute pool. Genera el `.env` local. `make check` en su primera versión.
**Todo List:**
1. Crear `banking-streaming-asset/` y el `.gitignore`.
2. Variables mínimas: credenciales de Confluent Cloud, `cloud=AWS`, `region` (por defecto `us-east-1`), runtime.
3. Terraform: `confluent_environment`, `confluent_kafka_cluster`, `confluent_schema_registry_cluster`, service accounts, role bindings, API keys, tópicos, `confluent_flink_compute_pool`.
4. Outputs y script que genera el `.env` para los componentes locales.
5. `scripts/check.sh` y objetivo `make check` (ver 4.1, se amplía en cada fase).
6. `make destroy` (`scripts/down.sh all`): baja contenedores locales y ejecuta `terraform destroy`. `make down` solo toca lo local.
**Relevant Context:** documentación del provider `confluentinc/confluent` (verificar versión actual y disponibilidad de Flink en AWS por región).
**Notas de implementación (1.2):**
- Código en `banking-streaming-asset/`. `terraform fmt` y `terraform validate` pasan (provider `confluentinc/confluent ~> 2.93`). `make check` probado sin credenciales: falla con mensajes accionables y no crea nada.
- Clúster `BASIC` por defecto (variable `cluster_type`), SR con paquete ESSENTIALS, Flink pool con `max_cfu` parametrizable (mínimo 5).
- Terraform crea 4 service accounts (infra-manager, statements-runner, app-manager, connect-cdc), sus API keys, los tópicos `loan-requests`, `customers`, `payment_history` y los 3 tópicos internos de Connect (`<project>-connect-{configs,offsets,status}`), y registra el esquema Avro de `loan-requests`.
- Los tópicos CDC se llaman `customers` y `payment_history`; en 1.3 se enrutarán desde Debezium con un SMT (RegexRouter). El spike 1.1 puede cambiar estos nombres o el formato.
- `.env.runtime` (generado por `scripts/gen-env.sh`) lleva endpoints y credenciales para Connect y Flink. Está en `.gitignore`.
- Aún NO aplicado en Confluent Cloud: pendiente `make cloud-up` con credenciales reales.
- Entorno de desarrollo: Podman 6.1.2 instalado pero la VM no responde (`podman machine start`); `podman-compose` detectado.
**Status:** [x] done (validado en estático; apply pendiente)

### 1.3 PostgreSQL + Debezium
**Intent:** proveer clientes e historial de pagos y llevarlos a Confluent vía CDC.
**Expected Outcomes:** Postgres con `wal_level=logical`, datos sintéticos coherentes, Connect con Debezium publicando en los tópicos CDC, conector registrado por script y en `RUNNING`.
**Todo List:**
1. SQL de init: `customers` (ingresos, deuda, antigüedad, etc.) y `payment_history` (pagos, atrasos, impagos) con datos mock.
2. Rol de replicación con mínimos privilegios y publicación para ambas tablas.
3. Imagen de Connect sobre UBI mínima con el plugin Debezium Postgres, configurada hacia Confluent Cloud (SASL_SSL, converters + Schema Registry) según lo decidido en 1.1.
4. Script que registra el conector por REST y espera a `RUNNING`.
**Relevant Context:** `datagen-users-connector.json` (formato de configuración de conector, sin copiar el secreto).
**Notas de implementación (1.3):**
- Implementado durante el spike y validado con Confluent Cloud real: `postgres/` (init + entrypoint con `wal_level=logical`), `connect/` (UBI, Debezium 3.7.0.Final + AvroConverter), `connect/connector-postgres.json`, `scripts/register-connector.sh` (idempotente por `PUT /config`, espera `RUNNING`).
- Rol `cdc` (REPLICATION + `SELECT` solo sobre las dos tablas), publicación `dbz_publication` explícita, `REPLICA IDENTITY FULL`. Datos: 50 clientes y 312 pagos.
- Recuperación ante tópicos CDC contaminados: `flink-sql/SPIKE.md` §14.
**Status:** [x] done

### 1.4 Generador mínimo de `loan-requests` y Flink SQL (enriquecimiento + score + decisión)
**Intent:** cerrar el camino crítico con la lógica de negocio.
**Expected Outcomes:** un mecanismo mínimo para publicar solicitudes (script con la REST API de Kafka / `INSERT ... VALUES` en Flink; se decide en el spike) y sentencias Flink que enriquecen, puntúan por rangos y escriben `loan-decisions` con score, decisión y motivos. Umbrales parametrizables.
**Todo List:**
1. Generador mínimo (sin frontend ni Java): enviar N solicitudes sobre `customer_id` existentes y alguna sobre clientes inexistentes (caso borde).
2. Tablas Flink sobre los tópicos según 1.1.
3. Join de enriquecimiento según la estrategia elegida en 1.1.
4. Reglas por rangos (ingresos, ratio de deuda, impagos, importe solicitado) y umbral de aprobación.
5. `INSERT INTO loan-decisions` y despliegue vía `confluent_flink_statement` en Terraform.
6. Nota en la documentación sobre el modo IA futuro, sin implementarlo.
**Relevant Context:** resultados de 1.1.
**Notas de implementación (1.4):**
- Documentación y reglas en `banking-streaming-asset/flink-sql/README.md`. SQL: `10/11` (stats), `20` (tabla `loan-decisions`), `21-*.sql.tftpl` (scoring con umbrales de `var.scoring`).
- Despliegue: módulo `terraform-flink/` con estado propio (4 `confluent_flink_statement`), `make flink-up`. `make destroy` lo borra antes que la infraestructura base. Generador: `scripts/generate-loans.sh` (`make loans`), por `INSERT … VALUES` de Flink sobre el esquema Avro registrado.
- El tópico `loan-decisions` lo crea Flink con su `CREATE TABLE` (no está en `terraform/`).
- Validado en la nube real: 1001/1004 APPROVED, 1002/1003 REJECTED con motivos, cliente inexistente `CUSTOMER_NOT_FOUND`. `flink-check` OK y sin drift en `terraform/`.
- **Riesgo conocido:** reemplazar el INSERT relee `loan-requests` y reemite decisiones; las solicitudes anteriores al agregado de pagos pueden puntuar más alto (ver README). Cambiar umbrales implica este reemplazo.
- Modo IA: solo documentado (columna `model`).
**Status:** [x] done

### 1.5 Smoke test end-to-end
**Intent:** demostrar de forma automática el criterio de éxito de la Fase 1.
**Expected Outcomes:** `scripts/smoke-test.sh` que genera solicitudes, espera y verifica que llegan decisiones coherentes a `loan-decisions` (por ejemplo: un cliente solvente aprobado y uno con impagos rechazado). Prueba adicional de update en `customers`.
**Todo List:**
1. Casos de prueba deterministas basados en los datos de init.
2. Consumo de `loan-decisions` con timeout explícito.
3. Resultado claro de OK/FAIL con código de salida.
**Notas de implementación (1.5):**
- `make smoke` → `scripts/smoke-test.sh`. Solo usa solicitudes de su ejecución (`smoke-<run>-<n>`), sin asumir `loan-decisions` vacío. Comprueba precondiciones (statements `RUNNING`, datos del seed) y valida `request_id`, `customer_id`, importe, plazo, `score`, `decision` y `reasons` exactos. Plazo global `SMOKE_TIMEOUT` (600 s); salidas 0/1/2/3.
- Casos: A 1001 → APPROVED (100); B 1002 → REJECTED (20, con `PAST_DEFAULTS`); C 1001 con 2 000 000 → REJECTED (45, `AMOUNT_TOO_HIGH`); D 999999 → REJECTED (`CUSTOMER_NOT_FOUND`). Scores calculados a mano a partir de las reglas.
- Validado en Confluent Cloud real: 2 ejecuciones consecutivas OK (393 s y 404 s) y una con plazo corto que termina con código 3.
- **Limitación conocida (fuera del MVP):** latencia de decisión de 5-7 min con poco tráfico, y reemisión de decisiones al reemplazar el INSERT. Detalle en `flink-sql/README.md`.
- No incluye aún la prueba adicional de UPDATE en `customers` (ya validada en el spike 1.1); queda como mejora.
**Status:** [x] done

**Gate de Fase 1: PASSED** (aprobado por el usuario).
- `make smoke` ejecutado satisfactoriamente **dos veces** contra Confluent Cloud (393 s y 404 s); además una ejecución con plazo corto terminó con código 3 sin colgarse.
- `make flink-check`: **OK** (customers 50 filas, payment_history 312 filas).
- `terraform/`: **sin drift** (`plan -detailed-exitcode` = 0).
- **2 Flink statements permanentes RUNNING** (`dml-customer-payment-stats`, `dml-loan-decisions`).
- Reemisión de decisiones históricas al reemplazar el INSERT: **known limitation** (fuera del MVP).
- Latencia observada de 5-7 minutos en el INSERT permanente: **known limitation / investigation pending**.
- Prueba adicional de UPDATE en `customers` en el smoke test: fuera del alcance de la Fase 1 (validada en el spike 1.1).

### Issues técnicos abiertos
| ID | Prioridad | Descripción | Condición de cierre |
|---|---|---|---|
| ISSUE-1 | **Alta — bloquea la demo/workshop final** | Latencia de 5-7 min `requested_at → decided_at` del INSERT permanente con poco tráfico. El mismo temporal join lanzado como consulta nueva emite en ~54 s. `scan.idle-timeout` no cambió el resultado en la sonda. | Investigar el avance del watermark del statement permanente y fijar una latencia aceptable antes de considerar el asset listo para demo/workshop. **No bloquea** el desarrollo de la Fase 2. |
| ISSUE-2 | Media (fuera del MVP) | Reemplazar el INSERT relee `loan-requests` y reemite decisiones históricas con el mismo `request_id`. | Consumidores deben quedarse con la última decisión por `request_id`. |

**Regla para la Fase 2:** la UI refleja el comportamiento real del pipeline. Sin decisiones simuladas, sin scoring local y sin atajos; ISSUE-1 no se resuelve en el frontend.

---

# FASE 2 — Experiencia de usuario

### 2.0 Identidades de mínimo privilegio por aplicación (Terraform)
**Intent:** que productor y backends no reutilicen credenciales de infraestructura, Connect ni Flink.
**Notas de implementación (2.0):**
- **Ya implementado y aplicado** en `terraform/main.tf` (bloque aditivo «FASE 2»): service accounts `producer`, `requests-api` y `dashboard-api`, con API key de Kafka y de Schema Registry cada una (`for_each` sobre `local.apps`).
- Permisos por ACL (clúster BASIC no admite role bindings por recurso): `producer` → `WRITE`/`DESCRIBE` en `loan-requests`; `requests_api` → `WRITE`/`DESCRIBE` en `loan-requests`, `READ`/`DESCRIBE` en `loan-decisions` y `READ` en el grupo con prefijo `<project>-requests-api-`; `dashboard_api` → `READ`/`DESCRIBE` en `loan-decisions` y `READ` en el grupo con prefijo `<project>-dashboard-api-`. En Schema Registry, `DeveloperRead` acotado al subject `<topic>-value` de cada app.
- Output sensible `app_credentials`; `scripts/gen-env.sh` lo vuelca a `.env.runtime` (`PRODUCER_*`, `REQUESTS_API_*`, `DASHBOARD_API_*`, `TOPIC_LOAN_*`).
- **Estado verificado tras el ETIMEDOUT:** el apply de Fase 2 quedó completo, `terraform/` sin drift; `flink-up.sh plan` → *No changes*; `make flink-check` OK (customers 50, payment_history 312). No se repitió `make smoke` ni la validación de Fase 1.
- Los permisos de `loan-decisions` dependen de que el tópico exista (lo crea Flink); las ACL son por nombre literal y no lo requieren.
- Comandos externos con plazo duro: macOS no trae `timeout`; usar `perl -e 'alarm N; exec @ARGV' <cmd>`.
**Status:** [x] done

### 2.1 Productor Java definitivo
**Intent:** generar tráfico mock continuo de solicitudes de préstamo, reemplazando el generador mínimo.
**Expected Outcomes:** productor Java (Maven, cliente Kafka, serialización compatible con Schema Registry) con ritmo y `customer_id` configurables por variable, imagen UBI con usuario no root.
**Todo List:**
1. Proyecto Maven con versiones actuales y lockfile equivalente (versiones fijadas).
2. Reutilizar el esquema de `loan-requests` definido en Terraform.
3. Dockerfile UBI, servicio en compose.
**Progreso:** `producer-java/` corregido para ser compatible con el contrato de Flink (el JSON plano anterior NO lo era). Ahora publica **Avro + Schema Registry**: value `KafkaAvroSerializer`, key `StringSerializer` (`String.valueOf(customer_id)`, compatible con `raw`). Reutiliza `terraform/schemas/loan-requests.avsc` (empaquetado como recurso por `pom.xml`, sin copia). `GenericRecord` con `customer_id` int, `amount` decimal(12,2) (rechaza scale ≠ 2), `requested_at` epoch millis. `auto.register.schemas=false`, `use.latest.version=true`. Variables reales de `gen-env.sh`: `KAFKA_BOOTSTRAP_SERVERS`, `PRODUCER_KAFKA_API_KEY/SECRET`, `SCHEMA_REGISTRY_URL`, `PRODUCER_SR_API_KEY/SECRET`, `TOPIC_LOAN_REQUESTS`. Archivos: `pom.xml`, `LoanRequest`, `LoanRequestAvro` (nuevo), `LoanRequestFactory`, `LoanRequestProducerApp`, `LoanRequestFactoryTest`, `README.md`. `mvn test`: PASS (9 tests); SLF4J alineado en 1.7.36. **Dockerfile UBI** multi-etapa persistido (`producer-java/Dockerfile` + `Dockerfile.dockerignore`, contexto = raíz del asset) con `registry.access.redhat.com/ubi9/openjdk-17` y `openjdk-17-runtime` (`registry.redhat.io` exige login, descartado). **Docker image build: PASS** (`localhost/loan-request-producer:0.1.0`, 415 MB, USER 185). **Compose:** servicio `producer-java` añadido a `compose/compose.yaml` (profile `producer`, `restart: "no"`, proceso finito, solo las 7 variables del productor, sin `env_file`, sin dependencia de postgres/connect; el `up -d` por defecto no lo construye ni arranca). Validación estática de compose: PASS.
**Validación runtime: producer-java runtime validation: PASS.** Compose (`--profile producer run --rm --no-deps`) con credenciales propias del productor y una solicitud fija (1001 / 10000.00 / 24 / PERSONAL, vía `LOAN_CUSTOMER_ID` y afines, opcionales): exit 0, `KafkaAvroSerializer` con `auto.register.schemas=false`, publicada en `loan-requests` partition 0 offset 18 y confirmada por lectura acotada filtrada por `request_id` (1 fila). Corrección encontrada: la propiedad es `basic.auth.credentials.source` (con «s»); con la errata el cliente de Schema Registry daba 401. `mvn test` 10 tests PASS. No se esperó `loan-decisions` (ISSUE-1).
**Status:** [x] COMPLETED

### 2.2 Frontend de solicitudes (React Carbon + Express)
**Intent:** crear solicitudes de préstamo manualmente.
**Expected Outcomes:** formulario Carbon y backend Express con validación estricta que produce a `loan-requests`. Solo préstamos en esta fase.
**Progreso:**
- **Checkpoint 1 – Express backend skeleton + strict validation + Avro preparation/encoding + offline tests: PASS.** `frontend-requests/` (Node ESM, deps: `express` 5.3.0, `avsc` 5.7.9; `npm audit --omit=dev`: 0 vulnerabilidades). `POST /api/loan-requests` acepta solo `customer_id`, `amount`, `term_months`, `purpose` (rechaza campos desconocidos, incluidos `request_id`/`requested_at`); `request_id` (UUID) y `requested_at` los genera el servidor. `amount`: >0, precision 12, máx. 2 decimales, sin redondeo, normalizado a scale 2. `term_months` ∈ {12,24,36,48,60,120,240}; `purpose` ∈ {HOME, CAR, EDUCATION, PERSONAL, BUSINESS, DEBT_CONSOLIDATION}. Avro: `src/avro.js` carga directamente `terraform/schemas/loan-requests.avsc` (sin copia) con logical types `decimal(12,2)` y `timestamp-millis`, más helper del wire format de Confluent (sin usarlo contra Schema Registry). Clave del registro = `String(customer_id)`. Publisher inyectable (`publisher.publish({key, loanRequest})`); sin publisher la API responde 503. La API nunca calcula score ni devuelve decisión (solo 202 con `request_id` y `requested_at`). Errores HTTP genéricos; bind fijo a `127.0.0.1`. Tests locales offline (`node --test`, 29 tests): PASS. Sin Kafka, Flink, Terraform, Podman ni Compose.
- **Checkpoint 2 – Kafka + Schema Registry publisher implementation + offline tests: PASS.** Cliente Kafka: `@platformatic/kafka` 2.14.0 (Apache-2.0, release 2026-10-07, Node ≥22.22, JS puro con SASL PLAIN + TLS, producer reutilizable); descartados `kafkajs` (último release 2023-02) y `@confluentinc/kafka-javascript` (binding nativo librdkafka, peor encaje en imagen UBI mínima). Schema Registry: GET de solo lectura a `/subjects/loan-requests-value/versions/latest` (equivale a `auto.register.schemas=false`); comprueba que el esquema registrado coincide con `loan-requests.avsc` por fingerprint y cachea el ID. Wire format Confluent (`0x00` + ID big-endian + Avro) con los helpers de `avro.js`. Key `String(customer_id)`, `acks=all`, `retries=0`, timeouts finitos (sin idempotencia: la ACL de `requests_api` no concede IDEMPOTENT_WRITE). 202 solo tras confirmación del broker; fallos de Kafka/SR → 503 genérico. Variables: `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`, `REQUESTS_API_KAFKA_API_KEY/SECRET`, `REQUESTS_API_SR_API_KEY/SECRET`, `TOPIC_LOAN_REQUESTS` (validadas al arrancar sin imprimir valores). Ciclo de vida: connect → publish → disconnect, shutdown limpio en SIGTERM/SIGINT; bind fijo a `127.0.0.1`. Archivos nuevos: `src/config.js`, `src/schemaRegistry.js`, `src/publisher.js`, `src/runtime.js`, `test/publisher.test.js`, `test/integration.test.js`, `test/helpers.js`; modificado `src/server.js`. `npm test`: 53 tests PASS; `npm audit --omit=dev`: 0 vulnerabilidades. Sin tocar Confluent Cloud, Podman, Compose, Terraform ni Flink.
- **Node requests API runtime validation against Confluent Cloud: PASS.** Backend real arrancado directamente con Node (`node --env-file=../.env --env-file=../.env.runtime src/server.js`, sin Docker/Compose) en `127.0.0.1:3517` con timeout de arranque de 30 s. Logs sin secretos: `kafka_connected` (TLS + SASL PLAIN con `@platformatic/kafka`), `schema_resolved` (`loan-requests-value`, schema ID 100001, `fingerprint_match: true`), `publish_confirmed` (topic `loan-requests`, partition 0, offset 19). Un único `POST /api/loan-requests` (1001 / 10000.00 / 24 / PERSONAL) → HTTP 202 con solo `request_id` y `requested_at` (sin score, decision ni datos internos), emitido únicamente tras el ACK de Kafka. Verificado por lectura Flink acotada (`scan.startup.mode=timestamp`, `scan.bounded.mode=latest-offset`, `--timeout 80`, `--max-rows 5`, filtro por `request_id`): 1 fila con `customer_id=1001`, `amount=10000.00`, `term_months=24`, `purpose=PERSONAL`, `requested_at` válido. SIGTERM → shutdown limpio, sin procesos Node ni puerto abierto, statement ad hoc borrado (GET → 404). No se esperó `loan-decisions`. Cambios de código mínimos para observabilidad: `publish()` devuelve `{topic, partition, offset}`, `connect()` devuelve topic/subject/schemaId, eventos de log `kafka_connected`/`schema_resolved`/`publish_confirmed`, timeout de arranque (30 s) y logger a stdout; `npm test`: 53 PASS.
- **Limitación conocida:** `retries=0` evita reintentos automáticos, pero un fallo de publicación ambiguo (p. ej. timeout tras escribir) seguido de un reintento del cliente podría producir otra solicitud con un `request_id` nuevo. La deduplicación / escritura idempotente queda fuera del MVP.
- **React + Carbon loan request UI + offline tests/build: PASS.** UI en `frontend-requests/web/` (Vite 8.3.5, React 19.3.1, `@carbon/react` 1.120.0, Sass; todo en `devDependencies`). Formulario Carbon (`customer_id`, `amount`, `term_months`, `purpose`) con validación en cliente que replica las reglas del backend; llama a la URL relativa `/api/loan-requests` (sin hosts/puertos hardcodeados). Máquina de estados idle → submitting → success | error; SUCCESS significa solo "solicitud enviada" (muestra `request_id` y `requested_at`); la UI no calcula score, no simula decisión y no espera `loan-decisions`. Errores genéricos (400/503/otros). Tests de UI (vitest + Testing Library, fetch mockeado): 15 PASS; `npm test` = 53 API + 15 UI, todos PASS. `vite build` → `web/dist` (JS 332 kB, CSS ~1.2 MB con fuentes IBM Plex empaquetadas localmente vía `$font-path` de Carbon; 0 avisos de resolución; sin CDNs ni localhost en el bundle). `web/dist` y `node_modules` ignorados por git. Sin Docker, Compose, Confluent ni backend real.
- **Express static serving + UBI Node Dockerfile + offline tests/image build: PASS.** Express sirve `web/dist` en producción (`createApp({staticDir})`; `run()` lo activa si existe `web/dist/index.html`, si no avisa con `static_dir_missing` y sigue como API pura, lo que mantiene tests y desarrollo local). `GET /` → `index.html`; assets con `Cache-Control: immutable` (nombres con hash), `index.html` con `no-cache`, `X-Content-Type-Options: nosniff`; sin dotfiles, sin listado de directorios, sin salida de `web/dist` (path traversal probado). Fallback SPA solo para `GET` sin extensión; `/api` y `/api/*` desconocidos → 404 JSON (nunca HTML); un asset inexistente con extensión → 404 JSON. `GET /health` → `200 {"status":"ok"}` (no consulta Kafka/SR/Flink, no expone configuración). Bind por defecto `127.0.0.1` (HOST configurable en el checkpoint siguiente). Tests: `test/static.test.js` (12) → `npm test` = 65 API + 15 UI, PASS. `frontend-requests/Dockerfile` multi-stage sobre `registry.access.redhat.com/ubi9/nodejs-24-minimal` (Node 24.22.1 LTS, npm 11.19.1; sin `registry.redhat.io`): stage `deps` (`npm ci --omit=dev --ignore-scripts`), stage `build` (`npm ci --ignore-scripts`, `npm test` + `vite build`; la imagen solo se genera si los tests pasan), runtime con `node src/server.js` directamente (sin Vite dev, sin nginx, un proceso). Contexto = raíz del asset; `terraform/schemas/loan-requests.avsc` copiado en build desde su origen a `/app/terraform/schemas/` (sin duplicarlo en el repo). `Dockerfile.dockerignore` en allowlist (sin `.env*`, `.env.runtime`, `*.tfstate*`, `node_modules`, `web/dist` del host). Imagen `loan-request-frontend:0.1.0` construida (275 MB), `USER 1001` (GID 0), `NODE_ENV=production`, sin `.env*`/`tfstate`/`tfvars` en el sistema de ficheros, sin devDependencies (react/vite/@carbon ausentes), sin variables de credenciales en el entorno de la imagen. Arrancada sin configuración: `ConfigError` listando las 7 variables requeridas y exit 1 (sin conectar a nada). Sin Compose, sin Confluent, sin Flink. Nota: el build de Sass muestra avisos de deprecación (`if()`) procedentes de `@carbon/react`; no afectan al resultado.
- **frontend-requests Compose + container networking: PASS.** Validación: `npm test` 72 API + 15 UI PASS; `podman compose config` exit 0 (servicios por defecto: postgres, connect, frontend-requests; `producer-java` queda fuera por su profile); sin `env_file`, sin `depends_on`, sin variables vacías, `HOST=0.0.0.0` solo en el entorno del contenedor. No se arrancó ningún servicio. Cambios: Express: nueva variable `HOST` (`resolveHost`), por defecto `127.0.0.1`; lista cerrada `127.0.0.1` | `0.0.0.0` (cualquier otro valor, incluido vacío, falla al arrancar antes de crear el runtime). `PORT` mantiene su validación (entero 1–65535). `HOST=0.0.0.0` solo se usa dentro del namespace de red del contenedor. Servicio `frontend-requests` en `compose/compose.yaml` (servicio normal, sin profile): `build.context: ..`, `dockerfile: frontend-requests/Dockerfile`, imagen `localhost/loan-request-frontend:0.1.0`, `restart: unless-stopped`; entorno explícito (sin `env_file`): `HOST`, `PORT=8080` y las 7 variables del backend (`KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`, `REQUESTS_API_KAFKA_API_KEY/SECRET`, `REQUESTS_API_SR_API_KEY/SECRET`, `TOPIC_LOAN_REQUESTS`); puerto publicado solo en loopback del host `127.0.0.1:8080:8080`; sin `depends_on` (su dependencia real es Confluent Cloud); healthcheck `GET /health` (interval 15s, timeout 5s, retries 3, start_period 40s) que solo comprueba que Express responde. `producer-java` intacto (`profiles: ["producer"]`). `scripts/local-up.sh` no se modifica: `up -d --build` levanta ahora postgres, connect y frontend-requests (y no producer-java).
- **Integrated frontend-requests container → Confluent Cloud → loan-requests validation: PASS.** Solo se levantó `frontend-requests` (`up -d --build --no-deps`; postgres y connect ya estaban en ejecución y no se tocaron). Contenedor `healthy` a los 5 s, usuario `uid=1001`, puerto publicado `127.0.0.1:8080->8080`. Logs de arranque: `kafka_connected` (topic `loan-requests`), `schema_resolved` (`loan-requests-value`, schema ID 100001, `fingerprint_match: true`), `listening` en `0.0.0.0:8080` (dentro del contenedor). Desde el host: `GET /` → 200 `text/html` (React, título «Loan requests», assets `/assets/index-*.js|css`; el JS responde 200 `text/javascript`, 332 kB); `GET /health` → 200 `{"status":"ok"}`. Un único `POST /api/loan-requests` (1001 / `"10000.00"` / 24 / PERSONAL) → HTTP 202 con solo `request_id` y `requested_at` (sin score, decision, metadata de Kafka/SR ni secretos). Log `publish_confirmed`: `request_id` coincidente, topic `loan-requests`, partition 0, offset 20; 0 patrones sensibles en los logs del contenedor. Verificación acotada con `scripts/flink-sql.sh` (`scan.startup.mode=timestamp` anterior al POST, `scan.bounded.mode=latest-offset`, `--timeout 80`, `--max-rows 5`, filtro por `request_id`): `customer_id=1001`, `amount=10000.00`, `term_months=24`, `purpose=PERSONAL`, `requested_at=2026-11-04 17:50:40.680`. Nota: el SELECT devolvió la fila repetida 3 veces y el `COUNT(*)` acotado devolvió `1` (emitido 2 veces como changelog), es decir, el topic contiene una sola fila para ese `request_id` (evidencia: un solo POST, un solo `publish_confirmed`, un solo offset y `COUNT(*)` final = 1); la causa de la repetición en el SELECT no se investigó (probable relectura de páginas/changelog en la lectura de resultados acotados del script, hipótesis sin verificar). No se esperó `loan-decisions`. Limpieza: `frontend-requests` detenido (SIGTERM → `shutdown` limpio) y eliminado, puerto 8080 libre, postgres/connect siguen `Up`; sentencias Flink ad hoc de esta validación borradas por el script (quedan 4 `adhoc-*` anteriores, todas `COMPLETED`, no creadas aquí). Sin cambios de código.
- **README: DONE.** `frontend-requests/README.md` (en español): propósito y pipeline (no calcula score, no decide, no espera `loan-decisions`), componentes, formulario, validación, variables de entorno (solo nombres), desarrollo local, Docker, Compose, API (ejemplo, significado del 202 y tabla de errores), seguridad, tests (72 API + 15 UI = 87 PASS) y limitaciones conocidas.
- **Resumen de checkpoints finales de 2.2:** Express + validación estricta: PASS · Preparación Avro: PASS · Publisher Kafka + Schema Registry: PASS · Validación runtime del backend: PASS · React + Carbon: PASS · Build contenedor UBI: PASS · Compose/networking: PASS · Integrado contenedor → Confluent Cloud → `loan-requests`: PASS · README: DONE.
- **Nota abierta:** la repetición de filas en el SELECT acotado de Flink (con `COUNT(*)` = 1) no se investigó; la limitación de `retries=0` se mantiene arriba.
**Status:** [x] COMPLETED

### 2.3 Dashboard modo Kafka (React Carbon Charts + Express)
**Intent:** visualizar decisiones en tiempo real en modo Confluent-only. Diseño **simplificado** (asset de demo, no production-grade).
**Expected Outcomes:** `loan-decisions` → consumer Kafka Node → estado en memoria → Express → React + Carbon Charts. El navegador nunca accede a Kafka. Sin SSE/WebSocket (polling HTTP cada 3 s), sin abstracción de fuente de datos, sin diseño anticipado para la Fase 3, sin multi-réplica.
**Diseño simplificado:**
- Backend `frontend-dashboard/` (Express, `@platformatic/kafka` 2.14.0, `avsc`). Credenciales `DASHBOARD_API_*`; `group.id` = `DASHBOARD_API_GROUP_PREFIX` + `dashboard` (una sola instancia). Consume desde `earliest` en cada arranque (sin persistencia, sin autocommit). Schema Registry solo lectura (`loan-decisions-value`, GET por ID; nunca registra).
- Estado en memoria deduplicado por `request_id` (la última decisión recibida gana), máximo 1000 decisiones únicas.
- Endpoints únicos: `GET /health`, `GET /api/summary`, `GET /api/decisions?limit=N` (1–200, por defecto 50). Sin `/api/source` ni `/api/stream`.
- UI (checkpoint B): KPIs (total, aprobadas, rechazadas, tasa de aprobación, score medio, importe total aprobado), donut APPROVED/REJECTED, barras de distribución de score, tabla de últimas decisiones, empty state real. Polling cada 3 s. Sin datos simulados.
- Checkpoints: **A** backend + tests + validación con `loan-decisions` real · **B** React + Carbon Charts + tests/build · **C** Dockerfile UBI + Compose (`127.0.0.1:8081`) + README + validación integrada.
**Notas de implementación (A):**
- Contrato real de `loan-decisions-value` (Schema Registry, solo lectura): subject `loan-decisions-value`, id 100011, versión 2, record `org.apache.flink.avro.generated.record.loan_decisions_value` con `customer_id` int, `amount` bytes decimal(12,2), `term_months` int, `purpose` string, `requested_at` y `decided_at` long timestamp-millis, `score` int, `decision` string, `reasons` string, `dti_ratio` ["null","double"], `model` string. **`request_id` NO está en el value** (es la PK de Flink y viaja en la clave Kafka); por eso el decoder lo extrae de la clave (string raw, o Avro con framing de Confluent) sin necesitar el schema de la clave.
- Código: `src/config.js` (variables `DASHBOARD_API_*`, `TOPIC_LOAN_DECISIONS`), `src/decoder.js` (wire format + Avro + clave), `src/store.js`, `src/consumer.js`, `src/server.js`. Tests: `node --test` 30 PASS (config, decoder, store, API, consumer simulado, ciclo de vida de `run()`).
- **CHECKPOINT A validación real contra Confluent Cloud: PASS.** Backend arrancado directamente con Node (sin Docker/Compose) en `127.0.0.1:3518` con credenciales `DASHBOARD_API_*`. Logs sin secretos: `schema_resolved` (`loan-decisions-value`, id 100011), `consumer_started`, `listening`. Tras ~50 s: `GET /health` → 200; `GET /api/summary` → 34 decisiones únicas (14 APPROVED, 20 REJECTED, tasa 0,41, score medio 52,1, importe aprobado 95000.00, distribución de score coherente); `GET /api/decisions?limit=3` → decisiones reales con `request_id` (extraído de la clave) y todos los campos del contrato, incluida la solicitud de la validación de 2.2 (`faac37d6-…`, 1001 / 10000.00 / 24 / PERSONAL, APPROVED, score 100). 0 `decision_skipped`/`consumer_failed` (todos los mensajes decodificados). SIGTERM → `shutdown` limpio, sin procesos ni puerto abierto, 0 secretos en logs. Solo lectura: no se registró ni modificó ningún esquema; no se ejecutó Terraform, Flink, Docker ni Compose. El consumer usa el grupo `<prefijo>dashboard` (ACL `READ` por prefijo) sin autocommit.
- **CHECKPOINT B — React + Carbon Charts dashboard: PASS.** UI en `frontend-dashboard/web/` (Vite + React + Carbon): `App`, `useDashboardData` (polling 3 s a `/api/summary` y `/api/decisions?limit=50`, conserva últimos datos y muestra aviso discreto si falla), `Kpis` (6 KPIs), `Charts` (donut Approved/Rejected + barras de distribución de score), `DecisionsTable` (8 columnas). Estado vacío exacto «No decisions received yet». `npm test`: 30 API + 9 UI PASS (Carbon Charts mockeado en jsdom; se verifican los datos que reciben donut y barras). `npm run build`: OK (JS 687 kB / 212 kB gzip; solo aviso informativo de tamaño de chunk). Sin Docker, Compose ni Confluent Cloud en este checkpoint.
- **CHECKPOINT C — contenedor, Compose, README, `make up`: PASS.**
  - `frontend-dashboard/Dockerfile` + `Dockerfile.dockerignore` (allowlist), mismo patrón que `frontend-requests`: UBI 9 Node 24 (`registry.access.redhat.com`), multi-stage (deps de producción / build con `npm test` + `vite build` / runtime), `USER 1001`, sin Vite ni nginx en runtime. `podman build -f frontend-dashboard/Dockerfile -t loan-decision-dashboard:0.1.0 .` OK. Imagen: usuario 1001, sin `.env*`/`tfstate`, sin variables sensibles, 0 coincidencias de los valores de `DASHBOARD_API_*` en las capas, sin devDependencies (`vite`, `react`, `@carbon/*` ausentes de `node_modules`).
  - Servicio `frontend-dashboard` en `compose/compose.yaml` (normal, sin profile, sin `env_file`, sin `depends_on`). Solo recibe `HOST=0.0.0.0`, `PORT=8081`, `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`, `DASHBOARD_API_*` (5) y `TOPIC_LOAN_DECISIONS`. Puerto `127.0.0.1:8081:8081`. Healthcheck `GET /health` (solo Express; timeouts finitos). `compose config` válido.
  - **Runtime contra `loan-decisions` real: PASS.** Levantado solo `frontend-dashboard`: contenedor `healthy`; `/health` → `{"status":"ok"}`; `/` → HTML del dashboard + assets JS/CSS 200; `/api/summary` → 34 decisiones reales (14 APPROVED, 20 REJECTED, tasa 0,41, score medio 52,1, importe aprobado 95000.00); `/api/decisions` → decisiones reales (incluida `faac37d6-…` de 2.2). Puerto no accesible desde la IP LAN (solo loopback). Contenedor detenido y eliminado tras validar.
  - **`make up`** = `cloud-up` → `local-up` → `flink-up`. `local-up` ya hacía `compose up -d --build` sin lista de servicios, así que incluye `frontend-dashboard` (postgres, connect, `:8080`, `:8081`) sin cambios; `flink-up` se añade al final (necesita el conector CDC en RUNNING) para que el flujo `loan-requests` → Flink → `loan-decisions` esté desplegado. `local-up.sh` imprime las URLs 8080/8081. Verificado con `make -n up` (no se ejecutó `make up`).
  - README: `frontend-dashboard/README.md` (DONE).
  - Resumen: backend consumer PASS · React + Carbon Charts PASS · UBI container PASS · Compose PASS · dashboard runtime against loan-decisions PASS · README DONE.
  - Nota (ISSUE-1): una decisión nueva puede tardar varios minutos; el dashboard muestra el estado vacío real o las decisiones existentes, nunca datos simulados.
**Status:** [x] COMPLETED (A · B · C)

**Gate de Fase 2:** revisión del usuario.

**PHASE 2 DEMO GATE: PASSED** — `make up` → Loan Request UI → Confluent → Flink → Dashboard validado end-to-end con el comando real (no `make -n`).
- `make check`: PASS (tras `make down` de los contenedores locales que dejó la sesión anterior: ocupaban 5432/8083).
- `make up` (= `cloud-up` → `local-up` → `flink-up`): PASS, EXIT=0, **59 s** (Terraform sin cambios, imágenes en caché). `:8080` y `:8081` responden `/health` 200 a los **~24 s**. Tras el up: frontend-requests y frontend-dashboard `healthy`; postgres y connect `Up` (Compose no define healthcheck para ellos; conector `RUNNING`/`RUNNING`); sentencias Flink `dml-loan-decisions` y `dml-customer-payment-stats` `RUNNING` (los DDL, `COMPLETED`).
- Flujo: `POST :8080/api/loan-requests` (1001 / 10000.00 / 24 / PERSONAL) → `202`, `request_id` `6125ce42-e476-44c6-801d-ce0ceb8d5ab4`, `publish_confirmed` en `loan-requests` partición 0 offset 21 (≈1,2 s). Decisión real de Flink: **APPROVED, score 100, MEETS_CRITERIA**, `decided_at` 19:17:56.977Z, es decir **≈5 min 11 s (311 s)** tras el POST (ISSUE-1, sin tocar). El `request_id` aparece en `GET :8081/api/decisions?limit=50` y `/api/summary` pasó de 34 a 35 decisiones (14→15 aprobadas, 20 rechazadas).
- Tiempo hasta que aparece en el dashboard: **no medido con precisión** (mis esperas de polling terminaron antes de que llegara la decisión y la comprobación definitiva se hizo al cabo de ~23 min). El consumer del dashboard solo añade segundos a la latencia de Flink; para la demo contar ≈5-6 min desde el POST.
- Cambios mínimos que reveló `make up` (los dos son defectos reales de arranque en frío, no rediseño):
  1. `connect/connector-postgres.json`: `snapshot.mode` `initial` → `when_needed`. Con un offset de Connect antiguo, el LSN ya no estaba disponible en Postgres recreado y el conector quedaba `FAILED` en `local-up`.
  2. `scripts/flink-up.sh` (y, por coherencia, `generate-loans.sh`, `smoke-test.sh`): `chmod +x`. El Makefile los invoca directamente y daban `Permission denied` (`make up` fallaba en `flink-up`).
- Entorno conservado (sin `make destroy`).
**LOAN REQUEST UI DECISION FEEDBACK = BEST EFFORT** (funcional; la visibilidad de la decisión puede tardar, ver «Estado real»).
- Tras el envío la UI muestra «Processing application...», hace polling cada 2 s (máx. 90 s) a `GET /api/loan-requests/:request_id/decision` y presenta `Loan approved` / `Loan rejected` con score y motivos reales leídos de `loan-decisions` (Express con las credenciales existentes de `requests_api`; sin scoring local, sin ACLs ni service accounts nuevos, sin tocar `frontend-dashboard`). Tests `frontend-requests`: 83 API + 21 UI = 104 PASS. `compose.yaml` pasa ahora `TOPIC_LOAN_DECISIONS` y `REQUESTS_API_GROUP_PREFIX`.
- Caso A (1001): **APPROVED, score 100, MEETS_CRITERIA**. Caso B (1002): **REJECTED, score 20, DTI_TOO_HIGH,PAST_DEFAULTS,FREQUENT_LATE_PAYMENTS,SCORE_BELOW_THRESHOLD**. Ambos `request_id` aparecen en `:8081/api/decisions`.
- ISSUE-1 sigue **RESUELTO PARA LA DEMO**: `sql.tables.scan.idle-timeout = '5 s'` configurado en `dml-loan-decisions` (`decided_at` ≈ 5-7 s tras `requested_at`).
- Estado real: Flink genera la decisión normalmente en ~5-7 s (`sql.tables.scan.idle-timeout = '5 s'`), pero la visibilidad para consumers `read_committed` puede tardar más (medido ~15-57 s tras el POST; UI y dashboard la ven a la vez). La UI espera hasta **90 s** y, si no llega, muestra «The application is still being processed. Check the dashboard for updates.».
- `READ_UNCOMMITTED` en el consumer de `frontend-requests` se probó, no resolvió la latencia de forma consistente (1001: 10,8 s; 1002: 52,7 s) y **se revirtió** (vuelve `read_committed`). La causa de la demora de visibilidad no se investiga más. `frontend-dashboard` no se tocó.
- ISSUE-1 sigue **RESOLVED FOR DEMO** respecto a la latencia de procesamiento de Flink.
**DEMO SIMPLIFICATION (supersede el feedback de decisión en :8080)**
- `frontend-requests` (:8080) vuelve a ser solo generador de solicitudes: sin polling ni endpoint `GET .../decision`, sin consumer de `loan-decisions` (eliminados `src/decisions.js` y las variables `TOPIC_LOAN_DECISIONS`/`REQUESTS_API_GROUP_PREFIX` del servicio). Tras el POST muestra «Loan request submitted», `request_id`, `requested_at`, aviso de dashboard y botón «Open Dashboard» (`http://127.0.0.1:8081`). Tests: 72 API + 19 UI. ACLs/Terraform sin tocar.
- `producer-java` modo `DEMO_TRAFFIC=true` (1 solicitud cada `DEMO_TRAFFIC_INTERVAL_SECONDS`, 10 por defecto; clientes 1001–1050; mismo Avro). Servicio compose `producer-demo-traffic` (profile `demo-traffic`), `make demo-traffic` / `make demo-traffic-stop`; `make up` no lo inicia; `make down`/`destroy` lo eliminan. `README.md` raíz documenta `make up`, `demo-traffic`, `demo-traffic-stop`, `destroy`.
- Verificado: modo normal (POST → 202, `/decision` → 404); demo-traffic: 7 envíos en ~64 s, sin `request_id` repetidos; `demo-traffic-stop` elimina el contenedor.




---

# FASE 3 — Capa analítica opcional (watsonx.data)

### 3.1 Tableflow + Iceberg REST Catalog (Terraform condicional)
**Intent:** exponer `loan-decisions` como tabla Iceberg sin afectar al flujo principal.
**Expected Outcomes:** con `enable_watsonxdata=true`, `confluent_tableflow_topic` con almacenamiento gestionado por Confluent y outputs del endpoint del catálogo Iceberg REST. Con `false`, no se crea nada.
**Todo List:**
1. Recursos condicionales (`count`) y credenciales necesarias.
2. Verificar disponibilidad de Tableflow en la región/cluster elegidos.
3. Documentar cómo registrar el catálogo en watsonx.data (paso manual en la instancia existente).
**Status:** [ ] pending

### 3.2 Dashboard modo Presto
**Intent:** segunda implementación de la capa de datos del dashboard.
**Expected Outcomes:** con `ENABLE_WATSONXDATA=true` el backend consulta Presto (consultas parametrizadas, TLS, credenciales por variable). Con `false`, sigue en modo Kafka.
**Status:** [ ] pending

**Gate de Fase 3:** revisión del usuario. Un fallo en esta fase no invalida las anteriores.

---

# FASE 4 — Packaging del asset

### 4.1 `make check` completo
**Intent:** validar el entorno sin crear infraestructura.
**Expected Outcomes:** `make check` comprueba como mínimo, y devuelve error con mensajes accionables:
- Terraform disponible.
- Docker o Podman disponible.
- Compose disponible.
- Credenciales/configuración requerida presente.
- Variables obligatorias configuradas.
- Puertos locales requeridos libres.
- `ENABLE_WATSONXDATA` con valor válido (`true`/`false`).
- Si `ENABLE_WATSONXDATA=true`: variables de watsonx.data (host, puerto, credenciales, catálogo, esquema).
No ejecuta `terraform apply` ni levanta contenedores.
**Status:** [ ] pending

### 4.2 Orquestación `make up` / `make down` / `make destroy`
**Expected Outcomes:** `make up` ejecuta check, Terraform, genera `.env`, levanta compose, registra el conector y siembra datos. `make down` limpia lo local. `make destroy` elimina cloud y local. Compose con perfiles y healthchecks. Funciona con Docker y Podman.
**Status:** [ ] pending

### 4.3 README y laboratorio HTML
**Expected Outcomes:**
- `banking-streaming-asset/README.md` técnico en castellano.
- `laboratorio_banca_streaming.html` fuera del zip, con el estilo de `laboratorio_confluent_cloud.html`: descripción del flujo, descarga del zip, variables del `.env`, comandos, verificación en la UI de Confluent, modo con/sin watsonx.data y nota del modo IA.
- Tarjeta en `index.html` (patrón en líneas 121–150) y línea en el `README.md` de la raíz.
**Status:** [ ] pending

### 4.4 ZIP final
**Expected Outcomes:** procedimiento (objetivo `make zip` o script) que genera el zip excluyendo el HTML del laboratorio, secretos, `node_modules`, `target` y estado de Terraform.
**Status:** [ ] pending

---

# FASE 5 — Extensión V2: `payments` (backlog, fuera del alcance inicial)
- Tópico `payments`, generación en el productor Java y en el frontend de solicitudes.
- Definir cómo los pagos nuevos afectan al historial o al scoring de decisiones futuras (p. ej. escribir en Postgres vía sink, o agregación en Flink).
- Reflejarlo en el dashboard.
**Status:** [ ] pending (no se implementa en la primera versión)

---

## Riesgos y puntos abiertos
- **CDC → Flink** es el riesgo principal. Se aborda en el spike 1.1, que fija las decisiones de formato, keys, changelog y join.
- **Imagen de Debezium/Connect:** imagen propia sobre UBI mínima con el plugin. Si es inviable, documentar la excepción.
- **Disponibilidad:** Flink y Tableflow en AWS dependen de región y tipo de cluster. Validar con el provider.
- **Costes:** Confluent Cloud factura mientras existan los recursos. Documentar `make destroy`.
- **Validación:** sin credenciales de Confluent Cloud no se puede probar el despliegue real desde este entorno de planificación. Durante la implementación el spike 1.1 y el smoke test 1.5 requieren una cuenta real. Se validarán además `terraform validate`/`fmt`, builds de contenedores y linters.
