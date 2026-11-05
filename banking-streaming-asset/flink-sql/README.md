# Flink SQL — scoring de préstamos (tarea 1.4)

## Flujo
```
loan-requests ─┐                                        ┌─> loan-decisions
customers (CDC) ─ temporal join ─> score por rangos ────┘
payment_history (CDC) ─> customer_payment_stats ─ temporal join ┘
```

| Fichero | Qué hace |
|---|---|
| `00-primary-keys.sql` | PK (`NOT ENFORCED`) de las tablas CDC; necesaria para los temporal joins |
| `10-table-customer-payment-stats.sql` | Tabla upsert con el agregado de pagos por cliente |
| `11-insert-customer-payment-stats.sql` | Flujo continuo que mantiene ese agregado |
| `20-table-loan-decisions.sql` | Tabla/tópico `loan-decisions` (Flink crea el tópico y el esquema Avro) |
| `21-insert-loan-decisions.sql.tftpl` | Scoring. Plantilla: los umbrales salen de `var.scoring` |
| `spike/` | SQL de referencia del spike 1.1 (no se despliega) |

## Despliegue
`make flink-up` → `scripts/flink-up.sh`: declara las PKs (idempotente) y aplica `terraform-flink/`, un módulo con **estado propio**
(`confluent_flink_statement` × 4). No toca `terraform/` (infraestructura base). `make destroy` borra primero ese módulo.
Generar solicitudes: `make loans ARGS="-n 5 -m 1"` o `-r 1001:10000:24` (cliente:importe:plazo).
Ver el SQL renderizado: `bash scripts/render-decisions-sql.sh`.

## Reglas de decisión (RULES_V1)
Puntos máximos 100. Umbrales por defecto en `terraform/variables.tf` (`scoring`).

| Criterio | Rangos (por defecto) | Puntos |
|---|---|---|
| DTI = (deuda mensual + importe/plazo) / ingresos | ≤0,30 · ≤0,40 · ≤0,50 · mayor | 40 · 25 · 10 · 0 |
| Historial | impagos > 0 · sin retrasos · ≤2 retrasos · más | 0 · 30 · 20 · 8 |
| Antigüedad laboral | ≥5 · ≥3 · ≥1 años · menos | 15 · 10 · 5 · 0 |
| Importe / ingresos anuales | ≤0,5 · ≤1,0 · mayor | 15 · 8 · 0 |

`APPROVED` si puntos ≥ 60 **y** no hay impagos. Si no, `REJECTED` con `reasons` (lista separada por comas):
`CUSTOMER_NOT_FOUND`, `DTI_TOO_HIGH`, `PAST_DEFAULTS`, `FREQUENT_LATE_PAYMENTS`, `AMOUNT_TOO_HIGH`, `SHORT_EMPLOYMENT`, `SCORE_BELOW_THRESHOLD`; `MEETS_CRITERIA` si se aprueba.
Cliente inexistente: score 0 y `CUSTOMER_NOT_FOUND`.

### Cambiar umbrales
Editar `scoring` (p. ej. `-var` o un `.tfvars` ignorado por git) y `make flink-up`. **Un `statement` de Flink no se modifica en caliente:**
Terraform reemplaza el INSERT, que relee `loan-requests` desde el principio y **reemite** decisiones con los mismos `request_id`.

## Riesgo conocido: replay y tiempo de evento
El temporal join usa la versión de cliente/estadísticas vigente en el `$rowtime` de la solicitud. Al reemitir solicitudes antiguas:
- si la tabla `customer_payment_stats` es más reciente que la solicitud, no hay versión vigente → se asumen 0 impagos/retrasos y el score sube.
  Caso real observado: `t1-…-2` (cliente 1002) pasó de score 20 (`PAST_DEFAULTS`) a 50 al relanzar el INSERT.
- Las decisiones nuevas (solicitudes posteriores al agregado) son correctas.
Mitigación en producción: no reemplazar el INSERT salvo necesidad, y consumir `loan-decisions` por `request_id` (la última decisión gana).
Pendiente para una fase posterior: derivar `late/defaulted` sin depender del orden temporal (p. ej. regular join o evaluar con la versión más reciente).

## Limitaciones conocidas (fuera del MVP)
- **Reemisión al reemplazar el INSERT** (ver arriba): se acepta y no se resuelve en el MVP. Cambiar umbrales relee `loan-requests` y reemite decisiones históricas.
- **ISSUE-1 (latencia de decisión) — RESUELTO PARA LA DEMO.**
  - Antes: ~311-315 s típicos con poco tráfico (`requested_at` → `decided_at`; 314 s en smoke, 38 s con actividad previa).
    El INSERT permanente esperaba el watermark de las tablas versionadas (`customers`, `customer_payment_stats`), cuyas particiones quedaban sin tráfico.
  - Configuración aplicada: `sql.tables.scan.idle-timeout = '5 s'` solo en el statement `dml-loan-decisions` (`terraform-flink/main.tf`, `merge(local.props, …)`).
  - Después, tras ≥60 s de reposo: 7,2 s y 5,7 s (dos solicitudes, cliente 1001).
  - El mecanismo interno exacto de Flink no se ha demostrado; la propiedad se adoptó por la evidencia de la medición.
  - Cambiar `properties` de un statement existente exige reemplazarlo (`terraform taint confluent_flink_statement.decisions_insert` y `make flink-up`), con la reemisión descrita arriba.

## Smoke test (1.5)
`make smoke` → `scripts/smoke-test.sh`. Plazo global `SMOKE_TIMEOUT` (600 s por defecto); código de salida 0 OK · 1 validación fallida · 2 precondición · 3 plazo agotado.
Solo usa solicitudes de su ejecución (`smoke-<run>-<n>`) y lee `loan-decisions` filtrando por ese prefijo, sin asumir tópico vacío.
Precondiciones: statements permanentes `RUNNING` y datos del seed (1001 solvente, 1002 con 5 retrasos y 3 impagos, 999999 inexistente).

| Caso | Entrada | Esperado |
|---|---|---|
| A | 1001 · 5 000 · 24 m | score 100, APPROVED, `MEETS_CRITERIA` |
| B | 1002 · 1 000 · 12 m | score 20, REJECTED, `DTI_TOO_HIGH, PAST_DEFAULTS, FREQUENT_LATE_PAYMENTS, SCORE_BELOW_THRESHOLD` |
| C | 1001 · 2 000 000 · 12 m | score 45, REJECTED, `DTI_TOO_HIGH, AMOUNT_TOO_HIGH, SCORE_BELOW_THRESHOLD` |
| D | 999999 · 5 000 · 24 m | score 0, REJECTED, `CUSTOMER_NOT_FOUND` |

Valida por caso `request_id`, `customer_id`, importe, plazo, `score`, `decision` y `reasons` (exactos). Si un `request_id` aparece con valores distintos, falla.
Si cambias `var.scoring`, actualiza los scores esperados en el script.

## Estado y pruebas (validado con Confluent Cloud real)
| Solicitud | Score | Decisión | Motivos |
|---|---|---|---|
| 1001 · 10 000 · 24 m | 100 | APPROVED | MEETS_CRITERIA |
| 1002 · 3 000 · 12 m | 20 | REJECTED | DTI_TOO_HIGH, PAST_DEFAULTS, FREQUENT_LATE_PAYMENTS, SCORE_BELOW_THRESHOLD |
| 1003 · 60 000 · 12 m | 30–40 | REJECTED | DTI_TOO_HIGH, AMOUNT_TOO_HIGH, SCORE_BELOW_THRESHOLD |
| 1004 · 5 000 · 24 m | 100 | APPROVED | MEETS_CRITERIA |
| cliente 9xxxx | 0 | REJECTED | CUSTOMER_NOT_FOUND |

## Modo IA (futuro, no implementado)
El contrato de `loan-decisions` incluye `model` (hoy `RULES_V1`). Un modo con IA podría escribir en el mismo tópico con otro valor
(p. ej. `LLM_V1`) y su propio `reasons`, o enriquecer `reasons` en un segundo statement, sin cambiar el esquema ni a los consumidores.
