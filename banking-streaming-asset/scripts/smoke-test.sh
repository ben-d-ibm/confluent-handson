#!/usr/bin/env bash
# Smoke test end-to-end (Fase 1): loan-requests -> Flink (enriquecimiento + scoring) -> loan-decisions.
#
# Garantías:
#  - Trabaja SOLO con las solicitudes de esta ejecución (request_id único: smoke-<run>-<n>). Ignora cualquier
#    decisión histórica y no asume que loan-decisions esté vacío.
#  - Casos deterministas sobre el seed (1001 solvente, 1002 moroso) y un cliente inexistente (999999).
#  - Antes de enviar nada comprueba las precondiciones de los datos y que los statements de Flink estén RUNNING.
#  - Plazo global (SMOKE_TIMEOUT, 600 s por defecto; la latencia observada es de 5-7 min, ver flink-sql/README.md): nunca espera indefinidamente.
#  - Valida por caso: request_id, customer_id, amount, term_months, score, decision y reasons (exactos).
#
# Códigos de salida: 0 OK · 1 alguna validación falló · 2 precondición no cumplida · 3 plazo agotado sin recibir todas las decisiones.
#   scripts/smoke-test.sh            (o make smoke)
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
[[ -f "${RUNTIME_ENV_FILE}" ]] || die "Falta .env.runtime (make cloud-up)."
set -a; source "${RUNTIME_ENV_FILE}"; set +a

TOTAL="${SMOKE_TIMEOUT:-600}"
[[ "${TOTAL}" =~ ^[0-9]+$ ]] && (( TOTAL >= 60 )) || die "SMOKE_TIMEOUT debe ser un entero >= 60."
DEADLINE=$(( SECONDS + TOTAL ))
SQL="${ROOT_DIR}/scripts/flink-sql.sh"

# Ejecuta una consulta acotada respetando el plazo global. Devuelve 124 si no queda tiempo.
run_sql() {
  local rem=$(( DEADLINE - SECONDS ))
  (( rem > 5 )) || return 124
  bash "${SQL}" --timeout $(( rem < 90 ? rem : 90 )) --max-rows 5000 "$1" 2>/dev/null || true
}
# Último valor de una columna en un changelog (filas +I/+U), por clave en la columna 0.
latest() { jq -rs '[.[] | select(.row and (.op==0 or .op==2))] | group_by(.row[0]) | map(last)'; }

# ---------------------------------------------------------------- 1. Precondiciones
info "== 1/4 Precondiciones =="
base="${FLINK_REST_ENDPOINT}/sql/v1/organizations/${CONFLUENT_ORG_ID}/environments/${CONFLUENT_ENV_ID}"
phases="$(curl -sS --connect-timeout 10 --max-time 30 -K <(printf 'user = "%s"\n' "${FLINK_API_KEY}:${FLINK_API_SECRET}") \
  "${base}/statements?page_size=100" | jq -r '.data[]? | select(.status.phase=="RUNNING") | .name')" || true
for s in dml-loan-decisions dml-customer-payment-stats; do
  grep -q -- "-${s}$" <<<"${phases}" || { fail "El statement permanente '*-${s}' no está RUNNING (make flink-up)."; exit 2; }
done
ok "Statements permanentes de Flink en RUNNING"

cust="$(run_sql "SELECT customer_id, monthly_income, monthly_debt, employment_years FROM customers /*+ OPTIONS('scan.bounded.mode'='latest-offset') */ WHERE customer_id IN (1001, 1002, 999999)" | latest)"
stats="$(run_sql "SELECT customer_id, late_payments, defaulted_payments FROM customer_payment_stats /*+ OPTIONS('scan.bounded.mode'='latest-offset') */ WHERE customer_id IN (1001, 1002)" | latest)"
check_pre() { jq -e "$1" >/dev/null <<<"$2" || { fail "Precondición de datos no cumplida: $3"; exit 2; }; }
check_pre '[.[]|select(.row[0]=="1001" and (.row[1]|tonumber)==8000 and (.row[2]|tonumber)==500 and (.row[3]|tonumber)==10)]|length==1' "${cust}" "cliente 1001 (ingresos 8000, deuda 500, 10 años)"
check_pre '[.[]|select(.row[0]=="1002" and (.row[1]|tonumber)==2200 and (.row[2]|tonumber)==1800 and (.row[3]|tonumber)==1)]|length==1' "${cust}" "cliente 1002 (ingresos 2200, deuda 1800, 1 año)"
check_pre '[.[]|select(.row[0]=="999999")]|length==0' "${cust}" "el cliente 999999 NO debe existir"
check_pre '[.[]|select(.row[0]=="1001" and (.row[1]|tonumber)==0 and (.row[2]|tonumber)==0)]|length==1' "${stats}" "1001 sin retrasos ni impagos"
check_pre '[.[]|select(.row[0]=="1002" and (.row[1]|tonumber)==5 and (.row[2]|tonumber)==3)]|length==1' "${stats}" "1002 con 5 retrasos y 3 impagos"
ok "Datos del seed conformes (1001 solvente, 1002 moroso, 999999 inexistente)"

# ---------------------------------------------------------------- 2. Envío
RUN="$(date +%s)${RANDOM}"
PREFIX="smoke"
START_MS=$(( ($(date +%s) - 120) * 1000 ))   # margen por desfase de relojes; la correlación es por request_id
info "== 2/4 Envío de solicitudes (run ${RUN}) =="
# Orden = sufijo del request_id: -1 A, -2 B, -3 C, -4 D
GEN_PREFIX="${PREFIX}" GEN_RUN="${RUN}" bash "${ROOT_DIR}/scripts/generate-loans.sh" \
  -r 1001:5000:24 -r 1002:1000:12 -r 1001:2000000:12 -r 999999:5000:24 >/dev/null \
  || { fail "No se pudieron enviar las solicitudes."; exit 1; }
ok "4 solicitudes enviadas: ${PREFIX}-${RUN}-1 .. ${PREFIX}-${RUN}-4"

EXPECTED="$(jq -n --arg p "${PREFIX}-${RUN}" '[
  {case:"A (solvente)",           id:($p+"-1"), customer:1001,   amount:5000,    term:24, score:100, decision:"APPROVED", reasons:["MEETS_CRITERIA"]},
  {case:"B (con impagos)",        id:($p+"-2"), customer:1002,   amount:1000,    term:12, score:20,  decision:"REJECTED", reasons:["DTI_TOO_HIGH","PAST_DEFAULTS","FREQUENT_LATE_PAYMENTS","SCORE_BELOW_THRESHOLD"]},
  {case:"C (importe excesivo)",   id:($p+"-3"), customer:1001,   amount:2000000, term:12, score:45,  decision:"REJECTED", reasons:["DTI_TOO_HIGH","AMOUNT_TOO_HIGH","SCORE_BELOW_THRESHOLD"]},
  {case:"D (cliente inexistente)",id:($p+"-4"), customer:999999, amount:5000,    term:24, score:0,   decision:"REJECTED", reasons:["CUSTOMER_NOT_FOUND"]}
]')"

# ---------------------------------------------------------------- 3. Espera acotada
info "== 3/4 Esperando decisiones (plazo restante $(( DEADLINE - SECONDS )) s) =="
GOT="[]"; n=0
while (( SECONDS < DEADLINE )); do
  out="$(run_sql "SELECT request_id, customer_id, amount, term_months, score, decision, reasons, model FROM \`loan-decisions\` /*+ OPTIONS('scan.startup.mode'='timestamp','scan.startup.timestamp-millis'='${START_MS}','scan.bounded.mode'='latest-offset') */ WHERE request_id LIKE '${PREFIX}-${RUN}-%'")"
  GOT="$(jq -s '[.[] | select(.row and (.op==0 or .op==2)) | .row]' <<<"${out}")"
  n="$(jq '[.[][0]] | unique | length' <<<"${GOT}")"
  info "  decisiones recibidas de este run: ${n}/4"
  (( n >= 4 )) && break
  sleep 5
done
if (( n < 4 )); then
  fail "Plazo de ${TOTAL} s agotado: solo ${n}/4 decisiones de este run (${PREFIX}-${RUN}-*)."
  jq -r '.[] | "  recibida: \(.[0])"' <<<"${GOT}" >&2
  exit 3
fi

# ---------------------------------------------------------------- 4. Validación
info "== 4/4 Validación =="
rc=0
while IFS= read -r exp; do
  id="$(jq -r .id <<<"${exp}")"; name="$(jq -r .case <<<"${exp}")"
  # Todas las emisiones del mismo request_id deben coincidir (sin decisiones contradictorias)
  rows="$(jq -c --arg id "${id}" '[.[] | select(.[0]==$id)] | unique' <<<"${GOT}")"
  if [[ "$(jq length <<<"${rows}")" != "1" ]]; then
    fail "Caso ${name}: ${id} tiene $(jq length <<<"${rows}") decisiones distintas: ${rows}"; rc=1; continue
  fi
  errs="$(jq -r --argjson e "${exp}" '.[0] as $r | [
      (if $r[0] != $e.id then "request_id \($r[0]) != \($e.id)" else empty end),
      (if ($r[1]|tonumber) != $e.customer then "customer_id \($r[1]) != \($e.customer)" else empty end),
      (if ($r[2]|tonumber) != $e.amount then "amount \($r[2]) != \($e.amount)" else empty end),
      (if ($r[3]|tonumber) != $e.term then "term_months \($r[3]) != \($e.term)" else empty end),
      (if ($r[4]|tonumber) != $e.score then "score \($r[4]) != \($e.score)" else empty end),
      (if $r[5] != $e.decision then "decision \($r[5]) != \($e.decision)" else empty end),
      (if ($r[6]|split(",")|sort) != ($e.reasons|sort) then "reasons [\($r[6])] != [\($e.reasons|join(","))]" else empty end)
    ] | .[]' <<<"${rows}")"
  got="$(jq -r '.[0] | "score=\(.[4]) decision=\(.[5]) reasons=\(.[6])"' <<<"${rows}")"
  if [[ -z "${errs}" ]]; then
    ok "Caso ${name}: ${id} · cliente $(jq -r .customer <<<"${exp}") · ${got}"
  else
    fail "Caso ${name}: ${id} · ${got}"; sed 's/^/        /' <<<"${errs}" >&2; rc=1
  fi
done < <(jq -c '.[]' <<<"${EXPECTED}")

if (( rc == 0 )); then ok "SMOKE TEST OK (run ${RUN}, $(( TOTAL - (DEADLINE - SECONDS) )) s)"; else fail "SMOKE TEST FALLIDO (run ${RUN})"; fi
exit "${rc}"
