#!/usr/bin/env bash
# Ejecuta una sentencia de Flink SQL en Confluent Cloud por REST y devuelve el resultado.
#   scripts/flink-sql.sh "SELECT ..."            sentencia en línea
#   scripts/flink-sql.sh -f fichero.sql          sentencia desde fichero
#   scripts/flink-sql.sh --no-wait "INSERT ..."  lanza una sentencia continua y no espera resultados
#   --timeout N   segundos máximos leyendo resultados (por defecto 60; FLINK_SQL_TIMEOUT)
#   --max-rows N  corta tras N filas (por defecto 200)
#   --prop K=V    propiedad de la sentencia Flink (repetible), p. ej. --prop sql.tables.scan.idle-timeout=30s
# Salida: filas en JSON (una por línea). Mensajes de estado por stderr.
# Garantías: nunca espera indefinidamente (plazo global) y borra la sentencia ad hoc al terminar.
# Códigos de salida: 0 ok · 1 error de Flink/API · 2 uso incorrecto · 3 plazo agotado sin completar.
# Nota: las consultas agregadas sobre tablas CDC (COUNT, SUM...) son streaming y NO terminan;
# con ellas el script devuelve lo recibido hasta el plazo (código 3). Para comprobaciones usa
# consultas acotadas (LIMIT, SHOW ..., DESCRIBE ...) o 'make flink-check'.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
[[ -f "${RUNTIME_ENV_FILE}" ]] || die "Falta .env.runtime (make cloud-up)."
set -a; source "${RUNTIME_ENV_FILE}"; set +a

WAIT=1; STMT=""; TIMEOUT="${FLINK_SQL_TIMEOUT:-60}"; MAX_ROWS=200; PROPS='{}'
while [[ $# -gt 0 ]]; do
  case "$1" in
    -f) STMT="$(cat "$2")"; shift 2 ;;
    --no-wait) WAIT=0; shift ;;
    --timeout) TIMEOUT="$2"; shift 2 ;;
    --max-rows) MAX_ROWS="$2"; shift 2 ;;
    --prop) [[ "$2" == *=* ]] || { fail "--prop espera K=V"; exit 2; }
            PROPS="$(jq -c --arg k "${2%%=*}" --arg v "${2#*=}" '. + {($k):$v}' <<<"${PROPS}")"; shift 2 ;;
    *) STMT="$1"; shift ;;
  esac
done
[[ -n "${STMT}" ]] || { fail "Uso: $0 [--no-wait] [--timeout N] [--max-rows N] \"SQL\" | -f fichero.sql"; exit 2; }
[[ "${TIMEOUT}" =~ ^[0-9]+$ && "${MAX_ROWS}" =~ ^[0-9]+$ ]] || { fail "--timeout y --max-rows deben ser enteros"; exit 2; }

STATEMENTS_RUNNER_ID="$(terraform -chdir="${TF_DIR}" output -raw statements_runner_id)"
BASE="${FLINK_REST_ENDPOINT}/sql/v1/organizations/${CONFLUENT_ORG_ID}/environments/${CONFLUENT_ENV_ID}"
NAME="adhoc-$(date +%s)-$RANDOM"
AUTH="${FLINK_API_KEY}:${FLINK_API_SECRET}"
# Las credenciales se pasan por stdin (-K) para que no aparezcan en la lista de procesos
curl_auth() { curl -sS --connect-timeout 10 --max-time 30 -K <(printf 'user = "%s"\n' "${AUTH}") "$@"; }

body="$(jq -n --arg n "${NAME}" --arg s "${STMT}" --arg p "${FLINK_COMPUTE_POOL_ID}" --arg sa "${STATEMENTS_RUNNER_ID}" \
  --arg cat "${FLINK_ENVIRONMENT_NAME}" --arg db "${FLINK_CLUSTER_NAME}" --argjson pr "${PROPS}" \
  '{name:$n, spec:{statement:$s, compute_pool_id:$p, principal:$sa,
    properties:({"sql.current-catalog":$cat,"sql.current-database":$db} + $pr)}}')"

resp="$(curl_auth -X POST -H 'Content-Type: application/json' -d "${body}" "${BASE}/statements")"
if jq -e '.errors' >/dev/null 2>&1 <<<"${resp}"; then
  fail "Error al crear la sentencia:"; jq -r '.errors[] | "\(.title): \(.detail)"' <<<"${resp}" >&2; exit 1
fi
info "sentencia: ${NAME}" >&2
(( WAIT )) || { echo "${NAME}"; exit 0; }

# Se borra siempre la sentencia ad hoc al salir (también con Ctrl-C o error)
cleanup() { curl_auth -X DELETE "${BASE}/statements/${NAME}" >/dev/null 2>&1 || true; }
trap cleanup EXIT
trap 'exit 130' INT TERM PIPE

deadline=$(( SECONDS + TIMEOUT ))

# 1) Esperar a que salga de PENDING (acotado por el mismo plazo)
phase="PENDING"; st=""
while (( SECONDS < deadline )); do
  st="$(curl_auth "${BASE}/statements/${NAME}")"
  phase="$(jq -r '.status.phase // "UNKNOWN"' <<<"${st}")"
  [[ "${phase}" == "PENDING" ]] || break
  sleep 2
done
case "${phase}" in
  PENDING) fail "La sentencia sigue en PENDING tras ${TIMEOUT}s."; exit 3 ;;
  FAILED|STOPPED|FAILING) fail "Sentencia ${phase}: $(jq -r '.status.detail // ""' <<<"${st}")"; exit 1 ;;
esac
bounded="$(jq -r '.status.traits.is_bounded // false' <<<"${st}")"
[[ "${bounded}" == "true" ]] || warn "Consulta no acotada (streaming): se devolverá lo recibido en ${TIMEOUT}s como máximo."

# 2) Leer resultados. Cada iteración comprueba el plazo y el límite de filas; nunca hay bucle sin espera.
url="${BASE}/statements/${NAME}/results"
total=0
while :; do
  if (( SECONDS >= deadline )); then
    warn "Plazo de ${TIMEOUT}s agotado (${total} filas, fase ${phase})."
    exit 3
  fi
  page="$(curl_auth "${url}")"
  rows="$(jq -c '.results.data[]? | {op:.op, row:.row}' <<<"${page}")"
  if [[ -n "${rows}" ]]; then
    n_page=$(wc -l <<<"${rows}")
    if (( total + n_page >= MAX_ROWS )); then
      head -n $(( MAX_ROWS - total )) <<<"${rows}"
      warn "Límite de ${MAX_ROWS} filas alcanzado."; exit 0
    fi
    echo "${rows}"
    total=$(( total + n_page ))
  fi
  next="$(jq -r '.metadata.next // empty' <<<"${page}")"
  if [[ -n "${next}" ]]; then
    url="${next}"
    [[ -n "${rows}" ]] || sleep 2   # sin filas: no hacer polling a toda velocidad
    continue
  fi
  phase="$(curl_auth "${BASE}/statements/${NAME}" | jq -r '.status.phase // "UNKNOWN"')"
  case "${phase}" in
    COMPLETED) exit 0 ;;
    FAILED|STOPPED|FAILING) fail "Sentencia ${phase}"; exit 1 ;;
  esac
  sleep 2
done
