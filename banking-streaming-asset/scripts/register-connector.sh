#!/usr/bin/env bash
# Registra (o actualiza, de forma idempotente) el conector Debezium y espera a que esté RUNNING.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
[[ -f "${RUNTIME_ENV_FILE}" ]] || die "Falta .env.runtime. Ejecuta 'make cloud-up'."
set -a; source "${RUNTIME_ENV_FILE}"; set +a

NAME="${1:-banking-postgres-cdc}"
BASE="http://127.0.0.1:${CONNECT_PORT:-8083}"
CURL=(curl -fsS --connect-timeout 5 --max-time 30)

info "Esperando a la REST de Kafka Connect en ${BASE} ..."
for _ in $(seq 1 60); do
  "${CURL[@]}" "${BASE}/connectors" >/dev/null 2>&1 && break
  sleep 3
done
"${CURL[@]}" "${BASE}/connectors" >/dev/null 2>&1 || die "Kafka Connect no responde (revisa: make logs)."
ok "Kafka Connect responde"

cfg="$(jq --arg sr "${SCHEMA_REGISTRY_URL}" \
  'with_entries(.value |= (if type=="string" then gsub("__SCHEMA_REGISTRY_URL__"; $sr) else . end))' \
  "${ROOT_DIR}/connect/connector-postgres.json")"

# PUT /config crea o actualiza
"${CURL[@]}" -X PUT -H 'Content-Type: application/json' -d "${cfg}" "${BASE}/connectors/${NAME}/config" >/dev/null \
  || die "No se pudo registrar el conector ${NAME}."

info "Esperando a que el conector esté RUNNING ..."
failed_streak=0
sleep 3  # deja que Connect reinicie el conector con la nueva config antes de leer su estado
for _ in $(seq 1 40); do
  st="$("${CURL[@]}" "${BASE}/connectors/${NAME}/status" 2>/dev/null || true)"
  conn="$(jq -r '.connector.state // "?"' <<<"${st:-{\}}" 2>/dev/null || echo "?")"
  task="$(jq -r '.tasks[0].state // "?"' <<<"${st:-{\}}" 2>/dev/null || echo "?")"
  if [[ "${conn}" == "RUNNING" && "${task}" == "RUNNING" ]]; then
    ok "Conector ${NAME}: connector=RUNNING task=RUNNING"
    exit 0
  fi
  # Un FAILED puntual puede ser el estado anterior a la nueva config; solo es error si se mantiene
  if [[ "${conn}" == "FAILED" || "${task}" == "FAILED" ]]; then
    failed_streak=$((failed_streak + 1))
    if (( failed_streak >= 3 )); then
      fail "Conector en FAILED:"; jq -r '.tasks[0].trace // .connector.trace // empty' <<<"${st}" | head -15 >&2; exit 1
    fi
  else
    failed_streak=0
  fi
  sleep 3
done
die "El conector no llegó a RUNNING a tiempo (estado: connector=${conn} task=${task})."
