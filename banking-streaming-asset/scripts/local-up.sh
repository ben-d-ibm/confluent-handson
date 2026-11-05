#!/usr/bin/env bash
# Construye y levanta los contenedores locales (Postgres + Kafka Connect/Debezium) y registra el conector.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
[[ -f "${RUNTIME_ENV_FILE}" ]] || die "Falta .env.runtime. Ejecuta 'make cloud-up' primero."

rt="$(detect_runtime)" || die "No hay docker ni podman."
compose_cmd="$(detect_compose "${rt}")" || die "No hay compose para ${rt}."

info "== Levantando contenedores locales (${compose_cmd}) =="
# shellcheck disable=SC2086
${compose_cmd} -f "${ROOT_DIR}/compose/compose.yaml" \
  --env-file "${ENV_FILE}" --env-file "${RUNTIME_ENV_FILE}" up -d --build

"${ROOT_DIR}/scripts/register-connector.sh"
ok "Entorno local listo."
info "Solicitudes: http://127.0.0.1:8080   ·   Dashboard: http://127.0.0.1:8081"
