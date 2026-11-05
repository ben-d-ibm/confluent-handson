#!/usr/bin/env bash
# Tráfico de demo: arranca producer-java en modo continuo (1 solicitud cada DEMO_TRAFFIC_INTERVAL_SECONDS, 10 por defecto).
#   scripts/demo-traffic.sh        -> arranca (en segundo plano)
#   scripts/demo-traffic.sh stop   -> para y elimina el contenedor
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

action="${1:-start}"
[[ "${action}" == "start" || "${action}" == "stop" ]] || die "Uso: $0 [start|stop]"

load_env
[[ -f "${RUNTIME_ENV_FILE}" ]] || die "Falta .env.runtime. Ejecuta 'make up' primero."
rt="$(detect_runtime)" || die "No hay docker ni podman."
compose_cmd="$(detect_compose "${rt}")" || die "No hay compose para ${rt}."
service="producer-demo-traffic"

compose() {
  # shellcheck disable=SC2086
  ${compose_cmd} -f "${ROOT_DIR}/compose/compose.yaml" \
    --env-file "${ENV_FILE}" --env-file "${RUNTIME_ENV_FILE}" --profile demo-traffic "$@"
}

if [[ "${action}" == "start" ]]; then
  info "== Iniciando tráfico de demo (${service}) =="
  compose up -d --build --no-deps "${service}"
  ok "Tráfico de demo en marcha. Para pararlo: make demo-traffic-stop"
else
  info "== Parando tráfico de demo (${service}) =="
  compose stop -t 10 "${service}" || warn "No se pudo parar ${service} (¿ya estaba parado?)."
  # podman-compose no tiene `rm`: se elimina por la etiqueta del servicio con el propio runtime.
  ids="$("${rt}" ps -a -q --filter "label=com.docker.compose.service=${service}")"
  # shellcheck disable=SC2086
  [[ -z "${ids}" ]] || "${rt}" rm -f ${ids} >/dev/null
  ok "Tráfico de demo parado."
fi
