#!/usr/bin/env bash
# Valida que el entorno está listo. NO crea infraestructura ni arranca contenedores.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
set +e  # acumulamos errores en lugar de abortar al primero

ERRORS=0
check_fail() { fail "$*"; ERRORS=$((ERRORS + 1)); }

require_var() {
  local name="$1" hint="${2:-}"
  if [[ -z "${!name:-}" ]]; then
    check_fail "Variable obligatoria sin definir: ${name}${hint:+ (${hint})}"
  else
    ok "${name} definida"
  fi
}

info "== Herramientas =="
if command -v terraform >/dev/null 2>&1; then
  ok "terraform: $(terraform version | head -1)"
else
  check_fail "terraform no está instalado (https://developer.hashicorp.com/terraform/install)"
fi

command -v make >/dev/null 2>&1 && ok "make disponible" || check_fail "make no está instalado"
command -v curl >/dev/null 2>&1 && ok "curl disponible" || check_fail "curl no está instalado"
command -v jq   >/dev/null 2>&1 && ok "jq disponible"   || check_fail "jq no está instalado"

info "== Configuración (.env) =="
if [[ ! -f "${ENV_FILE}" ]]; then
  check_fail "No existe .env. Ejecuta 'make init' y rellénalo."
  info ""; fail "make check: ${ERRORS} problema(s) encontrados."; exit 1
fi
# shellcheck disable=SC1090
set -a; source "${ENV_FILE}"; set +a

perm="$(stat -f '%Lp' "${ENV_FILE}" 2>/dev/null || stat -c '%a' "${ENV_FILE}" 2>/dev/null)"
if [[ "${perm}" != "600" ]]; then
  warn ".env tiene permisos ${perm}; se recomienda 600 (chmod 600 .env)"
fi

require_var CONFLUENT_CLOUD_API_KEY    "Cloud API key de Confluent Cloud"
require_var CONFLUENT_CLOUD_API_SECRET
require_var POSTGRES_PASSWORD          "ejecuta 'make init' para generarla"
require_var CDC_PASSWORD

region="${CLOUD_REGION:-us-east-1}"
if [[ "${region}" =~ ^[a-z]{2}-[a-z]+-[0-9]+$ ]]; then
  ok "CLOUD_REGION=${region}"
else
  check_fail "CLOUD_REGION='${region}' no tiene formato de región AWS (ej. us-east-1)"
fi

if [[ "${FLINK_MAX_CFU:-5}" =~ ^[0-9]+$ ]] && (( ${FLINK_MAX_CFU:-5} >= 5 && ${FLINK_MAX_CFU:-5} <= 50 )); then
  ok "FLINK_MAX_CFU=${FLINK_MAX_CFU:-5}"
else
  check_fail "FLINK_MAX_CFU debe ser un entero entre 5 y 50"
fi

info "== Contenedores =="
if rt="$(detect_runtime)"; then
  ok "runtime: ${rt}"
  if "${rt}" info >/dev/null 2>&1; then
    ok "${rt} responde"
  else
    hint=""; [[ "${rt}" == "podman" ]] && hint=" (prueba 'podman machine start')"
    check_fail "${rt} está instalado pero no responde${hint}"
  fi
  if compose_cmd="$(detect_compose "${rt}")"; then
    ok "compose: ${compose_cmd}"
  else
    check_fail "No se encontró compose para ${rt} (docker compose / podman compose / podman-compose)"
  fi
else
  check_fail "No hay runtime de contenedores: instala docker o podman (CONTAINER_RUNTIME=${CONTAINER_RUNTIME:-auto})"
fi

info "== Puertos locales =="
for var in POSTGRES_PORT CONNECT_PORT; do
  port="${!var:-}"
  if [[ ! "${port}" =~ ^[0-9]+$ ]] || (( port < 1024 || port > 65535 )); then
    check_fail "${var}='${port}' no es un puerto válido (1024-65535)"
  elif is_port_free "${port}"; then
    ok "${var}=${port} libre"
  else
    check_fail "${var}=${port} está en uso"
  fi
done

info "== watsonx.data (opcional) =="
case "${ENABLE_WATSONXDATA:-false}" in
  false) ok "ENABLE_WATSONXDATA=false (modo solo Confluent)" ;;
  true)
    ok "ENABLE_WATSONXDATA=true"
    for v in WXD_HOST WXD_PORT WXD_USER WXD_API_KEY WXD_CATALOG WXD_SCHEMA; do require_var "$v"; done
    if [[ -n "${WXD_PORT:-}" && ! "${WXD_PORT}" =~ ^[0-9]+$ ]]; then
      check_fail "WXD_PORT debe ser numérico"
    fi ;;
  *) check_fail "ENABLE_WATSONXDATA debe ser 'true' o 'false' (valor actual: '${ENABLE_WATSONXDATA}')" ;;
esac

info ""
if (( ERRORS > 0 )); then
  fail "make check: ${ERRORS} problema(s) encontrados."
  exit 1
fi
ok "make check: entorno listo."
