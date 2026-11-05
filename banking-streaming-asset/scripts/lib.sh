#!/usr/bin/env bash
# Funciones comunes. Se usa con `source` desde el resto de scripts.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${ROOT_DIR}/terraform"
TF_FLINK_DIR="${ROOT_DIR}/terraform-flink"
ENV_FILE="${ROOT_DIR}/.env"
RUNTIME_ENV_FILE="${ROOT_DIR}/.env.runtime"

if [[ -t 1 ]]; then
  C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YEL=$'\033[33m'; C_RST=$'\033[0m'
else
  C_RED=""; C_GRN=""; C_YEL=""; C_RST=""
fi

info() { printf '%s\n' "$*"; }
ok()   { printf '%s[ OK ]%s %s\n' "${C_GRN}" "${C_RST}" "$*"; }
warn() { printf '%s[WARN]%s %s\n' "${C_YEL}" "${C_RST}" "$*" >&2; }
fail() { printf '%s[FAIL]%s %s\n' "${C_RED}" "${C_RST}" "$*" >&2; }
die()  { fail "$*"; exit 1; }

# Carga .env y exporta las variables que consume Terraform (TF_VAR_*).
load_env() {
  [[ -f "${ENV_FILE}" ]] || die "No existe ${ENV_FILE}. Ejecuta 'make init' y rellena el fichero."
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a

  export TF_VAR_confluent_cloud_api_key="${CONFLUENT_CLOUD_API_KEY:-}"
  export TF_VAR_confluent_cloud_api_secret="${CONFLUENT_CLOUD_API_SECRET:-}"
  export TF_VAR_region="${CLOUD_REGION:-us-east-1}"
  export TF_VAR_project_name="${PROJECT_NAME:-banking-streaming}"
  export TF_VAR_flink_max_cfu="${FLINK_MAX_CFU:-5}"
  export TF_IN_AUTOMATION=1
}

# Devuelve docker o podman según CONTAINER_RUNTIME (auto prefiere podman, luego docker).
detect_runtime() {
  local pref="${CONTAINER_RUNTIME:-auto}"
  case "${pref}" in
    docker|podman) command -v "${pref}" >/dev/null 2>&1 && { echo "${pref}"; return 0; } ;;
    auto)
      if command -v podman >/dev/null 2>&1; then echo podman; return 0; fi
      if command -v docker >/dev/null 2>&1; then echo docker; return 0; fi ;;
  esac
  return 1
}

# Comando de compose disponible para el runtime ("docker compose", "podman compose" o "podman-compose").
detect_compose() {
  local rt="$1"
  if "${rt}" compose version >/dev/null 2>&1; then echo "${rt} compose"; return 0; fi
  if [[ "${rt}" == "podman" ]] && command -v podman-compose >/dev/null 2>&1; then echo "podman-compose"; return 0; fi
  if command -v docker-compose >/dev/null 2>&1; then echo "docker-compose"; return 0; fi
  return 1
}

is_port_free() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    ! lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1
  else
    ! (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null
  fi
}
