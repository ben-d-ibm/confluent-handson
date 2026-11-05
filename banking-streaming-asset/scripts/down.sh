#!/usr/bin/env bash
# Da de baja TODO lo que crea el asset: recursos locales y recursos de Confluent Cloud.
#   scripts/down.sh local   -> solo contenedores y volúmenes locales
#   scripts/down.sh all     -> local + terraform destroy (por defecto)
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

scope="${1:-all}"
[[ "${scope}" == "local" || "${scope}" == "all" ]] || die "Uso: $0 [local|all]"

destroy_local() {
  info "== Eliminando recursos locales =="
  local compose_file="${ROOT_DIR}/compose/compose.yaml"
  if [[ -f "${compose_file}" ]]; then
    local rt compose_cmd
    if rt="$(detect_runtime)" && compose_cmd="$(detect_compose "${rt}")"; then
      # Sin --env-file obligatorio: el compose se invoca con lo que exista
      local env_args=()
      [[ -f "${ENV_FILE}" ]] && env_args+=(--env-file "${ENV_FILE}")
      [[ -f "${RUNTIME_ENV_FILE}" ]] && env_args+=(--env-file "${RUNTIME_ENV_FILE}")
      # shellcheck disable=SC2086
      ${compose_cmd} -f "${compose_file}" "${env_args[@]}" --profile demo-traffic down --volumes --remove-orphans \
        || warn "No se pudo bajar el compose (¿ya estaba parado?)."
    else
      warn "No hay runtime de contenedores disponible; se omite la limpieza local."
    fi
  else
    info "Aún no hay compose; nada que limpiar en local."
  fi
}

destroy_cloud() {
  info "== Destruyendo recursos de Confluent Cloud (terraform destroy) =="
  command -v terraform >/dev/null 2>&1 || die "terraform no está instalado."
  load_env
  [[ -d "${TF_DIR}/.terraform" ]] || terraform -chdir="${TF_DIR}" init -input=false
  # 1) Sentencias de Flink (estado propio) antes de borrar el compute pool que las aloja
  if [[ -n "$(ls -A "${TF_FLINK_DIR}"/terraform.tfstate 2>/dev/null)" ]] && \
     [[ -n "$(terraform -chdir="${TF_FLINK_DIR}" state list 2>/dev/null)" ]]; then
    local tf_out; tf_out() { terraform -chdir="${TF_DIR}" output -raw "$1"; }
    TF_VAR_organization_id="$(tf_out organization_id)" TF_VAR_environment_id="$(tf_out environment_id)" \
    TF_VAR_environment_name="$(tf_out environment_name)" TF_VAR_cluster_name="$(tf_out cluster_name)" \
    TF_VAR_compute_pool_id="$(tf_out flink_compute_pool_id)" TF_VAR_flink_rest_endpoint="$(tf_out flink_rest_endpoint)" \
    TF_VAR_statements_runner_id="$(tf_out statements_runner_id)" TF_VAR_flink_api_key="$(tf_out flink_api_key)" \
    TF_VAR_flink_api_secret="$(tf_out flink_api_secret)" \
      terraform -chdir="${TF_FLINK_DIR}" destroy -input=false -auto-approve
  fi
  terraform -chdir="${TF_DIR}" destroy -input=false -auto-approve
  rm -f "${RUNTIME_ENV_FILE}"
  ok "Recursos de Confluent Cloud eliminados."
}

destroy_local
[[ "${scope}" == "all" ]] && destroy_cloud
ok "Limpieza completada (${scope})."
