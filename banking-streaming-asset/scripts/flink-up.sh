#!/usr/bin/env bash
# Despliega las sentencias permanentes de Flink SQL (tarea 1.4) con Terraform (módulo terraform-flink/, estado propio).
#   1) Declara las PKs de las tablas CDC (idempotente: si ya existen, continúa).
#   2) terraform apply del módulo terraform-flink (tablas + INSERT de stats y de decisiones).
# No toca la infraestructura base (terraform/). Requiere 'make cloud-up' y el conector CDC en RUNNING.
#   scripts/flink-up.sh            aplica
#   scripts/flink-up.sh plan       solo muestra el plan
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
mode="${1:-apply}"
[[ "${mode}" == "apply" || "${mode}" == "plan" ]] || die "Uso: $0 [apply|plan]"

tf_out() { terraform -chdir="${TF_DIR}" output -raw "$1"; }

info "== 1/2 PKs de las tablas CDC =="
# Una sentencia por línea en 00-primary-keys.sql; 'ya definida' no es un error.
while IFS= read -r stmt || [[ -n "${stmt}" ]]; do
  stmt="${stmt%;}"
  [[ -n "${stmt// }" && "${stmt}" != --* ]] || continue
  out="$(bash "${ROOT_DIR}/scripts/flink-sql.sh" --timeout 60 "${stmt}" 2>&1)" && ok "${stmt%% NOT*}" \
    || { grep -q "already defined the primary key" <<<"${out}" && ok "PK ya definida: ${stmt%% NOT*}" || { echo "${out}" >&2; die "Falló: ${stmt}"; }; }
done < "${ROOT_DIR}/flink-sql/00-primary-keys.sql"

info "== 2/2 Terraform (terraform-flink) =="
export TF_VAR_organization_id="$(tf_out organization_id)"
export TF_VAR_environment_id="$(tf_out environment_id)"
export TF_VAR_environment_name="$(tf_out environment_name)"
export TF_VAR_cluster_name="$(tf_out cluster_name)"
export TF_VAR_compute_pool_id="$(tf_out flink_compute_pool_id)"
export TF_VAR_flink_rest_endpoint="$(tf_out flink_rest_endpoint)"
export TF_VAR_statements_runner_id="$(tf_out statements_runner_id)"
export TF_VAR_flink_api_key="$(tf_out flink_api_key)"
export TF_VAR_flink_api_secret="$(tf_out flink_api_secret)"
export TF_VAR_project_name="${PROJECT_NAME:-banking-streaming}"

terraform -chdir="${TF_FLINK_DIR}" init -input=false >/dev/null
if [[ "${mode}" == "plan" ]]; then
  terraform -chdir="${TF_FLINK_DIR}" plan -input=false
else
  terraform -chdir="${TF_FLINK_DIR}" apply -input=false -auto-approve
  ok "Sentencias de Flink desplegadas."
fi
