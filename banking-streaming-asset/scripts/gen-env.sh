#!/usr/bin/env bash
# Genera .env.runtime (permisos 600) con los endpoints y credenciales creados por Terraform.
# Lo consumen los contenedores locales (Connect/Debezium, generador, backends).
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env

tf_out() { terraform -chdir="${TF_DIR}" output -raw "$1"; }

command -v jq >/dev/null || die "Se necesita jq para leer app_credentials."
app_creds="$(terraform -chdir="${TF_DIR}" output -json app_credentials)"
app_cred() { jq -r --arg a "$1" --arg f "$2" '.[$a][$f]' <<<"${app_creds}"; }

umask 077
tmp="$(mktemp)"
trap 'rm -f "${tmp}"' EXIT

{
  echo "# Generado por scripts/gen-env.sh. NO editar ni versionar."
  echo "KAFKA_BOOTSTRAP_SERVERS=$(tf_out bootstrap_servers)"
  echo "KAFKA_REST_ENDPOINT=$(tf_out kafka_rest_endpoint)"
  echo "SCHEMA_REGISTRY_URL=$(tf_out schema_registry_url)"
  echo "CONNECT_KAFKA_API_KEY=$(tf_out connect_kafka_api_key)"
  echo "CONNECT_KAFKA_API_SECRET=$(tf_out connect_kafka_api_secret)"
  echo "CONNECT_SR_API_KEY=$(tf_out connect_sr_api_key)"
  echo "CONNECT_SR_API_SECRET=$(tf_out connect_sr_api_secret)"
  echo "CONNECT_GROUP_ID=$(tf_out connect_group_id)"
  echo "CONNECT_TOPIC_PREFIX=$(tf_out connect_topic_prefix)"
  echo "FLINK_ENVIRONMENT_NAME=$(tf_out environment_name)"
  echo "FLINK_CLUSTER_NAME=$(tf_out cluster_name)"
  echo "FLINK_COMPUTE_POOL_ID=$(tf_out flink_compute_pool_id)"
  echo "FLINK_REST_ENDPOINT=$(tf_out flink_rest_endpoint)"
  echo "FLINK_API_KEY=$(tf_out flink_api_key)"
  echo "FLINK_API_SECRET=$(tf_out flink_api_secret)"
  echo "CONFLUENT_ORG_ID=$(tf_out organization_id)"
  echo "CONFLUENT_ENV_ID=$(tf_out environment_id)"
  # Fase 2: topics y credenciales por aplicación (mínimo privilegio)
  echo "TOPIC_LOAN_REQUESTS=$(tf_out topic_loan_requests)"
  echo "TOPIC_LOAN_DECISIONS=$(tf_out topic_loan_decisions)"
  echo "PRODUCER_KAFKA_API_KEY=$(app_cred producer kafka_key)"
  echo "PRODUCER_KAFKA_API_SECRET=$(app_cred producer kafka_secret)"
  echo "PRODUCER_SR_API_KEY=$(app_cred producer sr_key)"
  echo "PRODUCER_SR_API_SECRET=$(app_cred producer sr_secret)"
  echo "REQUESTS_API_KAFKA_API_KEY=$(app_cred requests_api kafka_key)"
  echo "REQUESTS_API_KAFKA_API_SECRET=$(app_cred requests_api kafka_secret)"
  echo "REQUESTS_API_SR_API_KEY=$(app_cred requests_api sr_key)"
  echo "REQUESTS_API_SR_API_SECRET=$(app_cred requests_api sr_secret)"
  echo "REQUESTS_API_GROUP_PREFIX=$(app_cred requests_api group_prefix)"
  echo "DASHBOARD_API_KAFKA_API_KEY=$(app_cred dashboard_api kafka_key)"
  echo "DASHBOARD_API_KAFKA_API_SECRET=$(app_cred dashboard_api kafka_secret)"
  echo "DASHBOARD_API_SR_API_KEY=$(app_cred dashboard_api sr_key)"
  echo "DASHBOARD_API_SR_API_SECRET=$(app_cred dashboard_api sr_secret)"
  echo "DASHBOARD_API_GROUP_PREFIX=$(app_cred dashboard_api group_prefix)"
} > "${tmp}"

mv "${tmp}" "${RUNTIME_ENV_FILE}"
chmod 600 "${RUNTIME_ENV_FILE}"
trap - EXIT
ok "Generado ${RUNTIME_ENV_FILE#"${ROOT_DIR}/"} (permisos 600)."
