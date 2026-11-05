output "environment_id" {
  value = confluent_environment.main.id
}

output "environment_name" {
  description = "Catálogo de Flink"
  value       = confluent_environment.main.display_name
}

output "cluster_id" {
  value = confluent_kafka_cluster.main.id
}

output "cluster_name" {
  description = "Base de datos de Flink"
  value       = confluent_kafka_cluster.main.display_name
}

output "bootstrap_servers" {
  description = "host:puerto sin prefijo de protocolo"
  value       = replace(confluent_kafka_cluster.main.bootstrap_endpoint, "SASL_SSL://", "")
}

output "kafka_rest_endpoint" {
  value = confluent_kafka_cluster.main.rest_endpoint
}

output "schema_registry_url" {
  value = data.confluent_schema_registry_cluster.main.rest_endpoint
}

output "organization_id" {
  value = data.confluent_organization.main.id
}

output "flink_compute_pool_id" {
  value = confluent_flink_compute_pool.main.id
}

output "flink_rest_endpoint" {
  value = data.confluent_flink_region.main.rest_endpoint
}

output "flink_api_key" {
  value     = confluent_api_key.flink.id
  sensitive = true
}

output "flink_api_secret" {
  value     = confluent_api_key.flink.secret
  sensitive = true
}

output "statements_runner_id" {
  description = "Principal con el que se ejecutan las sentencias de Flink"
  value       = confluent_service_account.statements_runner.id
}

output "connect_kafka_api_key" {
  value     = confluent_api_key.connect_kafka.id
  sensitive = true
}

output "connect_kafka_api_secret" {
  value     = confluent_api_key.connect_kafka.secret
  sensitive = true
}

output "connect_sr_api_key" {
  value     = confluent_api_key.connect_schema_registry.id
  sensitive = true
}

output "connect_sr_api_secret" {
  value     = confluent_api_key.connect_schema_registry.secret
  sensitive = true
}

output "connect_group_id" {
  value = local.connect_group
}

output "connect_topic_prefix" {
  value = local.connect_prefix
}

# Fase 2: credenciales de las aplicaciones (mapa por aplicación). Se leen con
# `terraform output -json app_credentials` desde scripts/gen-env.sh.
output "app_credentials" {
  sensitive = true
  value = {
    for k, _ in local.apps : k => {
      kafka_key    = confluent_api_key.app_kafka[k].id
      kafka_secret = confluent_api_key.app_kafka[k].secret
      sr_key       = confluent_api_key.app_schema_registry[k].id
      sr_secret    = confluent_api_key.app_schema_registry[k].secret
      group_prefix = lookup(local.app_group_prefix, k, "")
    }
  }
}

output "topic_loan_requests" {
  value = local.topic_loan_requests
}

output "topic_loan_decisions" {
  value = local.topic_loan_decisions
}
