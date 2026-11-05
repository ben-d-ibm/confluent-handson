# Entradas que genera scripts/flink-up.sh a partir de los outputs del Terraform base (TF_VAR_*).
variable "organization_id" { type = string }
variable "environment_id" { type = string }
variable "environment_name" {
  description = "Catálogo de Flink"
  type        = string
}
variable "cluster_name" {
  description = "Base de datos de Flink"
  type        = string
}
variable "compute_pool_id" { type = string }
variable "flink_rest_endpoint" { type = string }
variable "statements_runner_id" {
  description = "Service account con el que se ejecutan las sentencias"
  type        = string
}
variable "flink_api_key" {
  type      = string
  sensitive = true
}
variable "flink_api_secret" {
  type      = string
  sensitive = true
}
variable "project_name" {
  type    = string
  default = "banking-streaming"
}
