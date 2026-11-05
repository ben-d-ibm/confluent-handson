variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API key (Cloud resource management)."
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API secret."
  type        = string
  sensitive   = true
}

variable "project_name" {
  description = "Prefijo para nombrar los recursos."
  type        = string
  default     = "banking-streaming"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,30}$", var.project_name))
    error_message = "project_name: minúsculas, números y guiones (2-31 caracteres)."
  }
}

variable "region" {
  description = "Región de AWS."
  type        = string
  default     = "us-east-1"

  validation {
    condition     = can(regex("^[a-z]{2}-[a-z]+-[0-9]+$", var.region))
    error_message = "region debe tener formato AWS, por ejemplo us-east-1."
  }
}

variable "flink_max_cfu" {
  description = "Máximo de CFU del compute pool de Flink."
  type        = number
  default     = 5

  validation {
    condition     = var.flink_max_cfu >= 5 && var.flink_max_cfu <= 50
    error_message = "flink_max_cfu debe estar entre 5 y 50."
  }
}

variable "cluster_type" {
  description = "Tipo de clúster Kafka. BASIC es el más barato para el laboratorio; STANDARD si Tableflow lo requiere en tu región."
  type        = string
  default     = "BASIC"

  validation {
    condition     = contains(["BASIC", "STANDARD"], var.cluster_type)
    error_message = "cluster_type debe ser BASIC o STANDARD."
  }
}

variable "topic_partitions" {
  description = "Particiones de los tópicos de datos."
  type        = number
  default     = 3
}
