terraform {
  required_version = ">= 1.5.0"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.93"
    }
  }
}

# Sin credenciales de Cloud: las sentencias usan la API key de Flink (credentials de cada recurso).
provider "confluent" {}
