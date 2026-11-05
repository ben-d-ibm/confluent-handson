data "confluent_organization" "main" {}

locals {
  cloud = "AWS"

  # Tópicos de negocio
  topic_loan_requests   = "loan-requests"
  topic_customers       = "customers"
  topic_payment_history = "payment_history"

  # Tópicos internos de Kafka Connect (modo distribuido) y group.id del worker
  connect_group  = "${var.project_name}-connect"
  connect_prefix = "${var.project_name}-connect-"
}

# -----------------------------------------------------------------------------
# Entorno, clúster y Schema Registry
# En Flink de Confluent Cloud: environment = catálogo, clúster Kafka = base de datos.
# -----------------------------------------------------------------------------
resource "confluent_environment" "main" {
  display_name = "${var.project_name}-env"

  stream_governance {
    package = "ESSENTIALS"
  }
}

resource "confluent_kafka_cluster" "main" {
  display_name = "${var.project_name}-cluster"
  availability = "SINGLE_ZONE"
  cloud        = local.cloud
  region       = var.region

  dynamic "basic" {
    for_each = var.cluster_type == "BASIC" ? [1] : []
    content {}
  }

  dynamic "standard" {
    for_each = var.cluster_type == "STANDARD" ? [1] : []
    content {}
  }

  environment {
    id = confluent_environment.main.id
  }
}

# El Schema Registry se aprovisiona automáticamente al crear el primer clúster del entorno.
data "confluent_schema_registry_cluster" "main" {
  environment {
    id = confluent_environment.main.id
  }

  depends_on = [confluent_kafka_cluster.main]
}

data "confluent_flink_region" "main" {
  cloud  = local.cloud
  region = var.region
}

# -----------------------------------------------------------------------------
# Service accounts
# -----------------------------------------------------------------------------
resource "confluent_service_account" "infrastructure_manager" {
  display_name = "${var.project_name}-infra-manager"
  description  = "Crea tópicos y esquemas (Terraform)"
}

resource "confluent_service_account" "statements_runner" {
  display_name = "${var.project_name}-statements-runner"
  description  = "Ejecuta las sentencias de Flink SQL"
}

resource "confluent_service_account" "app_manager" {
  display_name = "${var.project_name}-app-manager"
  description  = "Propietario de la API key de Flink"
}

resource "confluent_service_account" "connect_cdc" {
  display_name = "${var.project_name}-connect-cdc"
  description  = "Kafka Connect + Debezium (CDC desde PostgreSQL)"
}

# -----------------------------------------------------------------------------
# Role bindings
# -----------------------------------------------------------------------------
resource "confluent_role_binding" "infrastructure_manager_env_admin" {
  principal   = "User:${confluent_service_account.infrastructure_manager.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.main.resource_name
}

resource "confluent_role_binding" "statements_runner_env_admin" {
  principal   = "User:${confluent_service_account.statements_runner.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.main.resource_name
}

resource "confluent_role_binding" "app_manager_flink_developer" {
  principal   = "User:${confluent_service_account.app_manager.id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = confluent_environment.main.resource_name
}

# Nota: los clústeres BASIC no admiten role bindings con alcance de recurso (topic, group,
# transactional-id), solo ACLs. Por eso los permisos de Connect usan confluent_kafka_acl.
# Los bindings de entorno (EnvironmentAdmin, FlinkDeveloper, Assigner) sí funcionan en Basic.
resource "confluent_role_binding" "app_manager_assigner" {
  principal   = "User:${confluent_service_account.app_manager.id}"
  role_name   = "Assigner"
  crn_pattern = "${data.confluent_organization.main.resource_name}/service-account=${confluent_service_account.statements_runner.id}"
}

resource "confluent_role_binding" "connect_schema_registry" {
  principal   = "User:${confluent_service_account.connect_cdc.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_schema_registry_cluster.main.resource_name}/subject=*"
}

# -----------------------------------------------------------------------------
# ACLs de Kafka Connect (mínimo privilegio, válidas en Basic y Standard)
#  - CDC: escribir en sus dos tópicos
#  - Tópicos internos de Connect (prefijo): leer/escribir
#  - Consumer group del worker: leer
# -----------------------------------------------------------------------------
locals {
  connect_acls = {
    customers_write        = { resource_type = "TOPIC", name = local.topic_customers, pattern = "LITERAL", operation = "WRITE" }
    customers_describe     = { resource_type = "TOPIC", name = local.topic_customers, pattern = "LITERAL", operation = "DESCRIBE" }
    customers_desc_conf    = { resource_type = "TOPIC", name = local.topic_customers, pattern = "LITERAL", operation = "DESCRIBE_CONFIGS" }
    payments_write         = { resource_type = "TOPIC", name = local.topic_payment_history, pattern = "LITERAL", operation = "WRITE" }
    payments_describe      = { resource_type = "TOPIC", name = local.topic_payment_history, pattern = "LITERAL", operation = "DESCRIBE" }
    payments_desc_conf     = { resource_type = "TOPIC", name = local.topic_payment_history, pattern = "LITERAL", operation = "DESCRIBE_CONFIGS" }
    internal_read          = { resource_type = "TOPIC", name = local.connect_prefix, pattern = "PREFIXED", operation = "READ" }
    internal_write         = { resource_type = "TOPIC", name = local.connect_prefix, pattern = "PREFIXED", operation = "WRITE" }
    internal_describe      = { resource_type = "TOPIC", name = local.connect_prefix, pattern = "PREFIXED", operation = "DESCRIBE" }
    internal_describe_conf = { resource_type = "TOPIC", name = local.connect_prefix, pattern = "PREFIXED", operation = "DESCRIBE_CONFIGS" }
    group_read             = { resource_type = "GROUP", name = local.connect_group, pattern = "LITERAL", operation = "READ" }
  }
}

resource "confluent_kafka_acl" "connect" {
  for_each = local.connect_acls

  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }
  resource_type = each.value.resource_type
  resource_name = each.value.name
  pattern_type  = each.value.pattern
  principal     = "User:${confluent_service_account.connect_cdc.id}"
  host          = "*"
  operation     = each.value.operation
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.infra_kafka.id
    secret = confluent_api_key.infra_kafka.secret
  }
}

# -----------------------------------------------------------------------------
# API keys
# -----------------------------------------------------------------------------
resource "confluent_api_key" "infra_kafka" {
  display_name = "${var.project_name}-infra-kafka-key"
  description  = "Kafka API key de infrastructure-manager (Terraform)"

  owner {
    id          = confluent_service_account.infrastructure_manager.id
    api_version = confluent_service_account.infrastructure_manager.api_version
    kind        = confluent_service_account.infrastructure_manager.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [confluent_role_binding.infrastructure_manager_env_admin]
}

resource "confluent_api_key" "infra_schema_registry" {
  display_name = "${var.project_name}-infra-sr-key"
  description  = "Schema Registry API key de infrastructure-manager (Terraform)"

  owner {
    id          = confluent_service_account.infrastructure_manager.id
    api_version = confluent_service_account.infrastructure_manager.api_version
    kind        = confluent_service_account.infrastructure_manager.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.main.id
    api_version = data.confluent_schema_registry_cluster.main.api_version
    kind        = data.confluent_schema_registry_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [confluent_role_binding.infrastructure_manager_env_admin]
}

resource "confluent_api_key" "flink" {
  display_name = "${var.project_name}-flink-key"
  description  = "Flink API key de app-manager"

  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
  }

  managed_resource {
    id          = data.confluent_flink_region.main.id
    api_version = data.confluent_flink_region.main.api_version
    kind        = data.confluent_flink_region.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [
    confluent_role_binding.app_manager_flink_developer,
  ]
}

resource "confluent_api_key" "connect_kafka" {
  display_name = "${var.project_name}-connect-kafka-key"
  description  = "Kafka API key de Kafka Connect + Debezium"

  owner {
    id          = confluent_service_account.connect_cdc.id
    api_version = confluent_service_account.connect_cdc.api_version
    kind        = confluent_service_account.connect_cdc.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }
}

resource "confluent_api_key" "connect_schema_registry" {
  display_name = "${var.project_name}-connect-sr-key"
  description  = "Schema Registry API key de Kafka Connect + Debezium"

  owner {
    id          = confluent_service_account.connect_cdc.id
    api_version = confluent_service_account.connect_cdc.api_version
    kind        = confluent_service_account.connect_cdc.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.main.id
    api_version = data.confluent_schema_registry_cluster.main.api_version
    kind        = data.confluent_schema_registry_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }
}

# -----------------------------------------------------------------------------
# Tópicos
# -----------------------------------------------------------------------------
resource "confluent_kafka_topic" "loan_requests" {
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }
  topic_name       = local.topic_loan_requests
  partitions_count = var.topic_partitions
  rest_endpoint    = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.infra_kafka.id
    secret = confluent_api_key.infra_kafka.secret
  }
}

# Tópicos CDC: Debezium publica aquí (el nombre final se fija con un SMT en la tarea 1.3)
resource "confluent_kafka_topic" "customers" {
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }
  topic_name       = local.topic_customers
  partitions_count = var.topic_partitions
  rest_endpoint    = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.infra_kafka.id
    secret = confluent_api_key.infra_kafka.secret
  }
}

resource "confluent_kafka_topic" "payment_history" {
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }
  topic_name       = local.topic_payment_history
  partitions_count = var.topic_partitions
  rest_endpoint    = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.infra_kafka.id
    secret = confluent_api_key.infra_kafka.secret
  }
}

# Tópicos internos de Kafka Connect (distribuido). Deben ser compactados.
resource "confluent_kafka_topic" "connect_configs" {
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }
  topic_name       = "${local.connect_prefix}configs"
  partitions_count = 1
  rest_endpoint    = confluent_kafka_cluster.main.rest_endpoint
  config           = { "cleanup.policy" = "compact" }

  credentials {
    key    = confluent_api_key.infra_kafka.id
    secret = confluent_api_key.infra_kafka.secret
  }
}

resource "confluent_kafka_topic" "connect_offsets" {
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }
  topic_name       = "${local.connect_prefix}offsets"
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.main.rest_endpoint
  config           = { "cleanup.policy" = "compact" }

  credentials {
    key    = confluent_api_key.infra_kafka.id
    secret = confluent_api_key.infra_kafka.secret
  }
}

resource "confluent_kafka_topic" "connect_status" {
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }
  topic_name       = "${local.connect_prefix}status"
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.main.rest_endpoint
  config           = { "cleanup.policy" = "compact" }

  credentials {
    key    = confluent_api_key.infra_kafka.id
    secret = confluent_api_key.infra_kafka.secret
  }
}

# -----------------------------------------------------------------------------
# Esquema de loan-requests (contrato único para productor Java, frontend y Flink)
# -----------------------------------------------------------------------------
resource "confluent_schema" "loan_requests_value" {
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.main.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.main.rest_endpoint
  subject_name  = "${confluent_kafka_topic.loan_requests.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/loan-requests.avsc")
  hard_delete   = true

  credentials {
    key    = confluent_api_key.infra_schema_registry.id
    secret = confluent_api_key.infra_schema_registry.secret
  }
}

# -----------------------------------------------------------------------------
# Flink compute pool (las sentencias SQL se añaden en la tarea 1.4)
# -----------------------------------------------------------------------------
resource "confluent_flink_compute_pool" "main" {
  display_name = "${var.project_name}-pool"
  cloud        = local.cloud
  region       = var.region
  max_cfu      = var.flink_max_cfu

  environment {
    id = confluent_environment.main.id
  }

  depends_on = [
    confluent_role_binding.statements_runner_env_admin,
    confluent_role_binding.app_manager_assigner,
    confluent_role_binding.app_manager_flink_developer,
    confluent_api_key.flink,
  ]
}

# =============================================================================
# FASE 2 (aditivo): identidades de mínimo privilegio para productor y apps web
#  - producer:     escribe en loan-requests (productor Java)
#  - requests_api: escribe en loan-requests (formulario) y lee loan-decisions
#                  para informar del estado real de cada request_id
#  - dashboard_api: lee loan-decisions (dashboard modo Kafka)
# Clúster BASIC: solo ACLs a nivel de topic/group. En Schema Registry se usan
# role bindings con alcance de subject (sí soportados).
# =============================================================================
locals {
  topic_loan_decisions = "loan-decisions"

  apps = {
    producer      = { description = "Productor Java de loan-requests", sr_subject = local.topic_loan_requests }
    requests_api  = { description = "Backend del formulario de solicitudes", sr_subject = local.topic_loan_requests }
    dashboard_api = { description = "Backend del dashboard (modo Kafka)", sr_subject = local.topic_loan_decisions }
  }

  # Prefijo de consumer group por aplicación (los consumidores usan group.id con este prefijo)
  app_group_prefix = {
    requests_api  = "${var.project_name}-requests-api-"
    dashboard_api = "${var.project_name}-dashboard-api-"
  }

  app_acls = merge(
    {
      producer_write    = { app = "producer", type = "TOPIC", name = local.topic_loan_requests, pattern = "LITERAL", op = "WRITE" }
      producer_describe = { app = "producer", type = "TOPIC", name = local.topic_loan_requests, pattern = "LITERAL", op = "DESCRIBE" }

      requests_write    = { app = "requests_api", type = "TOPIC", name = local.topic_loan_requests, pattern = "LITERAL", op = "WRITE" }
      requests_describe = { app = "requests_api", type = "TOPIC", name = local.topic_loan_requests, pattern = "LITERAL", op = "DESCRIBE" }
      requests_read     = { app = "requests_api", type = "TOPIC", name = local.topic_loan_decisions, pattern = "LITERAL", op = "READ" }
      requests_dec_desc = { app = "requests_api", type = "TOPIC", name = local.topic_loan_decisions, pattern = "LITERAL", op = "DESCRIBE" }
      requests_group    = { app = "requests_api", type = "GROUP", name = local.app_group_prefix["requests_api"], pattern = "PREFIXED", op = "READ" }

      dashboard_read     = { app = "dashboard_api", type = "TOPIC", name = local.topic_loan_decisions, pattern = "LITERAL", op = "READ" }
      dashboard_describe = { app = "dashboard_api", type = "TOPIC", name = local.topic_loan_decisions, pattern = "LITERAL", op = "DESCRIBE" }
      dashboard_group    = { app = "dashboard_api", type = "GROUP", name = local.app_group_prefix["dashboard_api"], pattern = "PREFIXED", op = "READ" }
    }
  )
}

resource "confluent_service_account" "app" {
  for_each = local.apps

  display_name = "${var.project_name}-${replace(each.key, "_", "-")}"
  description  = each.value.description
}

resource "confluent_kafka_acl" "app" {
  for_each = local.app_acls

  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }
  resource_type = each.value.type
  resource_name = each.value.name
  pattern_type  = each.value.pattern
  principal     = "User:${confluent_service_account.app[each.value.app].id}"
  host          = "*"
  operation     = each.value.op
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.infra_kafka.id
    secret = confluent_api_key.infra_kafka.secret
  }
}

# Lectura de esquemas acotada al subject del tópico que usa cada aplicación
resource "confluent_role_binding" "app_schema_registry" {
  for_each = local.apps

  principal   = "User:${confluent_service_account.app[each.key].id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.main.resource_name}/subject=${each.value.sr_subject}-value"
}

# requests_api también deserializa loan-decisions
resource "confluent_role_binding" "requests_api_decisions_schema" {
  principal   = "User:${confluent_service_account.app["requests_api"].id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.main.resource_name}/subject=${local.topic_loan_decisions}-value"
}

resource "confluent_api_key" "app_kafka" {
  for_each = local.apps

  display_name = "${var.project_name}-${replace(each.key, "_", "-")}-kafka-key"
  description  = "Kafka API key: ${each.value.description}"

  owner {
    id          = confluent_service_account.app[each.key].id
    api_version = confluent_service_account.app[each.key].api_version
    kind        = confluent_service_account.app[each.key].kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }
}

resource "confluent_api_key" "app_schema_registry" {
  for_each = local.apps

  display_name = "${var.project_name}-${replace(each.key, "_", "-")}-sr-key"
  description  = "Schema Registry API key: ${each.value.description}"

  owner {
    id          = confluent_service_account.app[each.key].id
    api_version = confluent_service_account.app[each.key].api_version
    kind        = confluent_service_account.app[each.key].kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.main.id
    api_version = data.confluent_schema_registry_cluster.main.api_version
    kind        = data.confluent_schema_registry_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }
}
