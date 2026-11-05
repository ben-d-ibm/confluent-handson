# Sentencias permanentes de Flink SQL (tarea 1.4).
# Requisito: las tablas CDC (customers, payment_history) ya existen y tienen PK (scripts/flink-init.sh).
#
# Aviso: 'statement' no se puede modificar en caliente. Cambiar un umbral de var.scoring (o el SQL)
# REEMPLAZA la sentencia INSERT, que vuelve a leer los tópicos desde el principio y reemite decisiones
# (misma clave request_id) con los nuevos umbrales.

locals {
  props = {
    "sql.current-catalog"  = var.environment_name
    "sql.current-database" = var.cluster_name
  }

  statements = {
    stats_table     = file("${path.module}/../flink-sql/10-table-customer-payment-stats.sql")
    stats_insert    = file("${path.module}/../flink-sql/11-insert-customer-payment-stats.sql")
    decisions_table = file("${path.module}/../flink-sql/20-table-loan-decisions.sql")
    decisions_insert = templatefile(
      "${path.module}/../flink-sql/21-insert-loan-decisions.sql.tftpl", var.scoring
    )
  }
}

resource "confluent_flink_statement" "stats_table" {
  statement_name = "${var.project_name}-ddl-customer-payment-stats"
  statement      = local.statements.stats_table
  properties     = local.props

  organization { id = var.organization_id }
  environment { id = var.environment_id }
  compute_pool { id = var.compute_pool_id }
  principal { id = var.statements_runner_id }
  rest_endpoint = var.flink_rest_endpoint
  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }
}

resource "confluent_flink_statement" "decisions_table" {
  statement_name = "${var.project_name}-ddl-loan-decisions"
  statement      = local.statements.decisions_table
  properties     = local.props

  organization { id = var.organization_id }
  environment { id = var.environment_id }
  compute_pool { id = var.compute_pool_id }
  principal { id = var.statements_runner_id }
  rest_endpoint = var.flink_rest_endpoint
  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }
}

resource "confluent_flink_statement" "stats_insert" {
  statement_name = "${var.project_name}-dml-customer-payment-stats"
  statement      = local.statements.stats_insert
  properties     = local.props

  organization { id = var.organization_id }
  environment { id = var.environment_id }
  compute_pool { id = var.compute_pool_id }
  principal { id = var.statements_runner_id }
  rest_endpoint = var.flink_rest_endpoint
  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }

  depends_on = [confluent_flink_statement.stats_table]
}

resource "confluent_flink_statement" "decisions_insert" {
  statement_name = "${var.project_name}-dml-loan-decisions"
  statement      = local.statements.decisions_insert
  # ISSUE-1: marca como inactivas las particiones sin tráfico tras 5 s para que el watermark del temporal join avance.
  properties = merge(local.props, { "sql.tables.scan.idle-timeout" = "5 s" })

  organization { id = var.organization_id }
  environment { id = var.environment_id }
  compute_pool { id = var.compute_pool_id }
  principal { id = var.statements_runner_id }
  rest_endpoint = var.flink_rest_endpoint
  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }

  depends_on = [
    confluent_flink_statement.decisions_table,
    confluent_flink_statement.stats_insert,
  ]
}
