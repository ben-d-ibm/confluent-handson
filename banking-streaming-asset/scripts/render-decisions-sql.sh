#!/usr/bin/env bash
# Renderiza el SQL de decisiones con los umbrales de var.scoring (misma plantilla que usa Terraform).
# Útil para revisar o probar la lógica de forma ad hoc. Salida: SQL por stdout.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
terraform -chdir="${TF_DIR}" init -input=false >/dev/null
echo 'jsonencode(templatefile("${path.module}/../flink-sql/21-insert-loan-decisions.sql.tftpl", var.scoring))' \
  | terraform -chdir="${TF_DIR}" console 2>/dev/null | jq -r 'fromjson'
