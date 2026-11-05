#!/usr/bin/env bash
# Despliega la infraestructura de Confluent Cloud con Terraform y genera .env.runtime.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

"${ROOT_DIR}/scripts/check.sh"
load_env

info "== Terraform init =="
terraform -chdir="${TF_DIR}" init -input=false

info "== Terraform apply =="
terraform -chdir="${TF_DIR}" apply -input=false -auto-approve

"${ROOT_DIR}/scripts/gen-env.sh"

ok "Infraestructura de Confluent Cloud lista."
info "Para darlo todo de baja y dejar de consumir recursos:  make destroy"
