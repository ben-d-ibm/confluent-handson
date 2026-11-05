SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help init check up cloud-up local-up connector logs gen-env down destroy status fmt validate flink-check flink-up loans smoke demo-traffic demo-traffic-stop

help: ## Muestra esta ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  make %-18s %s\n", $$1, $$2}'

init: ## Crea .env desde .env.example con contraseñas aleatorias
	@./scripts/init-env.sh

check: ## Valida el entorno (no crea nada)
	@./scripts/check.sh

cloud-up: ## Despliega solo Confluent Cloud con Terraform
	@./scripts/cloud-up.sh

local-up: ## Levanta Postgres + Kafka Connect/Debezium y registra el conector
	@./scripts/local-up.sh

connector: ## (Re)registra el conector Debezium
	@./scripts/register-connector.sh

logs: ## Muestra los logs de los contenedores locales
	@source scripts/lib.sh && load_env && rt=$$(detect_runtime) && cc=$$(detect_compose $$rt) && \
	  $$cc -f compose/compose.yaml --env-file .env --env-file .env.runtime logs --tail=80

flink-check: ## Comprueba Flink y los tópicos CDC con consultas acotadas
	@./scripts/flink-check.sh

flink-up: ## Despliega las sentencias de Flink (stats + decisiones) con Terraform
	@./scripts/flink-up.sh

loans: ## Genera solicitudes de prueba: make loans ARGS="-n 5 -m 1"
	@./scripts/generate-loans.sh $(ARGS)

smoke: ## Smoke test end-to-end (4 casos deterministas, con plazo)
	@./scripts/smoke-test.sh

demo-traffic: ## Inicia tráfico de demo continuo (1 solicitud cada 10 s) vía producer-java
	@./scripts/demo-traffic.sh start

demo-traffic-stop: ## Para el tráfico de demo
	@./scripts/demo-traffic.sh stop

up: cloud-up local-up flink-up ## Levanta todo: Confluent Cloud + contenedores locales (postgres, connect, :8080, :8081) + conector + Flink

gen-env: ## Regenera .env.runtime desde los outputs de Terraform
	@./scripts/gen-env.sh

down: ## Para y borra contenedores y volúmenes locales (no toca la nube)
	@./scripts/down.sh local

destroy: ## DA DE BAJA TODO: contenedores locales + recursos de Confluent Cloud
	@./scripts/down.sh all

status: ## Muestra los recursos gestionados por Terraform
	@source scripts/lib.sh && load_env && terraform -chdir=terraform state list

fmt: ## Formatea el código Terraform
	@terraform -chdir=terraform fmt -recursive && terraform -chdir=terraform-flink fmt -recursive

validate: ## Valida la sintaxis de Terraform
	@terraform -chdir=terraform init -backend=false -input=false >/dev/null && terraform -chdir=terraform validate && \
	terraform -chdir=terraform-flink init -backend=false -input=false >/dev/null && terraform -chdir=terraform-flink validate
