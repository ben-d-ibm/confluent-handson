# banking-streaming-asset

Demo de streaming bancario: PostgreSQL → Debezium → Confluent Cloud → Flink SQL → dashboard.

- Solicitudes de préstamo: <http://127.0.0.1:8080>
- Dashboard de decisiones en tiempo real: <http://127.0.0.1:8081>

## Comandos

| Comando | Qué hace |
|---|---|
| `make up` | Levanta todo: Confluent Cloud (Terraform), contenedores locales (Postgres, Connect/Debezium, :8080, :8081), conector y sentencias de Flink. **No** inicia tráfico automático. |
| `make demo-traffic` | Inicia tráfico de demo continuo con `producer-java`: 1 solicitud cada 10 s (≈6/min; cambiable con `DEMO_TRAFFIC_INTERVAL_SECONDS`) para clientes 1001–1050. Las decisiones aparecen en el dashboard. |
| `make demo-traffic-stop` | Para y elimina el contenedor de tráfico de demo. |
| `make destroy` | Da de baja todo: contenedores y volúmenes locales (incluido el tráfico de demo) y recursos de Confluent Cloud. |
