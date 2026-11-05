#!/usr/bin/env bash
# Genera la configuración del worker de Kafka Connect desde variables de entorno y lo arranca.
set -euo pipefail

for v in KAFKA_BOOTSTRAP_SERVERS CONNECT_KAFKA_API_KEY CONNECT_KAFKA_API_SECRET \
         SCHEMA_REGISTRY_URL CONNECT_SR_API_KEY CONNECT_SR_API_SECRET \
         CONNECT_GROUP_ID CONNECT_TOPIC_PREFIX; do
  : "${!v:?${v} es obligatoria}"
done

umask 077
CFG="/tmp/connect-distributed.properties"
JAAS="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${CONNECT_KAFKA_API_KEY}\" password=\"${CONNECT_KAFKA_API_SECRET}\";"

{
  echo "bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS}"
  echo "group.id=${CONNECT_GROUP_ID}"
  echo "plugin.path=/opt/connect-plugins"
  echo "listeners=http://0.0.0.0:8083"
  echo "rest.advertised.host.name=localhost"
  # Los tópicos los gestiona Terraform: Connect no los crea (y no exige replication.factor por conector)
  echo "topic.creation.enable=false"

  # Tópicos internos (los crea Terraform, compactados)
  echo "config.storage.topic=${CONNECT_TOPIC_PREFIX}configs"
  echo "offset.storage.topic=${CONNECT_TOPIC_PREFIX}offsets"
  echo "status.storage.topic=${CONNECT_TOPIC_PREFIX}status"
  echo "config.storage.replication.factor=3"
  echo "offset.storage.replication.factor=3"
  echo "status.storage.replication.factor=3"

  # Converters por defecto (cada conector puede sobreescribirlos)
  echo "key.converter=org.apache.kafka.connect.json.JsonConverter"
  echo "value.converter=org.apache.kafka.connect.json.JsonConverter"

  # Permite ${env:VAR} en la config de los conectores: los secretos no viajan por la REST ni se guardan en claro
  echo "config.providers=env"
  echo "config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider"
  echo "config.providers.env.param.allowlist.pattern=^(CDC_PASSWORD|CONNECT_SR_API_KEY|CONNECT_SR_API_SECRET)\$"

  # TLS + SASL/PLAIN hacia Confluent Cloud en worker, productor, consumidor y admin
  for p in "" "producer." "consumer." "admin."; do
    echo "${p}security.protocol=SASL_SSL"
    echo "${p}sasl.mechanism=PLAIN"
    echo "${p}sasl.jaas.config=${JAAS}"
    echo "${p}request.timeout.ms=30000"
  done
  echo "producer.client.dns.lookup=use_all_dns_ips"
  echo "consumer.client.dns.lookup=use_all_dns_ips"
  echo "admin.client.dns.lookup=use_all_dns_ips"
  echo "client.dns.lookup=use_all_dns_ips"
} > "${CFG}"

echo "{\"level\":\"INFO\",\"msg\":\"starting kafka connect\",\"group\":\"${CONNECT_GROUP_ID}\"}"
exec /opt/kafka/bin/connect-distributed.sh "${CFG}"
