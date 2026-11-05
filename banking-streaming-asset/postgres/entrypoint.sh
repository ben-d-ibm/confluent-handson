#!/usr/bin/env bash
# Arranque de PostgreSQL: initdb la primera vez, aplica los SQL de /docker-init y lanza el servidor.
set -euo pipefail

: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD es obligatoria}"
: "${CDC_PASSWORD:?CDC_PASSWORD es obligatoria}"
DB_NAME="${POSTGRES_DB:-bank}"
PGBIN="/usr/pgsql-${PG_MAJOR}/bin"
export PGDATA="${PGDATA:-/var/lib/pgsql/data}"

SERVER_OPTS=(-c wal_level=logical -c max_wal_senders=4 -c max_replication_slots=4)

MARKER="${PGDATA}/.banking_initialized"
if [[ ! -f "${MARKER}" ]]; then
  # Una carga anterior incompleta se descarta y se repite desde cero
  find "${PGDATA}" -mindepth 1 -delete 2>/dev/null || true
  echo "{\"level\":\"INFO\",\"msg\":\"initdb\",\"pgdata\":\"${PGDATA}\"}"
  pwfile="$(mktemp)"; trap 'rm -f "${pwfile}"' EXIT
  printf '%s' "${POSTGRES_PASSWORD}" > "${pwfile}"
  "${PGBIN}/initdb" -D "${PGDATA}" -U postgres --pwfile="${pwfile}" \
    --auth-local=scram-sha-256 --auth-host=scram-sha-256 --encoding=UTF8 --locale=C.UTF-8 >/dev/null

  # Servidor temporal solo por socket Unix para cargar el esquema y los datos
  "${PGBIN}/pg_ctl" -D "${PGDATA}" -o "-c listen_addresses='' -c unix_socket_directories=/tmp -c wal_level=logical" -w start >/dev/null
  export PGPASSWORD="${POSTGRES_PASSWORD}"
  psql_base=("${PGBIN}/psql" -h /tmp -U postgres -v ON_ERROR_STOP=1 -q)
  "${psql_base[@]}" -d postgres -c "CREATE DATABASE ${DB_NAME}"
  for f in /docker-init/*.sql; do
    echo "{\"level\":\"INFO\",\"msg\":\"init-sql\",\"file\":\"$(basename "${f}")\"}"
    "${psql_base[@]}" -d "${DB_NAME}" -v "cdc_password=${CDC_PASSWORD}" -f "${f}"
  done
  unset PGPASSWORD

  # Acceso del CDC desde la red interna del compose: solo el rol cdc, solo rangos privados, siempre SCRAM.
  # (El puerto se publica únicamente en 127.0.0.1 del host.)
  {
    for net in 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16; do
      echo "host  bank         cdc  ${net}  scram-sha-256"
      echo "host  replication  cdc  ${net}  scram-sha-256"
    done
  } >> "${PGDATA}/pg_hba.conf"
  "${PGBIN}/pg_ctl" -D "${PGDATA}" -m fast -w stop >/dev/null
  rm -f "${pwfile}"; trap - EXIT
  touch "${MARKER}"
fi

# Dentro del contenedor escucha en todas las interfaces; el puerto se publica solo en 127.0.0.1 (compose)
exec "${PGBIN}/postgres" -D "${PGDATA}" -c listen_addresses='*' "${SERVER_OPTS[@]}"
