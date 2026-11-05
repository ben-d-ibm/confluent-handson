#!/usr/bin/env bash
# Crea .env a partir de .env.example y genera contraseñas aleatorias para PostgreSQL.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

[[ -f "${ENV_FILE}" ]] && die ".env ya existe. Edítalo a mano o bórralo para regenerarlo."

command -v openssl >/dev/null 2>&1 || die "Se necesita openssl para generar contraseñas."

umask 077
cp "${ROOT_DIR}/.env.example" "${ENV_FILE}"

pg_pass="$(openssl rand -hex 16)"
cdc_pass="$(openssl rand -hex 16)"

# sed portable (macOS/Linux): se escribe a un temporal y se mueve
tmp="$(mktemp)"
sed -e "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=${pg_pass}|" \
    -e "s|^CDC_PASSWORD=.*|CDC_PASSWORD=${cdc_pass}|" "${ENV_FILE}" > "${tmp}"
mv "${tmp}" "${ENV_FILE}"
chmod 600 "${ENV_FILE}"

ok ".env creado con permisos 600."
info "Siguiente paso: edita .env y rellena CONFLUENT_CLOUD_API_KEY y CONFLUENT_CLOUD_API_SECRET."
