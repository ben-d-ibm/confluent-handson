#!/usr/bin/env bash
# Comprobación de salud de Flink + CDC con consultas ACOTADAS (nunca se quedan colgadas).
#   1) Conectividad/autenticación: SHOW TABLES.
#   2) Tópicos CDC visibles como tablas Flink.
#   3) Conteo por tabla con 'scan.bounded.mode'='latest-offset' (la consulta termina sola).
# Nota: un COUNT(*) sobre una tabla sin acotar es una consulta streaming que no termina; no usarlo como health check.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
SQL="${ROOT_DIR}/scripts/flink-sql.sh"
rc_all=0

info "== 1/3 Conectividad y autenticación (SHOW TABLES) =="
tables="$(bash "${SQL}" --timeout 45 "SHOW TABLES" 2>/dev/null | jq -r '.row[0]')" \
  || die "Flink no responde o las credenciales no son válidas."
ok "Flink responde"

info "== 2/3 Tablas CDC =="
for t in customers payment_history; do
  if grep -qx "${t}" <<<"${tables}"; then ok "tabla ${t}"; else fail "falta la tabla ${t}"; rc_all=1; fi
done

info "== 3/3 Conteo acotado (latest-offset) =="
for t in customers payment_history; do
  # Changelog: el valor final es la última fila con op +I (0) o +U (2)
  n="$(bash "${SQL}" --timeout 60 --max-rows 20000 \
        "SELECT COUNT(*) AS n FROM ${t} /*+ OPTIONS('scan.bounded.mode'='latest-offset') */" 2>/dev/null \
        | jq -rs '[.[] | select(.op==0 or .op==2)] | last | .row[0] // empty')" || n=""
  if [[ -n "${n}" ]]; then ok "${t}: ${n} filas en Flink"; else fail "${t}: no se pudo contar"; rc_all=1; fi
done
exit "${rc_all}"
