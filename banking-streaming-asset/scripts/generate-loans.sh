#!/usr/bin/env bash
# Genera solicitudes de préstamo en el tópico loan-requests (INSERT ... VALUES de Flink; usa el esquema Avro registrado).
#   scripts/generate-loans.sh -n 5                 5 solicitudes aleatorias de clientes existentes (1001-1050)
#   scripts/generate-loans.sh -m 1                 1 solicitud de un cliente inexistente (caso borde)
#   scripts/generate-loans.sh -r 1001:5000:24      solicitud concreta cliente:importe:plazo_meses (repetible)
# request_id = <GEN_PREFIX>-<GEN_RUN>-<n> (por defecto "gen" y la hora actual). El smoke test fija ambos para correlacionar.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

N=0; MISSING=0; SPECIFIC=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -n) N="$2"; shift 2 ;;
    -m) MISSING="$2"; shift 2 ;;
    -r) SPECIFIC+=("$2"); shift 2 ;;
    *) die "Uso: $0 [-n N] [-m M] [-r cliente:importe:plazo]..." ;;
  esac
done
[[ "${N}" =~ ^[0-9]+$ && "${MISSING}" =~ ^[0-9]+$ ]] || die "-n y -m deben ser enteros"
(( N + MISSING + ${#SPECIFIC[@]} > 0 )) || die "Indica al menos una solicitud (-n, -m o -r)."
(( N + MISSING + ${#SPECIFIC[@]} <= 200 )) || die "Máximo 200 solicitudes por ejecución."

PREFIX="${GEN_PREFIX:-gen}"
RUN="${GEN_RUN:-$(date +%s)}"
[[ "${RUN}" =~ ^[0-9a-z]+$ && "${PREFIX}" =~ ^[0-9a-z-]+$ ]] || die "GEN_RUN y GEN_PREFIX solo admiten [0-9a-z] (y - en el prefijo)."
purposes=(HOME CAR EDUCATION PERSONAL BUSINESS)
values=(); seq=0

add() { # cliente importe plazo
  local c="$1" a="$2" t="$3" p="${purposes[RANDOM % ${#purposes[@]}]}"
  [[ "${c}" =~ ^[0-9]+$ && "${a}" =~ ^[0-9]+(\.[0-9]{1,2})?$ && "${t}" =~ ^[0-9]+$ ]] || die "Solicitud inválida: ${c}:${a}:${t}"
  seq=$(( seq + 1 ))
  values+=("('${PREFIX}-${RUN}-${seq}', ${c}, CAST(${a} AS DECIMAL(12, 2)), ${t}, '${p}', CURRENT_TIMESTAMP)")
}

for spec in ${SPECIFIC[@]+"${SPECIFIC[@]}"}; do IFS=: read -r c a t <<<"${spec}"; add "${c}" "${a}" "${t}"; done
for ((i = 0; i < N; i++)); do add $(( 1001 + RANDOM % 50 )) $(( (1 + RANDOM % 40) * 500 )) $(( (1 + RANDOM % 5) * 12 )); done
for ((i = 0; i < MISSING; i++)); do add $(( 90000 + RANDOM % 1000 )) 5000 24; done

sql="INSERT INTO \`loan-requests\` (request_id, customer_id, amount, term_months, purpose, requested_at) VALUES $(IFS=,; echo "${values[*]}")"
info "Enviando ${#values[@]} solicitud(es) (prefijo ${PREFIX}-${RUN}) ..."
bash "${ROOT_DIR}/scripts/flink-sql.sh" --timeout 90 "${sql}" >/dev/null && ok "Solicitudes enviadas: ${PREFIX}-${RUN}-1 .. ${PREFIX}-${RUN}-${#values[@]}"
