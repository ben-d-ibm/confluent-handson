// Formateo para mostrar. No calcula nada de negocio.

const NUMBER = { minimumFractionDigits: 2, maximumFractionDigits: 2 };

/** "10000.00" -> "10,000.00" */
export function formatAmount(amount) {
  const n = Number(amount);
  return Number.isFinite(n) ? n.toLocaleString('en-US', NUMBER) : '—';
}

/** 0.4117 -> "41.2%"; null -> "—" */
export function formatPercent(rate) {
  return typeof rate === 'number' ? `${(rate * 100).toFixed(1)}%` : '—';
}

export function formatScore(score) {
  return typeof score === 'number' ? score.toFixed(1) : '—';
}

/** ISO 8601 -> "2026-11-04 17:55:57" (UTC, determinista). */
export function formatDateTime(iso) {
  return typeof iso === 'string' ? iso.replace('T', ' ').slice(0, 19) : '—';
}
