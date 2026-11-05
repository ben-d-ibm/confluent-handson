// Cliente de la API del dashboard. Rutas relativas: sin hosts ni puertos. Solo lectura.
export const POLL_INTERVAL_MS = 3000;
export const DECISIONS_LIMIT = 50;

async function getJson(path, fetchImpl) {
  const response = await fetchImpl(path, { headers: { accept: 'application/json' } });
  if (!response.ok) throw new Error('request failed');
  return response.json();
}

/** Resumen + últimas decisiones. Lanza si cualquiera de las dos peticiones falla. */
export async function fetchDashboard(fetchImpl = fetch) {
  const [summary, decisions] = await Promise.all([
    getJson('/api/summary', fetchImpl),
    getJson(`/api/decisions?limit=${DECISIONS_LIMIT}`, fetchImpl),
  ]);
  if (typeof summary?.total !== 'number' || !Array.isArray(decisions?.decisions)) {
    throw new Error('unexpected response');
  }
  return { summary, decisions: decisions.decisions };
}
