import { useEffect, useState } from 'react';
import { fetchDashboard, POLL_INTERVAL_MS } from './api.js';

/**
 * Polling HTTP simple: consulta, espera `intervalMs` y vuelve a consultar.
 * Si una consulta falla se conserva la última información válida y se marca `stale`;
 * el siguiente ciclo reintenta sin más lógica.
 */
export function useDashboardData({ fetchImpl = fetch, intervalMs = POLL_INTERVAL_MS } = {}) {
  const [data, setData] = useState(null); // { summary, decisions } | null
  const [stale, setStale] = useState(false);

  useEffect(() => {
    let cancelled = false;
    let timer;

    const tick = async () => {
      try {
        const next = await fetchDashboard(fetchImpl);
        if (cancelled) return;
        setData(next);
        setStale(false);
      } catch {
        if (cancelled) return;
        setStale(true);
      }
      if (!cancelled) timer = setTimeout(tick, intervalMs);
    };
    tick();

    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [fetchImpl, intervalMs]);

  return { data, stale };
}
