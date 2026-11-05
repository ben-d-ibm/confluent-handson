import { Header, HeaderName, InlineNotification, Theme } from '@carbon/react';
import Charts from './components/Charts.jsx';
import DecisionsTable from './components/DecisionsTable.jsx';
import Kpis from './components/Kpis.jsx';
import { useDashboardData } from './useDashboardData.js';

// Pantalla única. Solo muestra lo que devuelve el backend: sin scoring local ni datos simulados.
export default function App({ fetchImpl, intervalMs }) {
  const { data, stale } = useDashboardData({ fetchImpl, intervalMs });

  return (
    <Theme theme="white">
      <Header aria-label="Bank loan decisions">
        <HeaderName prefix="">Loan decisions</HeaderName>
      </Header>
      <main className="page">
        <h1 className="page__title">Decisions dashboard</h1>
        <p className="page__lead">Loan decisions produced by the scoring pipeline, refreshed every 3 seconds.</p>

        {stale && (
          <InlineNotification
            kind="warning"
            lowContrast
            hideCloseButton
            role="status"
            title={data ? 'Data may be outdated.' : 'Dashboard data is not available yet.'}
            subtitle={data ? 'Showing the last information received. Retrying…' : 'Retrying…'}
          />
        )}

        {data === null ? (
          !stale && <p className="state-message">Loading…</p>
        ) : (
          <>
            <Kpis summary={data.summary} />
            {data.summary.total === 0 ? (
              <div className="empty-state">
                <p className="empty-state__title">No decisions received yet</p>
                <p className="empty-state__hint">
                  Submit a loan request; its decision will appear here when the pipeline produces it.
                </p>
              </div>
            ) : (
              <>
                <Charts summary={data.summary} />
                <DecisionsTable decisions={data.decisions} />
              </>
            )}
          </>
        )}
      </main>
    </Theme>
  );
}
