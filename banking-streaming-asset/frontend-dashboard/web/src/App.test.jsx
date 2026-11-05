import { render, screen, waitFor, within } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

// Carbon Charts dibuja SVG con d3 y no funciona bien en jsdom: se sustituye por un stub que expone los datos recibidos.
vi.mock('@carbon/charts-react', () => ({
  DonutChart: ({ data }) => <div data-testid="donut" data-chart={JSON.stringify(data)} />,
  SimpleBarChart: ({ data }) => <div data-testid="score-chart" data-chart={JSON.stringify(data)} />,
}));

import App from './App.jsx';
import { donutData, scoreData } from './components/Charts.jsx';

const SUMMARY = {
  total: 3,
  approved: 2,
  rejected: 1,
  approval_rate: 0.6667,
  average_score: 71.5,
  total_approved_amount: '25000.50',
  score_distribution: [
    { range: '0-19', count: 0 },
    { range: '20-39', count: 1 },
    { range: '40-59', count: 0 },
    { range: '60-79', count: 1 },
    { range: '80-100', count: 1 },
  ],
};

const DECISIONS = [
  {
    request_id: 'req-1',
    customer_id: 101,
    amount: '10000.00',
    term_months: 24,
    purpose: 'CAR',
    decided_at: '2026-11-04T17:55:57.000Z',
    score: 85,
    decision: 'APPROVED',
    reasons: 'GOOD_HISTORY',
  },
  {
    request_id: 'req-2',
    customer_id: 102,
    amount: '15000.50',
    term_months: 36,
    purpose: 'HOME',
    decided_at: '2026-11-04T17:56:10.000Z',
    score: 70,
    decision: 'APPROVED',
    reasons: 'OK',
  },
  {
    request_id: 'req-3',
    customer_id: 103,
    amount: '5000.00',
    term_months: 12,
    purpose: 'TRAVEL',
    decided_at: '2026-11-04T17:57:00.000Z',
    score: 30,
    decision: 'REJECTED',
    reasons: 'LATE_PAYMENTS',
  },
];

const EMPTY_SUMMARY = {
  total: 0,
  approved: 0,
  rejected: 0,
  approval_rate: null,
  average_score: null,
  total_approved_amount: '0.00',
  score_distribution: SUMMARY.score_distribution.map(({ range }) => ({ range, count: 0 })),
};

function json(body) {
  return Promise.resolve({ ok: true, json: () => Promise.resolve(body) });
}

function apiWith(summary, decisions) {
  return vi.fn((path) => (path.startsWith('/api/summary') ? json(summary) : json({ decisions })));
}

describe('dashboard', () => {
  it('muestra los KPIs devueltos por el backend', async () => {
    render(<App fetchImpl={apiWith(SUMMARY, DECISIONS)} intervalMs={60000} />);
    const kpis = await screen.findAllByTestId('kpi');
    const text = kpis.map((k) => k.textContent);
    expect(text).toEqual([
      'Total decisions3',
      'Approved2',
      'Rejected1',
      'Approval rate66.7%',
      'Average score71.5',
      'Total approved amount25,000.50',
    ]);
  });

  it('muestra el estado vacío exacto cuando no hay decisiones', async () => {
    render(<App fetchImpl={apiWith(EMPTY_SUMMARY, [])} intervalMs={60000} />);
    expect(await screen.findByText('No decisions received yet')).toBeInTheDocument();
    expect(screen.queryByTestId('donut')).not.toBeInTheDocument();
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
  });

  it('el donut recibe Approved y Rejected del resumen', async () => {
    render(<App fetchImpl={apiWith(SUMMARY, DECISIONS)} intervalMs={60000} />);
    const donut = await screen.findByTestId('donut');
    expect(JSON.parse(donut.dataset.chart)).toEqual([
      { group: 'Approved', value: 2 },
      { group: 'Rejected', value: 1 },
    ]);
  });

  it('el gráfico de score recibe la distribución del resumen', async () => {
    render(<App fetchImpl={apiWith(SUMMARY, DECISIONS)} intervalMs={60000} />);
    const chart = await screen.findByTestId('score-chart');
    expect(JSON.parse(chart.dataset.chart)).toEqual([
      { group: 'Decisions', key: '0-19', value: 0 },
      { group: 'Decisions', key: '20-39', value: 1 },
      { group: 'Decisions', key: '40-59', value: 0 },
      { group: 'Decisions', key: '60-79', value: 1 },
      { group: 'Decisions', key: '80-100', value: 1 },
    ]);
  });

  it('la tabla muestra las decisiones APPROVED y REJECTED', async () => {
    render(<App fetchImpl={apiWith(SUMMARY, DECISIONS)} intervalMs={60000} />);
    const table = await screen.findByRole('table');
    const headers = within(table).getAllByRole('columnheader').map((h) => h.textContent);
    expect(headers).toEqual([
      'Request ID',
      'Customer',
      'Amount',
      'Purpose',
      'Score',
      'Decision',
      'Reasons',
      'Decided at',
    ]);
    const rows = within(table).getAllByRole('row').slice(1);
    expect(rows).toHaveLength(3);
    expect(rows[0]).toHaveTextContent('req-1');
    expect(rows[0]).toHaveTextContent('10,000.00');
    expect(rows[0]).toHaveTextContent('APPROVED');
    expect(rows[0]).toHaveTextContent('2026-11-04 17:55:57');
    expect(rows[2]).toHaveTextContent('req-3');
    expect(rows[2]).toHaveTextContent('REJECTED');
    expect(rows[2]).toHaveTextContent('LATE_PAYMENTS');
  });

  it('repite las consultas a la API en cada ciclo de polling', async () => {
    const fetchImpl = apiWith(SUMMARY, DECISIONS);
    render(<App fetchImpl={fetchImpl} intervalMs={20} />);
    await waitFor(() => {
      const summaryCalls = fetchImpl.mock.calls.filter(([p]) => p === '/api/summary').length;
      expect(summaryCalls).toBeGreaterThanOrEqual(3);
    });
    expect(fetchImpl).toHaveBeenCalledWith('/api/decisions?limit=50', expect.anything());
  });

  it('un error de la API no rompe la pantalla y conserva los últimos datos', async () => {
    let failing = false;
    const fetchImpl = vi.fn((path) => {
      if (failing) return Promise.resolve({ ok: false, json: () => Promise.resolve({}) });
      return path.startsWith('/api/summary') ? json(SUMMARY) : json({ decisions: DECISIONS });
    });
    render(<App fetchImpl={fetchImpl} intervalMs={20} />);
    await screen.findByRole('table');

    failing = true;
    expect(await screen.findByText('Data may be outdated.')).toBeInTheDocument();
    expect(screen.getByRole('table')).toBeInTheDocument();
    expect(screen.getAllByTestId('kpi')).toHaveLength(6);

    failing = false;
    await waitFor(() => expect(screen.queryByText('Data may be outdated.')).not.toBeInTheDocument());
  });

  it('si la API falla desde el principio muestra aviso y no revienta', async () => {
    const fetchImpl = vi.fn(() => Promise.reject(new Error('down')));
    render(<App fetchImpl={fetchImpl} intervalMs={60000} />);
    expect(await screen.findByText('Dashboard data is not available yet.')).toBeInTheDocument();
    expect(screen.queryByTestId('kpi')).not.toBeInTheDocument();
  });
});

describe('adaptadores de datos de los gráficos', () => {
  it('donutData y scoreData no alteran los valores del resumen', () => {
    expect(donutData(SUMMARY)).toEqual([
      { group: 'Approved', value: 2 },
      { group: 'Rejected', value: 1 },
    ]);
    expect(scoreData(SUMMARY).map((d) => d.value)).toEqual([0, 1, 0, 1, 1]);
  });
});
