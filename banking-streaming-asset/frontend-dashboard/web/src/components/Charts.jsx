import { DonutChart, SimpleBarChart } from '@carbon/charts-react';

// Los datos salen tal cual del resumen del backend; aquí solo se adaptan al formato de Carbon Charts.

export function donutData(summary) {
  return [
    { group: 'Approved', value: summary.approved },
    { group: 'Rejected', value: summary.rejected },
  ];
}

export function scoreData(summary) {
  return summary.score_distribution.map(({ range, count }) => ({ group: 'Decisions', key: range, value: count }));
}

const DONUT_OPTIONS = {
  title: 'Approved vs rejected',
  resizable: true,
  height: '320px',
  theme: 'white',
  donut: { center: { label: 'Decisions' }, alignment: 'center' },
  color: { scale: { Approved: '#24a148', Rejected: '#da1e28' } },
};

const SCORE_OPTIONS = {
  title: 'Score distribution',
  resizable: true,
  height: '320px',
  theme: 'white',
  legend: { enabled: false },
  axes: {
    left: { mapsTo: 'value', title: 'Decisions', includeZero: true },
    bottom: { mapsTo: 'key', scaleType: 'labels', title: 'Score range' },
  },
  color: { scale: { Decisions: '#0f62fe' } },
};

export default function Charts({ summary }) {
  return (
    <section className="charts" aria-label="Charts">
      <div className="chart-panel">
        <DonutChart data={donutData(summary)} options={DONUT_OPTIONS} />
      </div>
      <div className="chart-panel">
        <SimpleBarChart data={scoreData(summary)} options={SCORE_OPTIONS} />
      </div>
    </section>
  );
}
