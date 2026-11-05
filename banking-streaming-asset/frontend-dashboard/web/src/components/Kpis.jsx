import { Tile } from '@carbon/react';
import { formatAmount, formatPercent, formatScore } from '../format.js';

export default function Kpis({ summary }) {
  const items = [
    ['Total decisions', String(summary.total)],
    ['Approved', String(summary.approved)],
    ['Rejected', String(summary.rejected)],
    ['Approval rate', formatPercent(summary.approval_rate)],
    ['Average score', formatScore(summary.average_score)],
    ['Total approved amount', formatAmount(summary.total_approved_amount)],
  ];
  return (
    <section className="kpis" aria-label="Key figures">
      {items.map(([label, value]) => (
        <Tile key={label} className="kpi" data-testid="kpi">
          <div className="kpi__label">{label}</div>
          <div className="kpi__value">{value}</div>
        </Tile>
      ))}
    </section>
  );
}
