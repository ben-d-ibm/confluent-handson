import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow, Tag } from '@carbon/react';
import { formatAmount, formatDateTime } from '../format.js';

const HEADERS = ['Request ID', 'Customer', 'Amount', 'Purpose', 'Score', 'Decision', 'Reasons', 'Decided at'];

export default function DecisionsTable({ decisions }) {
  return (
    <section className="decisions" aria-label="Recent decisions">
      <h2 className="section-title">Recent decisions</h2>
      <div className="decisions__scroll">
        <Table size="md" useZebraStyles>
          <TableHead>
            <TableRow>
              {HEADERS.map((h) => (
                <TableHeader key={h}>{h}</TableHeader>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {decisions.map((d) => (
              <TableRow key={d.request_id}>
                <TableCell className="mono">{d.request_id}</TableCell>
                <TableCell>{d.customer_id}</TableCell>
                <TableCell>{formatAmount(d.amount)}</TableCell>
                <TableCell>{d.purpose}</TableCell>
                <TableCell>{d.score}</TableCell>
                <TableCell>
                  <Tag type={d.decision === 'APPROVED' ? 'green' : 'red'} size="sm">
                    {d.decision}
                  </Tag>
                </TableCell>
                <TableCell>{d.reasons}</TableCell>
                <TableCell>{formatDateTime(d.decided_at)}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </section>
  );
}
