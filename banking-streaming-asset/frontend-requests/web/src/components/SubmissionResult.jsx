import { Button, InlineNotification } from '@carbon/react';

// Único destino externo permitido de la UI (dashboard local de la demo).
export const DASHBOARD_URL = 'http://127.0.0.1:8081';

/**
 * Tras el envío: solo acuse de recepción. La UI no consulta ni muestra decisiones;
 * las decisiones en tiempo real se ven en el dashboard.
 */
export default function SubmissionResult({ requestId, requestedAt, onReset }) {
  return (
    <section aria-label="Submission result">
      <InlineNotification
        kind="success"
        lowContrast
        hideCloseButton
        title="Loan request submitted"
        subtitle="The application is being processed. View real-time decisions in the dashboard."
        data-testid="submission-confirmation"
      />
      <dl className="result-list">
        <dt>Request ID</dt>
        <dd data-testid="request-id">{requestId}</dd>
        <dt>Requested at</dt>
        <dd data-testid="requested-at">{requestedAt}</dd>
      </dl>
      <Button kind="primary" href={DASHBOARD_URL} target="_blank" rel="noopener noreferrer">
        Open Dashboard
      </Button>{' '}
      <Button kind="tertiary" onClick={onReset}>
        Submit another request
      </Button>
    </section>
  );
}
