import { describe, it, expect, vi, afterEach } from 'vitest';
import { render, screen, cleanup } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import SubmissionResult, { DASHBOARD_URL } from './SubmissionResult.jsx';

afterEach(cleanup);

const renderResult = (onReset = () => {}) =>
  render(<SubmissionResult requestId="req-1" requestedAt="2026-11-04T14:17:39.729Z" onReset={onReset} />);

describe('SubmissionResult — solo acuse de recepción', () => {
  it('muestra título, request_id, requested_at y el mensaje de seguimiento en el dashboard', () => {
    renderResult();
    expect(screen.getByText('Loan request submitted')).toBeInTheDocument();
    expect(screen.getByTestId('request-id')).toHaveTextContent('req-1');
    expect(screen.getByTestId('requested-at')).toHaveTextContent('2026-11-04T14:17:39.729Z');
    expect(
      screen.getByText('The application is being processed. View real-time decisions in the dashboard.'),
    ).toBeInTheDocument();
  });

  it('"Open Dashboard" abre http://127.0.0.1:8081 en otra pestaña sin opener', () => {
    renderResult();
    const link = screen.getByRole('link', { name: /open dashboard/i });
    expect(link).toHaveAttribute('href', 'http://127.0.0.1:8081');
    expect(DASHBOARD_URL).toBe('http://127.0.0.1:8081');
    expect(link).toHaveAttribute('target', '_blank');
    expect(link).toHaveAttribute('rel', expect.stringContaining('noopener'));
  });

  it('no muestra Processing, APPROVED/REJECTED, score ni reasons', () => {
    const { container } = renderResult();
    expect(container.textContent).not.toMatch(/processing application|approved|rejected|score|reasons/i);
  });

  it('"Submit another request" llama a onReset', async () => {
    const onReset = vi.fn();
    renderResult(onReset);
    await userEvent.setup().click(screen.getByRole('button', { name: /submit another request/i }));
    expect(onReset).toHaveBeenCalledTimes(1);
  });
});
