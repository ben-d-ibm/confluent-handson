import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import App from './App.jsx';

const ACK = { request_id: 'c6648727-644b-48d0-9da1-f4dfc9fe6847', requested_at: '2026-11-04T14:17:39.729Z' };
const json = (status, body) => new Response(JSON.stringify(body), { status });

let fetchMock;
beforeEach(() => {
  fetchMock = vi.fn();
  vi.stubGlobal('fetch', fetchMock);
});
afterEach(() => vi.unstubAllGlobals());

const submitButton = () => screen.getByRole('button', { name: /submit request|submitting/i });

describe('formulario', () => {
  it('renderiza los campos con los valores iniciales sugeridos', () => {
    render(<App />);
    expect(screen.getByRole('heading', { name: /new loan request/i })).toBeInTheDocument();
    expect(screen.getByLabelText('Customer ID')).toHaveValue(1001);
    expect(screen.getByText(/1001-1050/)).toBeInTheDocument();
    expect(screen.getByLabelText('Amount')).toHaveValue('10000.00');
    expect(screen.getByLabelText('Term')).toBeInTheDocument();
    expect(screen.getByLabelText('Purpose')).toBeInTheDocument();
    expect(submitButton()).toBeEnabled();
  });

  it('ofrece exactamente los plazos permitidos, mostrados en meses', () => {
    render(<App />);
    const options = within(screen.getByLabelText('Term')).getAllByRole('option');
    expect(options.map((o) => o.value)).toEqual(['12', '24', '36', '48', '60', '120', '240']);
    expect(options.map((o) => o.textContent)).toEqual([
      '12 months', '24 months', '36 months', '48 months', '60 months', '120 months', '240 months',
    ]);
  });

  it('ofrece exactamente los propósitos permitidos con etiqueta amigable y valor de API', () => {
    render(<App />);
    const options = within(screen.getByLabelText('Purpose')).getAllByRole('option');
    expect(options.map((o) => o.value)).toEqual([
      'HOME', 'CAR', 'EDUCATION', 'PERSONAL', 'BUSINESS', 'DEBT_CONSOLIDATION',
    ]);
    expect(options.find((o) => o.value === 'DEBT_CONSOLIDATION').textContent).toBe('Debt consolidation');
  });
});

describe('envío', () => {
  it('submit válido envía el payload exacto a la ruta relativa /api/loan-requests', async () => {
    fetchMock.mockResolvedValue(json(202, ACK));
    const user = userEvent.setup();
    render(<App />);

    await user.selectOptions(screen.getByLabelText('Term'), '60');
    await user.selectOptions(screen.getByLabelText('Purpose'), 'DEBT_CONSOLIDATION');
    await user.click(submitButton());

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe('/api/loan-requests');
    expect(init.method).toBe('POST');
    expect(JSON.parse(init.body)).toEqual({
      customer_id: 1001,
      amount: '10000.00',
      term_months: 60,
      purpose: 'DEBT_CONSOLIDATION',
    });
  });

  it('SUBMITTING deshabilita el botón y los campos hasta recibir respuesta', async () => {
    let resolve;
    fetchMock.mockReturnValue(new Promise((r) => { resolve = r; }));
    const user = userEvent.setup();
    render(<App />);

    await user.click(submitButton());

    expect(await screen.findByText(/sending your request/i)).toBeInTheDocument();
    expect(submitButton()).toBeDisabled();
    expect(screen.getByLabelText('Amount')).toBeDisabled();

    await user.click(submitButton()); // un segundo clic no envía de nuevo
    expect(fetchMock).toHaveBeenCalledTimes(1);

    resolve(json(202, ACK));
    expect(await screen.findByTestId('request-id')).toBeInTheDocument();
  });

  it('202 muestra request_id, requested_at y el aviso de dashboard, sin decisión', async () => {
    fetchMock.mockResolvedValue(json(202, ACK));
    const user = userEvent.setup();
    const { container } = render(<App />);

    await user.click(submitButton());

    expect(await screen.findByTestId('request-id')).toHaveTextContent(ACK.request_id);
    expect(screen.getByTestId('requested-at')).toHaveTextContent(ACK.requested_at);
    expect(screen.getByText('Loan request submitted')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /open dashboard/i })).toBeInTheDocument();
    expect(container.textContent).not.toMatch(/processing application|score|approved|rejected/i);
    expect(fetchMock).toHaveBeenCalledTimes(1); // sin polling de decisiones
  });

  it('"Submit another request" vuelve a un formulario editable', async () => {
    fetchMock.mockResolvedValue(json(202, ACK));
    const user = userEvent.setup();
    render(<App />);
    await user.click(submitButton());
    await user.click(await screen.findByRole('button', { name: /submit another request/i }));
    expect(submitButton()).toBeEnabled();
    expect(screen.getByLabelText('Amount')).toHaveValue('10000.00');
  });
});

describe('errores', () => {
  const failWith = async (response) => {
    fetchMock.mockResolvedValue(response);
    const user = userEvent.setup();
    const view = render(<App />);
    await user.click(submitButton());
    await screen.findByText(/request not submitted/i);
    return view;
  };

  it('400 muestra el mensaje de validación y no expone el detalle del backend', async () => {
    const { container } = await failWith(json(400, { error: 'Invalid request', detail: 'amount schema avro' }));
    expect(screen.getByText('Please check the loan request details.')).toBeInTheDocument();
    expect(container.textContent).not.toMatch(/avro|schema|Invalid request|amount schema/i);
    expect(submitButton()).toBeEnabled(); // se puede corregir y reintentar
  });

  it('503 muestra el mensaje temporal', async () => {
    const { container } = await failWith(json(503, { error: 'Service unavailable' }));
    expect(screen.getByText('The request could not be submitted. Please try again.')).toBeInTheDocument();
    expect(container.textContent).not.toMatch(/score|decision/i);
  });

  it('un error inesperado (500) muestra un mensaje genérico', async () => {
    await failWith(json(500, { error: 'Internal error', stack: 'Error at /srv/app.js:10' }));
    expect(screen.getByText('Something went wrong. Please try again later.')).toBeInTheDocument();
    expect(screen.queryByText(/stack|app\.js/i)).not.toBeInTheDocument();
  });

  it('un fallo de red muestra el mismo mensaje genérico', async () => {
    fetchMock.mockRejectedValue(new TypeError('Failed to fetch http://internal:3000'));
    const user = userEvent.setup();
    const { container } = render(<App />);
    await user.click(submitButton());
    expect(await screen.findByText('Something went wrong. Please try again later.')).toBeInTheDocument();
    expect(container.textContent).not.toMatch(/internal|failed to fetch/i);
  });

  it('un 202 con cuerpo inválido no se presenta como éxito', async () => {
    await failWith(json(202, { unexpected: true }));
    expect(screen.queryByTestId('request-id')).not.toBeInTheDocument();
  });
});

describe('validación de UX (el backend sigue siendo la autoridad)', () => {
  it('no envía un importe con más de 2 decimales y muestra el error de campo', async () => {
    const user = userEvent.setup();
    render(<App />);
    const amount = screen.getByLabelText('Amount');
    await user.clear(amount);
    await user.type(amount, '10.005');
    await user.click(submitButton());
    expect(await screen.findByText(/up to 2 decimals\./i, { selector: '.cds--form-requirement' })).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('no envía un importe 0', async () => {
    const user = userEvent.setup();
    render(<App />);
    const amount = screen.getByLabelText('Amount');
    await user.clear(amount);
    await user.type(amount, '0');
    await user.click(submitButton());
    expect(fetchMock).not.toHaveBeenCalled();
  });
});

describe('contrato de la UI', () => {
  it('no referencia hosts, puertos ni URLs externas salvo el enlace al dashboard local', async () => {
    const files = import.meta.glob('./**/*.{js,jsx}', { query: '?raw', import: 'default', eager: true });
    for (const [path, source] of Object.entries(files)) {
      if (path.endsWith('.test.jsx')) continue;
      const withoutDashboard = source.replaceAll("'http://127.0.0.1:8081'", '');
      expect(withoutDashboard, path).not.toMatch(/localhost|127\.0\.0\.1|https?:\/\//);
    }
  });
});
