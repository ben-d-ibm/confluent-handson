import { useState } from 'react';
import { Header, HeaderName, Theme } from '@carbon/react';
import LoanRequestForm from './components/LoanRequestForm.jsx';
import SubmissionResult from './components/SubmissionResult.jsx';
import { ERROR_MESSAGES, SubmitError, submitLoanRequest } from './api.js';

// Estados: idle → submitting → success | error. La UI no calcula ni muestra decisiones: eso es del dashboard.
export default function App({ submit = submitLoanRequest }) {
  const [status, setStatus] = useState('idle');
  const [result, setResult] = useState(null);
  const [errorMessage, setErrorMessage] = useState('');
  const [formKey, setFormKey] = useState(0);

  const handleSubmit = async (payload) => {
    if (status === 'submitting') return;
    setStatus('submitting');
    setErrorMessage('');
    try {
      setResult(await submit(payload));
      setStatus('success');
    } catch (err) {
      const kind = err instanceof SubmitError ? err.kind : 'unexpected';
      setErrorMessage(ERROR_MESSAGES[kind] ?? ERROR_MESSAGES.unexpected);
      setStatus('error');
    }
  };

  const reset = () => {
    setResult(null);
    setStatus('idle');
    setFormKey((k) => k + 1);
  };

  return (
    <Theme theme="white">
      <Header aria-label="Bank loan requests">
        <HeaderName prefix="">Loan requests</HeaderName>
      </Header>
      <main className="page">
        <h1 className="page__title">New loan request</h1>
        <p className="page__lead">Enter the loan details and submit the request for processing.</p>
        <div className="form-panel">
          {status === 'success' ? (
            <SubmissionResult requestId={result.requestId} requestedAt={result.requestedAt} onReset={reset} />
          ) : (
            <LoanRequestForm key={formKey} status={status} errorMessage={errorMessage} onSubmit={handleSubmit} />
          )}
        </div>
      </main>
    </Theme>
  );
}
