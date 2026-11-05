import { useState } from 'react';
import {
  Button,
  Form,
  InlineLoading,
  InlineNotification,
  NumberInput,
  Select,
  SelectItem,
  Stack,
  TextInput,
} from '@carbon/react';
import { DEFAULT_VALUES, PURPOSE_OPTIONS, TERM_OPTIONS, toPayload, validateForm } from '../loanForm.js';

/**
 * Formulario controlado. Es presentacional: delega el envío en `onSubmit(payload)`,
 * `status` ('idle' | 'submitting' | 'error') y `errorMessage` los gobierna App.
 */
export default function LoanRequestForm({ status, errorMessage, onSubmit }) {
  const [values, setValues] = useState({ ...DEFAULT_VALUES });
  const [touched, setTouched] = useState(false);

  const submitting = status === 'submitting';
  const errors = touched ? validateForm(values) : {};

  const set = (field) => (value) => setValues((prev) => ({ ...prev, [field]: String(value ?? '') }));

  const handleSubmit = (event) => {
    event.preventDefault();
    if (submitting) return;
    setTouched(true);
    if (Object.keys(validateForm(values)).length > 0) return;
    onSubmit(toPayload(values));
  };

  return (
    <Form onSubmit={handleSubmit} noValidate aria-label="Loan request form">
      <Stack gap={6}>
        {status === 'error' && (
          <InlineNotification
            kind="error"
            lowContrast
            hideCloseButton
            title="Request not submitted"
            subtitle={errorMessage}
          />
        )}

        <NumberInput
          id="customer-id"
          label="Customer ID"
          helperText="Customers in the dataset are 1001-1050."
          min={1}
          step={1}
          allowEmpty
          hideSteppers
          value={values.customerId}
          invalid={Boolean(errors.customerId)}
          invalidText={errors.customerId}
          disabled={submitting}
          onChange={(_event, state) => set('customerId')(state?.value)}
        />

        <TextInput
          id="amount"
          labelText="Amount"
          helperText="Greater than 0, up to 2 decimals (for example 10000.00)."
          inputMode="decimal"
          autoComplete="off"
          value={values.amount}
          invalid={Boolean(errors.amount)}
          invalidText={errors.amount}
          disabled={submitting}
          onChange={(event) => set('amount')(event.target.value)}
        />

        <Select
          id="term"
          labelText="Term"
          value={values.term}
          disabled={submitting}
          onChange={(event) => set('term')(event.target.value)}
        >
          {TERM_OPTIONS.map((o) => (
            <SelectItem key={o.value} value={o.value} text={o.label} />
          ))}
        </Select>

        <Select
          id="purpose"
          labelText="Purpose"
          value={values.purpose}
          disabled={submitting}
          onChange={(event) => set('purpose')(event.target.value)}
        >
          {PURPOSE_OPTIONS.map((o) => (
            <SelectItem key={o.value} value={o.value} text={o.label} />
          ))}
        </Select>

        <div className="form-actions">
          <Button type="submit" disabled={submitting}>
            {submitting ? 'Submitting…' : 'Submit request'}
          </Button>
          {submitting && <InlineLoading description="Sending your request" status="active" />}
        </div>
      </Stack>
    </Form>
  );
}
