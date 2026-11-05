-- Flujo continuo: recalcula el agregado de pagos de un cliente cuando cambia su historial.
-- Fechas: payment_history.due_date llega como INT (días desde 1970-01-01); ver SPIKE.md sección 5.
INSERT INTO customer_payment_stats
SELECT
  customer_id,
  COUNT(*)                                     AS total_payments,
  COUNT(*) FILTER (WHERE status = 'LATE')      AS late_payments,
  COUNT(*) FILTER (WHERE status = 'DEFAULTED') AS defaulted_payments,
  COUNT(*) FILTER (WHERE paid_date IS NULL)    AS unpaid_payments,
  MAX(CAST(TIMESTAMPADD(DAY, due_date, DATE '1970-01-01') AS DATE)) AS last_due_date
FROM payment_history
GROUP BY customer_id
