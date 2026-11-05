-- Agregado por cliente del historial de pagos (materializado en el tópico customer_payment_stats).
-- Conversión de días Unix (INT) a DATE: CAST(TIMESTAMPADD(DAY, <dias>, DATE '1970-01-01') AS DATE)
-- Los inserts/updates de payment_history reactualizan la fila del cliente (upsert por customer_id).
CREATE TABLE customer_payment_stats (
  PRIMARY KEY (customer_id) NOT ENFORCED
) AS
SELECT
  customer_id,
  COUNT(*)                                       AS total_payments,
  COUNT(*) FILTER (WHERE status = 'LATE')        AS late_payments,
  COUNT(*) FILTER (WHERE status = 'DEFAULTED')   AS defaulted_payments,
  COUNT(*) FILTER (WHERE paid_date IS NULL)      AS unpaid_payments,
  MAX(CAST(TIMESTAMPADD(DAY, due_date, DATE '1970-01-01') AS DATE)) AS last_due_date
FROM payment_history
GROUP BY customer_id
