-- SONDA del spike (no es la lógica final): enriquecimiento con temporal joins hacia un tópico legible.
CREATE TABLE spike_enriched AS
SELECT l.request_id, l.customer_id, l.amount, l.requested_at,
       c.monthly_income, c.monthly_debt, c.segment,
       COALESCE(p.total_payments, CAST(0 AS BIGINT))     AS total_payments,
       COALESCE(p.late_payments, CAST(0 AS BIGINT))      AS late_payments,
       COALESCE(p.defaulted_payments, CAST(0 AS BIGINT)) AS defaulted_payments
FROM `loan-requests` l
LEFT JOIN customers FOR SYSTEM_TIME AS OF l.`$rowtime` AS c ON l.customer_id = c.customer_id
LEFT JOIN customer_payment_stats FOR SYSTEM_TIME AS OF l.`$rowtime` AS p ON l.customer_id = p.customer_id
