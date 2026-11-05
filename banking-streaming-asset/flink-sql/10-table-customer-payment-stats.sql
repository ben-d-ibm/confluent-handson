-- Agregado de pagos por cliente. Tabla upsert (PK = customer_id) alimentada por 11-insert-customer-payment-stats.sql.
-- Se declara explícita (no CTAS) para poder recrear el flujo sin chocar con una tabla ya existente.
CREATE TABLE IF NOT EXISTS customer_payment_stats (
  customer_id        INT,
  total_payments     BIGINT NOT NULL,
  late_payments      BIGINT NOT NULL,
  defaulted_payments BIGINT NOT NULL,
  unpaid_payments    BIGINT NOT NULL,
  last_due_date      DATE,
  PRIMARY KEY (customer_id) NOT ENFORCED
)
