-- Decisiones de préstamo. Una fila por solicitud (clave = request_id), solo inserciones.
-- 'model' identifica el motor de decisión (RULES_V1 hoy; un modo con IA podría escribir otro valor sin cambiar el contrato).
CREATE TABLE IF NOT EXISTS `loan-decisions` (
  request_id    STRING,
  customer_id   INT NOT NULL,
  amount        DECIMAL(12, 2) NOT NULL,
  term_months   INT NOT NULL,
  purpose       STRING NOT NULL,
  requested_at  TIMESTAMP_LTZ(3) NOT NULL,
  decided_at    TIMESTAMP_LTZ(3) NOT NULL,
  score         INT NOT NULL,
  decision      STRING NOT NULL,
  reasons       STRING NOT NULL,
  dti_ratio     DOUBLE,
  model         STRING NOT NULL,
  PRIMARY KEY (request_id) NOT ENFORCED
) WITH (
  'changelog.mode' = 'append'
)
