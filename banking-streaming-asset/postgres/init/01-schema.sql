-- Esquema de la BD de clientes (modelo mínimo para scoring de préstamos)
CREATE ROLE cdc LOGIN REPLICATION PASSWORD :'cdc_password';

CREATE TABLE customers (
    customer_id      integer PRIMARY KEY,
    full_name        text           NOT NULL,
    email            text           NOT NULL,
    monthly_income   numeric(12,2)  NOT NULL CHECK (monthly_income >= 0),
    monthly_debt     numeric(12,2)  NOT NULL CHECK (monthly_debt >= 0),
    employment_years integer        NOT NULL CHECK (employment_years >= 0),
    segment          text           NOT NULL,
    updated_at       timestamptz    NOT NULL DEFAULT now()
);

CREATE TABLE payment_history (
    payment_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id integer       NOT NULL REFERENCES customers (customer_id),
    due_date    date          NOT NULL,
    paid_date   date,
    amount      numeric(12,2) NOT NULL CHECK (amount > 0),
    status      text          NOT NULL CHECK (status IN ('ON_TIME', 'LATE', 'DEFAULTED'))
);
CREATE INDEX payment_history_customer_idx ON payment_history (customer_id);

-- OBLIGATORIO para Flink: los updates/deletes de Debezium deben llevar la fila anterior ("before").
-- Con REPLICA IDENTITY DEFAULT, "before" llega vacío y Flink falla al leer el tópico (ver flink-sql/SPIKE.md).
ALTER TABLE customers       REPLICA IDENTITY FULL;
ALTER TABLE payment_history REPLICA IDENTITY FULL;

-- Mínimo privilegio para el CDC: replicación + SELECT solo en estas tablas
GRANT SELECT ON customers, payment_history TO cdc;

-- Publicación explícita (Debezium no la crea: publication.autocreate.mode=disabled)
CREATE PUBLICATION dbz_publication FOR TABLE customers, payment_history;
