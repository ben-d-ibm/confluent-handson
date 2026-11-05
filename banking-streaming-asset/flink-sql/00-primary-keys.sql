-- Los tópicos CDC se infieren como tablas Flink sin PK. Se declara (metadatos, no se aplica).
-- Necesaria para los temporal joins. Ver SPIKE.md sección 3.
ALTER TABLE customers       ADD PRIMARY KEY (customer_id) NOT ENFORCED;
ALTER TABLE payment_history ADD PRIMARY KEY (payment_id)  NOT ENFORCED
