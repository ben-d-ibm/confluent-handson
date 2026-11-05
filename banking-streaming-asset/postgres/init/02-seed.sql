-- Datos sintéticos DETERMINISTAS (los usa el smoke test).
-- 1001: solvente | 1002: moroso | 1003: caso límite | 1004-1050: generados

INSERT INTO customers (customer_id, full_name, email, monthly_income, monthly_debt, employment_years, segment) VALUES
  (1001, 'Ana Solvente',  'ana.solvente@example.com',  8000.00,  500.00, 10, 'GOLD'),
  (1002, 'Bruno Moroso',  'bruno.moroso@example.com',  2200.00, 1800.00,  1, 'STANDARD'),
  (1003, 'Carla Limite',  'carla.limite@example.com',  4000.00, 1500.00,  3, 'SILVER');

INSERT INTO customers (customer_id, full_name, email, monthly_income, monthly_debt, employment_years, segment)
SELECT i,
       'Cliente ' || i,
       'cliente' || i || '@example.com',
       2000 + (i * 379 % 7000),
       round((2000 + (i * 379 % 7000)) * ((i * 7 % 60) / 100.0), 2),
       i % 15,
       (ARRAY['STANDARD', 'SILVER', 'GOLD'])[1 + (i % 3)]
FROM generate_series(1004, 1050) AS i;

-- 1001: 12 pagos puntuales
INSERT INTO payment_history (customer_id, due_date, paid_date, amount, status)
SELECT 1001, (date '2026-01-01' - (j * interval '1 month'))::date, (date '2026-01-01' - (j * interval '1 month'))::date, 450.00, 'ON_TIME'
FROM generate_series(1, 12) AS j;

-- 1002: 3 impagos + 5 retrasos
INSERT INTO payment_history (customer_id, due_date, paid_date, amount, status)
SELECT 1002, (date '2026-01-01' - (j * interval '1 month'))::date,
       CASE WHEN j <= 3 THEN NULL ELSE (date '2026-01-01' - (j * interval '1 month') + interval '15 days')::date END,
       600.00,
       CASE WHEN j <= 3 THEN 'DEFAULTED' ELSE 'LATE' END
FROM generate_series(1, 8) AS j;

-- 1003: 2 retrasos y el resto puntual
INSERT INTO payment_history (customer_id, due_date, paid_date, amount, status)
SELECT 1003, (date '2026-01-01' - (j * interval '1 month'))::date,
       (date '2026-01-01' - (j * interval '1 month') + CASE WHEN j IN (2, 5) THEN interval '10 days' ELSE interval '0 days' END)::date,
       500.00,
       CASE WHEN j IN (2, 5) THEN 'LATE' ELSE 'ON_TIME' END
FROM generate_series(1, 10) AS j;

-- Generados: 6 pagos por cliente con mezcla determinista
INSERT INTO payment_history (customer_id, due_date, paid_date, amount, status)
SELECT i,
       (date '2026-01-01' - (j * interval '1 month'))::date,
       CASE WHEN (i * j) % 13 = 0 THEN NULL
            WHEN (i + j) % 7 = 0 THEN (date '2026-01-01' - (j * interval '1 month') + interval '12 days')::date
            ELSE (date '2026-01-01' - (j * interval '1 month'))::date END,
       300 + (i % 10) * 25,
       CASE WHEN (i * j) % 13 = 0 THEN 'DEFAULTED'
            WHEN (i + j) % 7 = 0 THEN 'LATE'
            ELSE 'ON_TIME' END
FROM generate_series(1004, 1050) AS i
CROSS JOIN generate_series(1, 6) AS j;
