#!/usr/bin/env bash
set -euo pipefail

COMPOSE_COMMAND=
if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_COMMAND="docker-compose"
else
  COMPOSE_COMMAND="docker compose"
fi

PGPASSWORD=${PGPASSWORD:-pass} ${COMPOSE_COMMAND} exec -T postgres psql -U tokenize -d tokenize <<'SQL'
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    email TEXT,
    phone TEXT
);
TRUNCATE customers;
INSERT INTO customers (email, phone)
SELECT
    format('user%03s@example.com', g) AS email,
    format('555-010%03s', g) AS phone
FROM generate_series(1, 100) AS g;
SQL

echo "Seeded customers table with sample data."
