#!/bin/sh
set -eu

postgres_user="${POSTGRES_USER:-banking}"
postgres_db="${POSTGRES_DB:-banking}"

docker compose up -d postgres
docker compose exec -T postgres psql -U "$postgres_user" -d "$postgres_db" -f /sql/schema.sql
docker compose exec -T postgres psql -U "$postgres_user" -d "$postgres_db" -f /sql/load_sources.sql
docker compose exec -T postgres psql -U "$postgres_user" -d "$postgres_db" -c "SELECT 'customers' AS table_name, COUNT(*) FROM customers UNION ALL SELECT 'accounts', COUNT(*) FROM accounts UNION ALL SELECT 'cards', COUNT(*) FROM cards UNION ALL SELECT 'loans', COUNT(*) FROM loans UNION ALL SELECT 'repayments', COUNT(*) FROM repayments UNION ALL SELECT 'merchants', COUNT(*) FROM merchants UNION ALL SELECT 'branches', COUNT(*) FROM branches;"
