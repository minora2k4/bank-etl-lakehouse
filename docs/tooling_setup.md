# Tooling Setup

## Docker

Image pipeline dùng `python:3.10-slim-bookworm` để nhẹ và không cần package ngoài.

Chạy pipeline và publish dữ liệu lên MinIO:

```bash
sh scripts/run_pipeline_and_publish.sh
```

Dữ liệu runtime nằm trong Docker named volumes, không bind về source folder local.

## MinIO

MinIO là object storage cho lakehouse MVP.

Start MinIO:

```bash
docker compose up -d minio
```

Console: `http://localhost:9001`

Credential:

- User: `minioadmin`
- Password: `minioadmin`

Bucket mặc định: `banking-lakehouse`

Trong bucket chỉ có:

- `source_data/`
- `lakehouse/`

Publish lại dữ liệu đã chạy lên MinIO:

```bash
sh scripts/publish_lakehouse_to_minio.sh
```

## PostgreSQL

Postgres dùng image `postgres:16-alpine`. Schema nằm tại `configs/resources/postgres/schema.sql`.

Load source CSV vào Postgres:

```bash
sh scripts/load_postgres_sources.sh
```

Các file CSV source được mount qua Docker volume tại `/workspace/data/source_db`.

## Airflow

Chạy Airflow:

```bash
docker compose --profile airflow up -d airflow
```

Web UI: `http://localhost:8081`

Credential:

- User: `admin`
- Password: `admin`

DAG chính là `banking_lakehouse_daily_pipeline`.

Test DAG:

```bash
docker compose --profile airflow exec -T airflow airflow dags test banking_lakehouse_daily_pipeline 2026-06-10
```

## Resources

- SQL query: `configs/resources/sql`
- Postgres schema/load SQL: `configs/resources/postgres`
- Data quality rules: `configs/resources/data_quality`
