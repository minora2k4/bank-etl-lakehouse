# Reliable Banking Data Lakehouse Pipeline

Project mô phỏng pipeline lakehouse ngân hàng theo ngữ cảnh VPBank/Việt Nam. Dữ liệu dùng VND, tỉnh thành Việt Nam, kênh VPBANK_NEO, NAPAS_QR, ATM, POS, BRANCH; tên bảng, cột, file và job dùng tiếng Anh.

## Chạy nhanh

Chạy pipeline và publish lên MinIO:

```bash
sh scripts/run_pipeline_and_publish.sh
```

Chạy dữ liệu lớn hơn:

```bash
CUSTOMERS=10000 TRANSACTIONS=1000000 sh scripts/run_pipeline_and_publish.sh
```

## Lakehouse

Dữ liệu runtime nằm trong Docker named volumes và được mirror lên MinIO, không lưu trong source folder local.

Bucket MinIO: `banking-lakehouse`

Trong bucket chỉ có:

- `source_data/`
- `lakehouse/`

Trong `lakehouse/` chỉ có bốn vùng:

- `raw`
- `clean`
- `curated`
- `quarantine`

Các vùng này chứa CSV trực tiếp. Transaction được tách theo file ngày, ví dụ `transactions_2026-06-10.csv`.

## Tooling

- MinIO: `http://localhost:9001`
- Airflow: `http://localhost:8081`
- Airflow user/password: `admin` / `admin`
- PostgreSQL source database: `sh scripts/load_postgres_sources.sh`
- Serving query MVP: `sh scripts/run_serving_query.sh`

Chi tiết setup nằm tại `docs/tooling_setup.md`.
