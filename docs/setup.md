# Cài Đặt

## Yêu Cầu

- Docker Desktop
- Docker Compose
- Power BI Desktop, nếu muốn mở dashboard

## Khởi Động Platform

```bash
sh scripts/setup.sh
```

Script này khởi động Kafka, Kafka UI, Spark, PostgreSQL, pgAdmin, MinIO và tạo các topic:

- `raw-transactions` (retention 7 ngày)
- `clean-transactions` (retention 7 ngày)
- `error-transactions` (retention 30 ngày)

## Chạy Luồng Batch Dự Phòng

```bash
sh scripts/start.sh
```

Luồng dự phòng sinh dữ liệu nguồn, ghi `raw_transactions.csv`, validate sang `clean_transactions.csv` và `error_transactions.csv`, xây dựng curated data, xuất dashboard CSV, tạo `docs/data_quality_report.md` và đẩy `lakehouse/` lên MinIO.

## Chạy Demo Streaming

```bash
sh scripts/start_streaming.sh
```

Script này chuẩn bị dữ liệu tham chiếu, nạp schema/source data vào PostgreSQL, khởi động Spark validator và dịch vụ ghi nhận PostgreSQL, sau đó gửi transaction được sinh tự động vào Kafka.

## URL Hữu Ích

| Công cụ | URL | Login |
| --- | --- | --- |
| Kafka UI | `http://localhost:8083` | không cần |
| Spark UI | `http://localhost:8082` | không cần |
| Jupyter | `http://localhost:8888` | không cần |
| MinIO | `http://localhost:9001` | `minioadmin` / `minioadmin` |
| pgAdmin | `http://localhost:5050` | `admin@bank.com` / `admin` |
| PostgreSQL | `localhost:5432` | `banking` / `banking` |

## MinIO

Bucket: `banking-lakehouse`

Upload hàng loạt (mirror toàn bộ `lakehouse/` lên MinIO):

```bash
sh scripts/publish.sh
```

Các file transaction kỳ vọng:

```text
lakehouse/raw/raw_transactions.csv
lakehouse/clean/clean_transactions.csv
lakehouse/error/error_transactions.csv
```

Để upload từng file bằng Python (cần cài package `minio`):

```python
from connector.minio import upload_file
upload_file("lakehouse/clean/clean_transactions.csv", "lakehouse/clean/clean_transactions.csv")
```

## PostgreSQL

Nạp dữ liệu source/tham chiếu:

```bash
sh scripts/postgres.sh
```

Các bảng ACID quan trọng:

- `processed_transactions` — idempotency và trạng thái xử lý
- `ledger_entries` — audit bất biến
- `transactions` — bản ghi giao dịch đã posted
- `accounts` — số dư tài khoản (có row lock khi cập nhật)
