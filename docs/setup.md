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

- `raw-transactions`
- `clean-transactions`
- `error-transactions`

## Chạy Luồng Batch Dự Phòng

```bash
sh scripts/start.sh
```

Luồng dự phòng sinh dữ liệu nguồn, ghi `raw_transactions.csv`, validate sang `clean_transactions.csv` và `error_transactions.csv`, xây dựng curated data, xuất dashboard CSV và chỉ đẩy `lakehouse/` lên MinIO.

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
| MinIO | `http://localhost:9001` | `minioadmin` / `minioadmin` |
| pgAdmin | `http://localhost:5050` | `admin@bank.com` / `admin` |
| PostgreSQL | `localhost:5432` | `banking` / `banking` |

## MinIO

Bucket: `banking-lakehouse`

Root path kỳ vọng:

```text
lakehouse/
```

Các file transaction kỳ vọng:

```text
lakehouse/raw/raw_transactions.csv
lakehouse/clean/clean_transactions.csv
lakehouse/error/error_transactions.csv
```

## PostgreSQL

Nạp dữ liệu source/tham chiếu:

```bash
sh scripts/postgres.sh
```

Các bảng ACID quan trọng:

- `processed_transactions`
- `ledger_entries`
- `transactions`
- `accounts`

## Power BI

Trỏ Power BI tới:

```text
dashboard
```
