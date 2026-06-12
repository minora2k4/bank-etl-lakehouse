# Reliable Banking Data Lakehouse Pipeline

Demo banking lakehouse theo hướng streaming-first, dùng Kafka, Spark, ghi nhận giao dịch ACID trên PostgreSQL, MinIO và các file CSV phục vụ Power BI.

## Chạy Luồng Batch Dự Phòng

Luồng batch vẫn được giữ để kiểm thử local và backfill:

```bash
sh scripts/start.sh
```

Chạy mẫu với dữ liệu lớn hơn:

```bash
CUSTOMERS=10000 TRANSACTIONS=1000000 sh scripts/start.sh
```

## Chạy Demo Streaming

Khởi động platform, tạo Kafka topic, chuẩn bị dữ liệu tham chiếu, chạy Spark validator, chạy dịch vụ ghi nhận PostgreSQL và đẩy output lakehouse:

```bash
sh scripts/start_streaming.sh
```

Chỉ khởi động platform:

```bash
sh scripts/setup.sh
```

Gửi message Kafka mẫu:

```bash
sh scripts/kafka.sh
```

## Dịch Vụ Chính

- MinIO: `http://localhost:9001`
- Kafka UI: `http://localhost:8083`
- Spark UI: `http://localhost:8082`
- Jupyter: `http://localhost:8888`
- PostgreSQL: `localhost:5432`
- pgAdmin: `http://localhost:5050`, user/password `admin@bank.com/admin`
- Folder input cho Power BI: `dashboard`

## Topic Kafka

- `raw-transactions`
- `clean-transactions`
- `error-transactions`

## Cấu Trúc Lakehouse

Bucket MinIO `banking-lakehouse` chỉ chứa:

- `lakehouse/raw/raw_transactions.csv`
- `lakehouse/clean/clean_transactions.csv`
- `lakehouse/error/error_transactions.csv`
- `lakehouse/curated/`
- `lakehouse/audit/`

`source_data/` không còn được mirror lên MinIO. PostgreSQL/source CSV giữ vai trò nguồn dữ liệu chuẩn cho dữ liệu master/tham chiếu.

## Cấu Trúc Source

- `src/kafka`: simulator và transaction producer.
- `src/spark`: rule chất lượng dữ liệu, lakehouse sink và Spark streaming validator.
- `src/application/postgres_transaction_updater.py`: cập nhật số dư ACID, chống xử lý trùng và ghi ledger audit.
