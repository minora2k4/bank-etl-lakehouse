# Kiến Trúc

## Mục Tiêu

Project hiện mô phỏng banking lakehouse theo hướng streaming-first:

1. PostgreSQL lưu dữ liệu master/tham chiếu và số dư tài khoản.
2. Kafka nhận event giao dịch thô ở topic `raw-transactions`.
3. Spark Structured Streaming validate và enrich event.
4. Event hợp lệ đi vào `clean-transactions` và `lakehouse/clean/clean_transactions.csv`.
5. Event lỗi đi vào `error-transactions` và `lakehouse/error/error_transactions.csv`.
6. `postgres_transaction_updater` consume clean event và cập nhật số dư trong PostgreSQL transaction.
7. Curated/dashboard CSV được xây dựng từ dữ liệu clean trong lakehouse.

## Luồng Xử Lý

```text
Transaction Producer
  -> Kafka raw-transactions
  -> Spark streaming validator
       -> Kafka clean-transactions -> PostgreSQL transaction updater -> accounts + ledger_entries
       -> Kafka error-transactions
  -> MinIO lakehouse/raw, lakehouse/clean, lakehouse/error
```

Việc ghi nhận giao dịch vào PostgreSQL được tách khỏi Spark một cách chủ động. Spark chịu trách nhiệm chất lượng dữ liệu; updater chịu trách nhiệm thay đổi trạng thái tài chính bằng `SELECT ... FOR UPDATE`, idempotency và ledger audit.

## Cấu Trúc Lakehouse

```text
banking-lakehouse/
  lakehouse/
    raw/
      raw_transactions.csv
    clean/
      clean_transactions.csv
    error/
      error_transactions.csv
    curated/
      dim_customer.csv
      dim_account.csv
      dim_merchant.csv
      fact_transaction.csv
      daily_transaction_summary.csv
      customer_summary.csv
    audit/
```

`source_data/` không còn được đẩy lên MinIO. PostgreSQL/source CSV là nguồn dữ liệu chuẩn cho customers, accounts, cards, loans, repayments, merchants và branches.

## Topic Kafka

| Topic | Producer | Consumer | Mục đích |
| --- | --- | --- | --- |
| `raw-transactions` | transaction producer | Spark validator | Event giao dịch thô |
| `clean-transactions` | Spark validator | PostgreSQL updater | Event hợp lệ, sẵn sàng ghi nhận |
| `error-transactions` | Spark validator/updater | lakehouse/dashboard | Lỗi chất lượng dữ liệu hoặc lỗi nghiệp vụ |

## Ghi Nhận Giao Dịch ACID Trên PostgreSQL

Updater dùng:

- `processed_transactions.transaction_id` làm khóa idempotency unique.
- `SELECT ... FOR UPDATE` trên `accounts` trước khi đổi số dư.
- `ledger_entries` làm bảng audit bất biến.
- Một database transaction cho trạng thái xử lý, business validation, insert ledger và update balance.
