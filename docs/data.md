# Dữ Liệu

## Dữ Liệu Nguồn

Simulator ghi CSV source/tham chiếu dưới `data/source_db` và event giao dịch lịch sử dưới `data/source_files`. PostgreSQL nạp dữ liệu tham chiếu từ `data/source_db`.

Bảng tham chiếu:

- `customers.csv`
- `accounts.csv`
- `cards.csv`
- `loans.csv`
- `repayments.csv`
- `merchants.csv`
- `branches.csv`

## Event Giao Dịch

Event giao dịch thô gồm các trường:

- `transaction_id`
- `account_id`
- `customer_id`
- `transaction_time`
- `ingestion_time`
- `amount_vnd`
- `transaction_type`
- `channel`
- `merchant_id`
- `merchant_category`
- `province`
- `currency`
- `status`

## File Lakehouse

| Layer | File | Ý nghĩa |
| --- | --- | --- |
| raw | `raw_transactions.csv` | Event nhận vào trước validate |
| clean | `clean_transactions.csv` | Event hợp lệ kèm trường dẫn xuất |
| error | `error_transactions.csv` | Event lỗi kèm metadata lỗi |
| curated | `fact_transaction.csv` | Fact giao dịch phục vụ phân tích |
| curated | `daily_transaction_summary.csv` | Summary cho dashboard |
| curated | `customer_summary.csv` | Feature khách hàng |

Giao dịch clean có thêm:

- `transaction_date`
- `transaction_hour`
- `delay_hours`
- `is_late_arriving`
- `is_outlier`
- `is_night_transaction`
- `data_quality_status`
- `processed_at`

Bản ghi error gồm:

- `error_id`
- `source_table`
- `raw_payload`
- `error_type`
- `error_message`
- `failed_column`
- `rule_name`
- `batch_id`
- `source_system`
- `ingestion_time`

## Đầu Ra Dashboard

Folder `dashboard` chứa CSV để Power BI đọc trực tiếp:

- `quality_summary.csv`
- `quality_errors.csv`
- `storage_benchmark.csv`
- `query_benchmark.csv`
- `file_inventory.csv`
- `daily_transaction_summary.csv`
