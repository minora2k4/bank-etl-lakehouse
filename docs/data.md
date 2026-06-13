# Dữ Liệu

## Event Giao Dịch

Event giao dịch thô gồm các trường:

- `transaction_id`
- `account_id`
- `customer_id`
- `card_id`
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
- `source_system`

## File Lakehouse

| Layer | File | Ý nghĩa |
| --- | --- | --- |
| raw | `raw_transactions.csv` | Event nhận vào trước validate |
| clean | `clean_transactions.csv` | Event hợp lệ kèm trường dẫn xuất |
| error | `error_transactions.csv` | Event lỗi kèm metadata lỗi |
| curated | `dim_customer.csv` | Dimension khách hàng |
| curated | `dim_account.csv` | Dimension tài khoản |
| curated | `dim_card.csv` | Dimension thẻ |
| curated | `dim_merchant.csv` | Dimension merchant |
| curated | `dim_branch.csv` | Dimension chi nhánh |
| curated | `fact_transaction.csv` | Fact giao dịch phục vụ phân tích |
| curated | `fact_loan.csv` | Fact khoản vay |
| curated | `fact_repayment.csv` | Fact lịch sử trả nợ |
| curated | `daily_transaction_summary.csv` | Summary theo ngày/kênh/tỉnh cho dashboard |
| curated | `customer_summary.csv` | Feature khách hàng cho credit risk |

Giao dịch clean có thêm các trường dẫn xuất:

- `transaction_date` — ngày giao dịch (từ `transaction_time`)
- `transaction_hour` — giờ giao dịch
- `delay_hours` — độ trễ ingestion so với thời điểm giao dịch
- `is_late_arriving` — 1 nếu `delay_hours > 24`
- `is_outlier` — 1 nếu `amount_vnd >= 500,000,000`
- `is_night_transaction` — 1 nếu giờ giao dịch từ 22h đến 5h
- `data_quality_status` — `VALID` hoặc `VALID_WITH_FLAGS`
- `processed_at` — timestamp validate
- `batch_id` — ID batch nạp dữ liệu

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

- `quality_summary.csv` — số records valid/invalid theo layer
- `quality_errors.csv` — thống kê lỗi theo source_table và error_type
- `storage_benchmark.csv` — dung lượng theo layer
- `query_benchmark.csv` — thời gian đọc daily_transaction_summary
- `file_inventory.csv` — danh sách file và row count mỗi lần chạy
- `daily_transaction_summary.csv` — copy từ curated, để Power BI đọc trực tiếp

## Báo Cáo Chất Lượng Dữ Liệu

Mỗi lần chạy pipeline, `docs/data_quality_report.md` được tạo tự động bởi `export_metrics`. File này tóm tắt số lượng records valid/invalid và bảng chi tiết lỗi theo source_table, error_type, cột và rule.
