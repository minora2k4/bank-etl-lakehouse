# Data Quality Design

Rules chính nằm tại `configs/resources/data_quality/rules.yaml`.

Lỗi quarantine gồm missing key, FK không tồn tại, amount không hợp lệ, timestamp tương lai, status/channel sai và duplicate transaction.

Lỗi nhẹ như thiếu `merchant_category` hoặc `province` được fill `UNKNOWN`.

Transaction hợp lệ được bổ sung `transaction_date`, `transaction_hour`, `delay_hours`, `is_late_arriving`, `is_outlier`, `is_night_transaction` và `data_quality_status`.
