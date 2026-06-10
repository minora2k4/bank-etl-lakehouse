# Performance Optimization

MVP dùng CSV để dễ audit và debug. Khi production-scale, hướng tối ưu là:

- Giữ raw CSV/JSONL cho audit.
- Chuyển clean và curated sang Parquet để giảm dung lượng và tăng tốc query.
- Tách transaction theo file ngày để giảm lượng dữ liệu phải scan.
- Dashboard đọc `daily_transaction_summary.csv` thay vì scan toàn bộ fact transaction.
- Benchmark được in ra log khi chạy pipeline.
