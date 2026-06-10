# Architecture

Pipeline gồm bốn vùng dữ liệu chính: raw, clean, curated và quarantine.

Raw lưu CSV từ source systems và thêm metadata `ingestion_time`, `ingestion_date`, `batch_id`, `source_system`, `raw_file_name`, `record_hash`.

Validation đọc dữ liệu raw, áp dụng rule cho key, value, numeric, timestamp, FK và duplicate. Record lỗi nghiêm trọng được đưa vào quarantine. Record hợp lệ hoặc lỗi nhẹ được chuẩn hóa sang clean.

Curated build dimension, fact và summary phục vụ dashboard, SQL query và Project 2 Credit Risk Scoring.

Trong mỗi vùng, CSV được đặt trực tiếp ở cấp folder đó. Transaction được tách theo file ngày.
