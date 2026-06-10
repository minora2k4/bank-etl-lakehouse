import os
from pathlib import Path


# Thư mục gốc của project.
root_dir = Path(__file__).resolve().parents[3]

# Thư mục mô phỏng các hệ thống nguồn.
data_dir = Path(os.getenv("DATA_DIR", root_dir / "data"))
source_db_dir = data_dir / "source_db"
source_files_dir = data_dir / "source_files"

# Thư mục lakehouse trong runtime.
lakehouse_dir = Path(os.getenv("LAKEHOUSE_DIR", root_dir / "lakehouse"))
raw_data_dir = lakehouse_dir / "raw"
clean_data_dir = lakehouse_dir / "clean"
curated_data_dir = lakehouse_dir / "curated"
quarantine_dir = lakehouse_dir / "quarantine"

# Thư mục dành cho Power BI đọc các file thống kê.
dashboard_dir = Path(os.getenv("DASHBOARD_DIR", root_dir / "dashboard"))
docs_dir = Path(os.getenv("DOCS_DIR", root_dir / "docs"))

# Ngày batch mặc định cho các file giao dịch theo ngày.
default_ingestion_date = os.getenv("INGESTION_DATE", "2026-06-10")

# Batch mặc định cho lineage.
default_batch_id = os.getenv("BATCH_ID", "BATCH_20260610_0001")

# Quy mô dữ liệu mặc định để test nhanh, có thể tăng bằng CLI.
default_volumes = {
    "customers": 1000,
    "branches": 50,
    "merchants": 500,
    "transactions": 10000,
    "loans": 500,
}

postgres_tables = [
    "customers",
    "accounts",
    "cards",
    "loans",
    "repayments",
    "merchants",
    "branches",
]
