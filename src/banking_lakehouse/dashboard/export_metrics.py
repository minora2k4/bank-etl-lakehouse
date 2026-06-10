import os
import time
from pathlib import Path

from banking_lakehouse.config.settings import clean_data_dir, curated_data_dir, dashboard_dir, default_ingestion_date, quarantine_dir
from banking_lakehouse.utils.io import read_csv, write_csv


def count_csv_rows(paths):
    """Đếm tổng số dòng dữ liệu trong các file CSV."""
    total = 0
    for path in paths:
        if path.exists():
            total += len(read_csv(path))
    return total


def folder_size(path):
    """Tính dung lượng thư mục theo byte."""
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            total += os.path.getsize(Path(root) / name)
    return total


def export_quality_summary(ingestion_date=default_ingestion_date):
    """Xuất thống kê chất lượng dữ liệu cho Power BI."""
    rows = [{
        "ingestion_date": ingestion_date,
        "customers_valid": count_csv_rows([clean_data_dir / "customers.csv"]),
        "accounts_valid": count_csv_rows([clean_data_dir / "accounts.csv"]),
        "transactions_valid": count_csv_rows(clean_data_dir.glob("transactions_*.csv")),
        "invalid_records": count_csv_rows(quarantine_dir.glob(f"*_{ingestion_date}.csv")),
    }]
    write_csv(dashboard_dir / "quality_summary.csv", rows)
    return rows


def export_storage_benchmark():
    """Xuất benchmark dung lượng dữ liệu cho Power BI."""
    rows = [
        {"dataset": "clean", "format": "csv", "size_bytes": folder_size(clean_data_dir)},
        {"dataset": "curated", "format": "csv", "size_bytes": folder_size(curated_data_dir)},
    ]
    write_csv(dashboard_dir / "storage_benchmark.csv", rows)
    return rows


def export_query_benchmark():
    """Xuất benchmark đọc bảng summary phục vụ dashboard."""
    path = curated_data_dir / "daily_transaction_summary.csv"
    started = time.perf_counter()
    rows = read_csv(path) if path.exists() else []
    total_transactions = sum(int(float(row.get("total_transactions", 0))) for row in rows)
    elapsed = round(time.perf_counter() - started, 4)
    result = [{
        "query_name": "daily_transaction_summary_scan",
        "rows": len(rows),
        "total_transactions": total_transactions,
        "runtime_seconds": elapsed,
    }]
    write_csv(dashboard_dir / "query_benchmark.csv", result)
    return result


def export_dashboard_metrics():
    """Xuất toàn bộ file thống kê để Power BI đọc từ folder dashboard."""
    dashboard_dir.mkdir(parents=True, exist_ok=True)
    return {
        "quality_summary": export_quality_summary(),
        "storage_benchmark": export_storage_benchmark(),
        "query_benchmark": export_query_benchmark(),
    }


if __name__ == "__main__":
    export_dashboard_metrics()
