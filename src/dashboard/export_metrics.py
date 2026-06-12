import os
import time
from pathlib import Path

from config.settings import (
    clean_data_dir,
    curated_data_dir,
    dashboard_dir,
    error_data_dir,
    raw_data_dir,
)
from utils.io import read_csv, write_csv


def list_csv_files(path):
    """Lấy danh sách CSV trong một folder."""
    return sorted(path.glob("*.csv")) if path.exists() else []


def count_csv_rows(paths):
    """Đếm tổng số dòng dữ liệu trong các file CSV."""
    total = 0
    for path in paths:
        if path.exists():
            total += len(read_csv(path))
    return total


def folder_size(path):
    """Tính dung lượng folder theo byte."""
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            total += os.path.getsize(Path(root) / name)
    return total


def export_quality_summary():
    """Xuất tổng quan chất lượng dữ liệu cho Power BI."""
    rows = [{
        "customers_valid": count_csv_rows([clean_data_dir / "customers.csv"]),
        "accounts_valid": count_csv_rows([clean_data_dir / "accounts.csv"]),
        "transactions_valid": count_csv_rows([clean_data_dir / "clean_transactions.csv"]),
        "invalid_records": count_csv_rows(list_csv_files(error_data_dir)),
    }]
    write_csv(dashboard_dir / "quality_summary.csv", rows)
    return rows


def export_quality_errors():
    """Xuất thống kê lỗi theo source_table và error_type."""
    grouped = {}
    for path in list_csv_files(error_data_dir):
        for row in read_csv(path):
            key = (
                row.get("source_table", ""),
                row.get("error_type", ""),
                row.get("failed_column", ""),
                row.get("rule_name", ""),
            )
            item = grouped.setdefault(key, {
                "source_table": key[0],
                "error_type": key[1],
                "failed_column": key[2],
                "rule_name": key[3],
                "error_count": 0,
            })
            item["error_count"] += 1
    rows = sorted(grouped.values(), key=lambda item: (item["source_table"], item["error_type"]))
    write_csv(dashboard_dir / "quality_errors.csv", rows)
    return rows


def export_storage_benchmark():
    """Xuất benchmark dung lượng dữ liệu cho Power BI."""
    rows = [
        {"dataset": "raw", "format": "csv", "size_bytes": folder_size(raw_data_dir)},
        {"dataset": "clean", "format": "csv", "size_bytes": folder_size(clean_data_dir)},
        {"dataset": "curated", "format": "csv", "size_bytes": folder_size(curated_data_dir)},
        {"dataset": "error", "format": "csv", "size_bytes": folder_size(error_data_dir)},
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


def export_file_inventory():
    """Xuất inventory file để Power BI và QA đối chiếu output mỗi lần chạy."""
    rows = []
    folders = {
        "raw": raw_data_dir,
        "clean": clean_data_dir,
        "curated": curated_data_dir,
        "error": error_data_dir,
    }
    for layer_name, folder in folders.items():
        for path in list_csv_files(folder):
            rows.append({
                "layer_name": layer_name,
                "file_name": path.name,
                "size_bytes": path.stat().st_size,
                "row_count": len(read_csv(path)),
            })
    write_csv(dashboard_dir / "file_inventory.csv", rows)
    return rows


def export_daily_transaction_summary():
    """Copy curated summary sang dashboard để Power BI đọc trực tiếp."""
    path = curated_data_dir / "daily_transaction_summary.csv"
    rows = read_csv(path) if path.exists() else []
    write_csv(dashboard_dir / "daily_transaction_summary.csv", rows)
    return rows


def export_dashboard_metrics():
    """Xuất toàn bộ thống kê về folder dashboard."""
    dashboard_dir.mkdir(parents=True, exist_ok=True)
    return {
        "quality_summary": export_quality_summary(),
        "quality_errors": export_quality_errors(),
        "storage_benchmark": export_storage_benchmark(),
        "query_benchmark": export_query_benchmark(),
        "file_inventory": export_file_inventory(),
        "daily_transaction_summary": export_daily_transaction_summary(),
    }


if __name__ == "__main__":
    export_dashboard_metrics()
