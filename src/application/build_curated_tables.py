from config.settings import clean_data_dir, curated_data_dir
from utils.io import read_csv, write_csv


def read_clean_table(table):
    """Đọc bảng clean dạng một file CSV."""
    path = clean_data_dir / f"{table}.csv"
    return read_csv(path) if path.exists() else []


def read_clean_transactions():
    """Đọc toàn bộ transactions sạch từ file clean_transactions.csv."""
    path = clean_data_dir / "clean_transactions.csv"
    return read_csv(path) if path.exists() else []


def build_dimensions():
    """Xây dựng dimension table từ dữ liệu sạch."""
    mapping = {
        "customers": ("dim_customer", "dim_customer.csv"),
        "accounts": ("dim_account", "dim_account.csv"),
        "cards": ("dim_card", "dim_card.csv"),
        "merchants": ("dim_merchant", "dim_merchant.csv"),
        "branches": ("dim_branch", "dim_branch.csv"),
    }
    outputs = []
    for clean_name, (_dataset, file_name) in mapping.items():
        rows = read_clean_table(clean_name)
        out = curated_data_dir / file_name
        write_csv(out, rows)
        outputs.append(out)
    return outputs


def build_facts():
    """Xây dựng fact table từ dữ liệu sạch."""
    outputs = []
    out = curated_data_dir / "fact_transaction.csv"
    write_csv(out, read_clean_transactions())
    outputs.append(out)

    for clean_name, _dataset, file_name in [
        ("loans", "fact_loan", "fact_loan.csv"),
        ("repayments", "fact_repayment", "fact_repayment.csv"),
    ]:
        rows = read_clean_table(clean_name)
        out = curated_data_dir / file_name
        write_csv(out, rows)
        outputs.append(out)
    return outputs


def build_daily_transaction_summary():
    """Xây dựng bảng tổng hợp giao dịch theo ngày, kênh, category, tỉnh."""
    grouped = {}
    for row in read_clean_transactions():
        key = (row["transaction_date"], row["channel"], row["merchant_category"], row["province"])
        item = grouped.setdefault(key, {
            "transaction_date": row["transaction_date"],
            "channel": row["channel"],
            "merchant_category": row["merchant_category"],
            "province": row["province"],
            "total_transactions": 0,
            "success_transactions": 0,
            "failed_transactions": 0,
            "total_amount_vnd": 0,
            "max_amount_vnd": 0,
            "outlier_transaction_count": 0,
        })
        amount = to_int(row.get("amount_vnd"))
        item["total_transactions"] += 1
        item["success_transactions"] += 1 if row.get("status") == "SUCCESS" else 0
        item["failed_transactions"] += 1 if row.get("status") == "FAILED" else 0
        item["total_amount_vnd"] += amount
        item["max_amount_vnd"] = max(item["max_amount_vnd"], amount)
        item["outlier_transaction_count"] += to_int(row.get("is_outlier"))
    rows = []
    for item in grouped.values():
        total = item["total_transactions"]
        item["avg_amount_vnd"] = round(item["total_amount_vnd"] / total, 2) if total else 0
        item["failed_rate"] = round(item["failed_transactions"] / total, 4) if total else 0
        rows.append(item)
    out = curated_data_dir / "daily_transaction_summary.csv"
    write_csv(out, rows)
    return out


def build_curated_tables():
    """Xây dựng toàn bộ dữ liệu phục vụ phân tích."""
    build_dimensions()
    build_facts()
    build_daily_transaction_summary()


def to_int(value):
    """Ép kiểu số an toàn."""
    try:
        if value in ["", None]:
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


if __name__ == "__main__":
    build_curated_tables()
