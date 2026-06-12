from collections import defaultdict

from config.settings import (
    clean_data_dir,
    default_ingestion_date,
    error_data_dir,
    raw_data_dir,
    source_db_dir,
)
from spark.validators import (
    validate_accounts,
    validate_customers,
    validate_passthrough,
    validate_transactions,
)
from utils.io import read_csv, write_csv


def load_source_dataset(table):
    """Đọc dữ liệu source giống PostgreSQL dùng làm dữ liệu tham chiếu/master."""
    path = source_db_dir / f"{table}.csv"
    return read_csv(path) if path.exists() else []


def load_raw_transactions():
    """Đọc event giao dịch thô từ layout raw theo hướng streaming-first."""
    path = raw_data_dir / "raw_transactions.csv"
    return read_csv(path) if path.exists() else []


def prepare_clean_data(ingestion_date=default_ingestion_date):
    """Kiểm tra source/tham chiếu data và phân tuyến event giao dịch sang clean/error."""
    customers = load_source_dataset("customers")
    customers_valid, customers_invalid = validate_customers(customers)
    write_csv(clean_data_dir / "customers.csv", customers_valid)
    write_errors("customers", customers_invalid)

    customer_ids = {row["customer_id"] for row in customers_valid}
    accounts = load_source_dataset("accounts")
    accounts_valid, accounts_invalid = validate_accounts(accounts, customer_ids)
    write_csv(clean_data_dir / "accounts.csv", accounts_valid)
    write_errors("accounts", accounts_invalid)

    account_ids = {row["account_id"] for row in accounts_valid}
    for table in ["cards", "loans", "repayments", "merchants", "branches"]:
        rows = load_source_dataset(table)
        valid_rows, invalid_rows = validate_passthrough(rows, table)
        write_csv(clean_data_dir / f"{table}.csv", valid_rows)
        write_errors(table, invalid_rows)

    transactions = load_raw_transactions()
    transactions_valid, transactions_invalid = validate_transactions(transactions, account_ids, customer_ids)
    write_clean_transactions(transactions_valid)
    write_errors("transactions", transactions_invalid)

    return {
        "customers_valid": len(customers_valid),
        "accounts_valid": len(accounts_valid),
        "transactions_valid": len(transactions_valid),
        "invalid_records": len(customers_invalid) + len(accounts_invalid) + len(transactions_invalid),
    }


def write_clean_transactions(rows):
    """Ghi toàn bộ giao dịch clean vào một file CSV MVP."""
    write_csv(clean_data_dir / "clean_transactions.csv", rows, clean_transaction_fields())


def write_errors(source_table, rows):
    """Ghi lỗi validation vào error layer mới."""
    if source_table == "transactions":
        write_csv(error_data_dir / "error_transactions.csv", rows, error_fields())
        return
    if not rows:
        return
    rows_by_table = defaultdict(list)
    for row in rows:
        rows_by_table[row["source_table"]].append(row)
    for table, table_rows in rows_by_table.items():
        write_csv(error_data_dir / f"{table}_errors.csv", table_rows, error_fields())


def clean_transaction_fields():
    """Schema giao dịch sau kiểm tra và enrich."""
    return [
        "transaction_id",
        "account_id",
        "customer_id",
        "card_id",
        "merchant_id",
        "transaction_time",
        "transaction_date",
        "transaction_hour",
        "ingestion_time",
        "delay_hours",
        "amount_vnd",
        "transaction_type",
        "channel",
        "merchant_category",
        "province",
        "currency",
        "status",
        "is_late_arriving",
        "is_outlier",
        "is_night_transaction",
        "data_quality_status",
        "processed_at",
        "batch_id",
    ]


def error_fields():
    """Schema cho bản ghi lỗi trong lakehouse."""
    return [
        "error_id",
        "source_table",
        "raw_payload",
        "error_type",
        "error_message",
        "failed_column",
        "rule_name",
        "batch_id",
        "source_system",
        "ingestion_time",
    ]


if __name__ == "__main__":
    prepare_clean_data()
