"""Adapter ghi CSV ra lakehouse (raw/clean/error)."""

from config.settings import clean_data_dir, error_data_dir, raw_data_dir
from utils.io import append_csv, write_csv


def write_raw_transactions(rows, append=True):
    """Ghi event giao dịch thô vào CSV raw của lakehouse."""
    writer = append_csv if append else write_csv
    writer(raw_data_dir / "raw_transactions.csv", rows)


def write_clean_transactions(rows, fieldnames=None, append=True):
    """Ghi event giao dịch hợp lệ vào CSV clean của lakehouse."""
    writer = append_csv if append else write_csv
    writer(clean_data_dir / "clean_transactions.csv", rows, fieldnames)


def write_error_transactions(rows, fieldnames=None, append=True):
    """Ghi event giao dịch lỗi vào CSV error của lakehouse."""
    writer = append_csv if append else write_csv
    writer(error_data_dir / "error_transactions.csv", rows, fieldnames)
