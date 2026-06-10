from banking_lakehouse.config.settings import default_batch_id, default_ingestion_date, postgres_tables, raw_data_dir, source_db_dir, source_files_dir
from banking_lakehouse.utils.io import now_ts, read_csv, row_hash, write_csv


def add_raw_metadata(rows, ingestion_date, batch_id, source_system, raw_file_name):
    """ThÃªm metadata audit cho dá»¯ liá»‡u raw."""
    out = []
    for row in rows:
        raw = dict(row)
        row["ingestion_time"] = row.get("ingestion_time") or now_ts()
        row["ingestion_date"] = ingestion_date
        row["batch_id"] = batch_id
        row["source_system"] = row.get("source_system") or source_system
        row["raw_file_name"] = raw_file_name
        row["record_hash"] = row_hash(raw)
        out.append(row)
    return out


def load_database_sources(ingestion_date=default_ingestion_date, batch_id=default_batch_id):
    """Load source tables mÃ´ phá»ng database vÃ o vÃ¹ng raw."""
    outputs = []
    for table in postgres_tables:
        path = source_db_dir / f"{table}.csv"
        if not path.exists():
            continue
        rows = add_raw_metadata(read_csv(path), ingestion_date, batch_id, "POSTGRES", path.name)
        out = raw_data_dir / f"{table}.csv"
        write_csv(out, rows)
        outputs.append(out)
    return outputs


def load_file_sources(ingestion_date=default_ingestion_date, batch_id=default_batch_id):
    """Load source files vÃ o vÃ¹ng raw."""
    outputs = []
    files = [
        ("transactions", source_files_dir / "historical_transactions_2026_06.csv", "transactions.csv"),
        ("merchant_risk", source_files_dir / "merchant_risk.csv", "merchant_risk.csv"),
    ]
    for dataset, path, output_name in files:
        if not path.exists():
            continue
        rows = add_raw_metadata(read_csv(path), ingestion_date, batch_id, "CSV_FILE", path.name)
        out = raw_data_dir / output_name
        write_csv(out, rows)
        outputs.append(out)
    return outputs


def load_sources(ingestion_date=default_ingestion_date, batch_id=default_batch_id):
    """Load toÃ n bá»™ source systems vÃ o lakehouse."""
    return load_database_sources(ingestion_date, batch_id) + load_file_sources(ingestion_date, batch_id)


if __name__ == "__main__":
    load_sources()
