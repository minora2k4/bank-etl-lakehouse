from config.settings import default_batch_id, default_ingestion_date, raw_data_dir, source_files_dir
from utils.io import now_ts, read_csv, row_hash, write_csv


def add_raw_metadata(rows, ingestion_date, batch_id, source_system, raw_file_name):
    """Thêm metadata audit cho raw event row."""
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
    """Giữ master/tham chiếu data ở source file PostgreSQL, không copy vào lakehouse raw."""
    return []


def load_file_sources(ingestion_date=default_ingestion_date, batch_id=default_batch_id):
    """Nạp event giao dịch vào raw event log của lakehouse."""
    outputs = []
    files = [
        (source_files_dir / "historical_transactions_2026_06.csv", "raw_transactions.csv"),
    ]
    for path, output_name in files:
        if not path.exists():
            continue
        rows = add_raw_metadata(read_csv(path), ingestion_date, batch_id, "CSV_FILE", path.name)
        out = raw_data_dir / output_name
        write_csv(out, rows)
        outputs.append(out)
    return outputs


def load_sources(ingestion_date=default_ingestion_date, batch_id=default_batch_id):
    """Nạp event nguồn vào raw layout tương thích lakehouse."""
    return load_database_sources(ingestion_date, batch_id) + load_file_sources(ingestion_date, batch_id)


if __name__ == "__main__":
    load_sources()
