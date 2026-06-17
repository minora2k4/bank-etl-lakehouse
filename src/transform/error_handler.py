import uuid

from utils.io import json_dumps


def error_record(row, source_table, error_type, failed_column, rule_name, message):
    """Tạo bản ghi lỗi có payload thô và thông tin rule lỗi."""
    return {
        "error_id": str(uuid.uuid4()),
        "source_table": source_table,
        "raw_payload": json_dumps(row),
        "error_type": error_type,
        "error_message": message,
        "failed_column": failed_column,
        "rule_name": rule_name,
        "batch_id": row.get("batch_id", ""),
        "source_system": row.get("source_system", ""),
        "ingestion_time": row.get("ingestion_time", ""),
    }
