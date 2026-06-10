import uuid

from banking_lakehouse.utils.io import json_dumps


def quarantine_record(row, source_table, error_type, failed_column, rule_name, message):
    """Tạo record quarantine có raw payload và thông tin rule lỗi."""
    return {
        "quarantine_id": str(uuid.uuid4()),
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