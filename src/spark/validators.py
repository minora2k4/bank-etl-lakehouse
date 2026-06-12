from collections import defaultdict
from datetime import datetime

from spark.error_handler import error_record


valid_gender = {"MALE", "FEMALE", "OTHER", "M", "F", "male"}
valid_account_status = {"ACTIVE", "CLOSED", "SUSPENDED"}
valid_transaction_status = {"SUCCESS", "FAILED", "PENDING"}
valid_channels = {"VPBANK_NEO", "NAPAS_QR", "ATM", "POS", "BRANCH", "INTERNET_BANKING"}


def normalize_gender(value):
    """Chuẩn hóa gender về enum chính."""
    mapping = {"M": "MALE", "F": "FEMALE", "male": "MALE"}
    return mapping.get(value, value or "OTHER")


def validate_customers(rows):
    """Kiểm tra và làm sạch bảng customers."""
    valid = []
    invalid = []
    for row in rows:
        errors = []
        if not row.get("customer_id"):
            errors.append(("MISSING_CUSTOMER_ID", "customer_id", "not_null", "customer_id is required"))
        age = to_int(row.get("age"))
        if age is None or age < 18 or age > 100:
            errors.append(("INVALID_AGE", "age", "range", "age must be between 18 and 100"))
        if row.get("gender") not in valid_gender:
            row["gender"] = "OTHER"
        row["gender"] = normalize_gender(row.get("gender"))
        row["province"] = row.get("province") or "UNKNOWN"
        row["monthly_income_vnd"] = row.get("monthly_income_vnd") or "0"
        row["age_group"] = age_group(age)
        row["income_band"] = income_band(to_int(row["monthly_income_vnd"]))
        row["data_quality_status"] = "FIXED_MINOR_ISSUE" if row["province"] == "UNKNOWN" else "VALID"
        if errors:
            for e in errors:
                invalid.append(error_record(row, "customers", *e))
        else:
            valid.append(row)
    return valid, invalid


def validate_accounts(rows, customer_ids):
    """Kiểm tra và làm sạch bảng accounts."""
    valid = []
    invalid = []
    for row in rows:
        errors = []
        if row.get("customer_id") not in customer_ids:
            errors.append(("CUSTOMER_NOT_FOUND", "customer_id", "referential_integrity", "customer_id not found"))
        if to_int(row.get("balance_vnd")) is None or to_int(row.get("balance_vnd")) < 0:
            errors.append(("INVALID_BALANCE", "balance_vnd", "numeric_min", "balance_vnd must be >= 0"))
        if row.get("status") not in valid_account_status:
            errors.append(("INVALID_STATUS", "status", "valid_values", "status is invalid"))
        if errors:
            for e in errors:
                invalid.append(error_record(row, "accounts", *e))
        else:
            row["data_quality_status"] = "VALID"
            valid.append(row)
    return valid, invalid


def validate_passthrough(rows, source_table):
    """Làm sạch nhẹ cho bảng dimension/fact không có rule nghiêm ngặt trong MVP."""
    for row in rows:
        row["data_quality_status"] = "VALID"
    return rows, []


def validate_transactions(rows, account_ids, customer_ids):
    """Kiểm tra giao dịch, tách bản ghi lỗi và loại trùng."""
    invalid = []
    candidates = []
    now = datetime.now()
    for row in rows:
        apply_schema_drift(row)
        errors = []
        if not row.get("transaction_id"):
            errors.append(("MISSING_TRANSACTION_ID", "transaction_id", "not_null", "transaction_id is required"))
        if not row.get("account_id") or row.get("account_id") not in account_ids:
            errors.append(("MISSING_ACCOUNT_ID", "account_id", "referential_integrity", "account_id is missing or not found"))
        if not row.get("customer_id") or row.get("customer_id") not in customer_ids:
            errors.append(("MISSING_CUSTOMER_ID", "customer_id", "referential_integrity", "customer_id is missing or not found"))
        amount = to_int(row.get("amount_vnd"))
        if amount is None or amount <= 0:
            errors.append(("INVALID_AMOUNT", "amount_vnd", "numeric_min", "amount_vnd must be > 0"))
        txn_time = parse_ts(row.get("transaction_time"))
        if txn_time is None:
            errors.append(("INVALID_TIMESTAMP", "transaction_time", "timestamp_parse", "transaction_time is invalid"))
        elif txn_time > now:
            errors.append(("FUTURE_TIMESTAMP", "transaction_time", "not_future", "transaction_time is in future"))
        if row.get("status") not in valid_transaction_status:
            errors.append(("INVALID_STATUS", "status", "valid_values", "status is invalid"))
        if row.get("channel") not in valid_channels:
            errors.append(("INVALID_CHANNEL", "channel", "valid_values", "channel is invalid"))
        if row.get("currency") and row.get("currency") != "VND":
            errors.append(("INVALID_CURRENCY", "currency", "valid_values", "currency must be VND"))
        if errors:
            for e in errors:
                invalid.append(error_record(row, "transactions", *e))
        else:
            row = clean_transaction(row, amount, txn_time)
            candidates.append(row)
    valid, duplicate_invalid = deduplicate_transactions(candidates)
    invalid.extend(duplicate_invalid)
    return valid, invalid


def clean_transaction(row, amount, txn_time):
    """Chuẩn hóa giao dịch và tạo trường dẫn xuất."""
    ingestion = parse_ts(row.get("ingestion_time")) or txn_time
    delay_hours = round((ingestion - txn_time).total_seconds() / 3600, 2)
    row["amount_vnd"] = amount
    row["merchant_category"] = row.get("merchant_category") or "UNKNOWN"
    row["province"] = row.get("province") or "UNKNOWN"
    row["transaction_date"] = txn_time.date().isoformat()
    row["transaction_hour"] = txn_time.hour
    row["delay_hours"] = delay_hours
    row["is_late_arriving"] = 1 if delay_hours > 24 else 0
    row["is_outlier"] = 1 if amount >= 500_000_000 else 0
    row["is_night_transaction"] = 1 if txn_time.hour < 6 or txn_time.hour >= 22 else 0
    row["data_quality_status"] = "VALID_WITH_FLAGS" if row["is_late_arriving"] or row["is_outlier"] else "VALID"
    row["processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return row


def deduplicate_transactions(rows):
    """Giữ giao dịch có ingestion_time mới nhất khi trùng transaction_id."""
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["transaction_id"]].append(row)
    valid = []
    invalid = []
    for txn_id, group in grouped.items():
        group = sorted(group, key=lambda r: parse_ts(r.get("ingestion_time")) or datetime.min, reverse=True)
        valid.append(group[0])
        for dup in group[1:]:
            invalid.append(error_record(
                dup,
                "transactions",
                "DUPLICATE_TRANSACTION_ID",
                "transaction_id",
                "unique",
                f"duplicate transaction_id {txn_id}",
            ))
    return valid, invalid


def apply_schema_drift(row):
    """Ánh xạ schema drift transaction_amount_vnd về amount_vnd nếu có."""
    if "amount_vnd" not in row and "transaction_amount_vnd" in row:
        row["amount_vnd"] = row["transaction_amount_vnd"]


def age_group(age):
    """Tạo age_group cho customers sau chuẩn hóa."""
    if age is None:
        return "UNKNOWN"
    if age < 25:
        return "18_24"
    if age < 35:
        return "25_34"
    if age < 50:
        return "35_49"
    return "50_PLUS"


def income_band(income):
    """Tạo income_band cho customers sau chuẩn hóa."""
    if income is None or income == 0:
        return "UNKNOWN"
    if income < 20_000_000:
        return "LOW"
    if income < 60_000_000:
        return "MEDIUM"
    return "HIGH"


def to_int(value):
    """Ép kiểu số an toàn."""
    try:
        if value in ["", None]:
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def parse_ts(value):
    """Phân tích timestamp từ source."""
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(str(value), fmt)
        except ValueError:
            continue
    return None
