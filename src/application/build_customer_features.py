from collections import defaultdict
from datetime import date, timedelta

from application.build_curated_tables import read_clean_table, read_clean_transactions, to_int
from config.settings import curated_data_dir
from utils.io import write_csv


def build_customer_features(feature_date="2026-06-10"):
    """Xây dựng customer_summary cho Project 2 credit risk."""
    customers = read_clean_table("customers")
    accounts = read_clean_table("accounts")
    loans = read_clean_table("loans")
    repayments = read_clean_table("repayments")
    txns = filter_last_30d(read_clean_transactions(), feature_date)

    acc = summarize_accounts(accounts)
    txn = summarize_transactions(txns)
    loan = summarize_loans(loans)
    rpm = summarize_repayments(repayments)

    rows = []
    for c in customers:
        cid = c["customer_id"]
        rows.append({
            "customer_id": cid,
            "age": c.get("age", ""),
            "province": c.get("province", ""),
            "monthly_income_vnd": c.get("monthly_income_vnd", ""),
            "customer_segment": c.get("customer_segment", ""),
            **acc.get(cid, zero_account()),
            **txn.get(cid, zero_transaction()),
            **loan.get(cid, zero_loan()),
            **rpm.get(cid, zero_repayment()),
        })

    out = curated_data_dir / "customer_summary.csv"
    write_csv(out, rows)
    return out


def filter_last_30d(rows, feature_date):
    """Lọc giao dịch trong 30 ngày gần feature_date."""
    end = date.fromisoformat(feature_date)
    start = end - timedelta(days=30)
    return [row for row in rows if start <= date.fromisoformat(row["transaction_date"]) <= end]


def summarize_accounts(rows):
    """Tổng hợp tài khoản theo customer."""
    out = defaultdict(zero_account)
    for row in rows:
        item = out[row["customer_id"]]
        item["num_accounts"] += 1
        item["total_balance_vnd"] += to_int(row.get("balance_vnd"))
    return dict(out)


def summarize_transactions(rows):
    """Tổng hợp hành vi giao dịch 30 ngày."""
    out = defaultdict(zero_transaction)
    active_days = defaultdict(set)
    failed_counts = defaultdict(int)
    for row in rows:
        item = out[row["customer_id"]]
        amount = to_int(row.get("amount_vnd"))
        item["transaction_count_30d"] += 1
        item["transaction_amount_30d_vnd"] += amount
        item["max_transaction_amount_30d_vnd"] = max(item["max_transaction_amount_30d_vnd"], amount)
        if row.get("status") == "FAILED":
            failed_counts[row["customer_id"]] += 1
        item["night_transaction_count_30d"] += to_int(row.get("is_night_transaction"))
        item["outlier_transaction_count_30d"] += to_int(row.get("is_outlier"))
        active_days[row["customer_id"]].add(row["transaction_date"])
    for cid, item in out.items():
        count = item["transaction_count_30d"]
        item["avg_transaction_amount_30d_vnd"] = round(item["transaction_amount_30d_vnd"] / count, 2) if count else 0
        item["failed_transaction_rate_30d"] = round(failed_counts[cid] / count, 4) if count else 0
        item["active_days_30d"] = len(active_days[cid])
    return dict(out)


def summarize_loans(rows):
    """Tổng hợp khoản vay theo customer."""
    out = defaultdict(zero_loan)
    for row in rows:
        item = out[row["customer_id"]]
        item["loan_count"] += 1
        item["total_loan_amount_vnd"] += to_int(row.get("loan_amount_vnd"))
    return dict(out)


def summarize_repayments(rows):
    """Tổng hợp lịch sử trả nợ theo customer."""
    out = defaultdict(zero_repayment)
    for row in rows:
        item = out[row["customer_id"]]
        item["max_days_past_due"] = max(item["max_days_past_due"], to_int(row.get("days_past_due")))
        item["late_payment_count"] += 1 if row.get("repayment_status") == "LATE" else 0
        item["missed_payment_count"] += 1 if row.get("repayment_status") == "MISSED" else 0
    return dict(out)


def zero_account():
    """Giá trị mặc định cho account summary."""
    return {"num_accounts": 0, "total_balance_vnd": 0}


def zero_transaction():
    """Giá trị mặc định cho transaction summary."""
    return {
        "transaction_count_30d": 0,
        "transaction_amount_30d_vnd": 0,
        "avg_transaction_amount_30d_vnd": 0,
        "max_transaction_amount_30d_vnd": 0,
        "failed_transaction_rate_30d": 0,
        "night_transaction_count_30d": 0,
        "outlier_transaction_count_30d": 0,
        "active_days_30d": 0,
    }


def zero_loan():
    """Giá trị mặc định cho loan summary."""
    return {"loan_count": 0, "total_loan_amount_vnd": 0}


def zero_repayment():
    """Giá trị mặc định cho repayment summary."""
    return {"max_days_past_due": 0, "late_payment_count": 0, "missed_payment_count": 0}


if __name__ == "__main__":
    build_customer_features()
