import random
from datetime import datetime


def inject_customer_issues(rows):
    """Inject lỗi khách hàng để kiểm thử data quality."""
    if not rows:
        return
    for row in random.sample(rows, max(1, len(rows) // 100)):
        row["province"] = ""
    for row in random.sample(rows, max(1, len(rows) // 100)):
        row["monthly_income_vnd"] = ""
    for row in random.sample(rows, max(1, len(rows) // 80)):
        row["gender"] = random.choice(["M", "F", "male"])
    random.choice(rows)["age"] = 150


def inject_account_issues(rows):
    """Inject lỗi tài khoản để kiểm thử FK và valid value."""
    if not rows:
        return
    random.choice(rows)["customer_id"] = "CUST_NOT_FOUND"
    random.choice(rows)["balance_vnd"] = -500000
    random.choice(rows)["status"] = random.choice(["UNKNOWN", "DELETED"])


def inject_transaction_issues(rows):
    """Inject lỗi transaction theo yêu cầu data quality."""
    if len(rows) < 20:
        return
    random.choice(rows)["transaction_id"] = ""
    random.choice(rows)["customer_id"] = ""
    random.choice(rows)["amount_vnd"] = -100000
    random.choice(rows)["transaction_time"] = "2028-01-01 00:00:00"
    random.choice(rows)["channel"] = "APP"
    random.choice(rows)["status"] = "DONE"
    rows[5]["transaction_id"] = rows[4]["transaction_id"]
    rows[6]["amount_vnd"] = 900_000_000
    old_time = datetime(2026, 4, 1, 8, 0, 0)
    rows[7]["transaction_time"] = old_time.strftime("%Y-%m-%d %H:%M:%S")
    rows[7]["ingestion_time"] = "2026-06-10 10:00:00"
    rows[8]["merchant_category"] = ""