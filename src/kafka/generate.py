import argparse
import hashlib
import random
from datetime import date, datetime, timedelta

from config.settings import default_volumes, source_db_dir, source_files_dir
from kafka.inject import inject_account_issues, inject_customer_issues, inject_transaction_issues
from utils.io import ensure_dir, write_csv


provinces = [
    ("HA_NOI", "CAU_GIAY"),
    ("HO_CHI_MINH", "QUAN_1"),
    ("DA_NANG", "HAI_CHAU"),
    ("HAI_PHONG", "LE_CHAN"),
    ("CAN_THO", "NINH_KIEU"),
    ("BAC_NINH", "TU_SON"),
    ("DONG_NAI", "BIEN_HOA"),
    ("BINH_DUONG", "THU_DAU_MOT"),
]
occupations = ["OFFICE_STAFF", "BUSINESS_OWNER", "ENGINEER", "TEACHER", "DOCTOR", "FREELANCER"]
segments = ["MASS", "MASS_AFFLUENT", "HNW"]
channels = ["VPBANK_NEO", "NAPAS_QR", "POS", "ATM", "INTERNET_BANKING", "BRANCH"]
transaction_types = ["TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT", "CARD_PURCHASE"]
merchant_categories = ["GROCERY", "RESTAURANT", "ECOMMERCE", "TRAVEL", "EDUCATION", "HEALTHCARE", "OTHER"]


def stable_hash(value):
    """Hash PII mô phỏng thay vì lưu dữ liệu nhạy cảm raw."""
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def pick_segment():
    """Chọn customer segment theo tỷ lệ trong tài liệu."""
    return random.choices(segments, weights=[60, 30, 10], k=1)[0]


def income_for_segment(segment):
    """Sinh thu nhập VND bám theo segment."""
    ranges = {
        "MASS": (5_000_000, 20_000_000),
        "MASS_AFFLUENT": (20_000_000, 60_000_000),
        "HNW": (60_000_000, 300_000_000),
    }
    lo, hi = ranges[segment]
    return random.randrange(lo, hi, 500_000)


def amount_for_segment(segment):
    """Sinh số tiền giao dịch VND theo segment."""
    ranges = {
        "MASS": (50_000, 3_000_000),
        "MASS_AFFLUENT": (100_000, 20_000_000),
        "HNW": (1_000_000, 500_000_000),
    }
    lo, hi = ranges[segment]
    return random.randrange(lo, hi, 10_000)


def generate_branches(n):
    """Sinh danh mục chi nhánh VPBank mô phỏng."""
    rows = []
    for i in range(1, n + 1):
        province, district = random.choice(provinces)
        rows.append({
            "branch_id": f"BR_{i:03d}",
            "branch_name": f"VPBank {province.title().replace('_', ' ')} {district.title().replace('_', ' ')}",
            "province": province,
            "district": district,
            "branch_type": random.choice(["BRANCH", "TRANSACTION_OFFICE"]),
        })
    return rows


def generate_customers(n):
    """Sinh hồ sơ khách hàng Việt Nam không dấu."""
    first = ["An", "Binh", "Chi", "Dung", "Giang", "Hoa", "Huy", "Linh", "Minh", "Nam", "Phuong", "Trang"]
    middle = ["Van", "Thi", "Minh", "Quoc", "Thanh", "Ngoc"]
    last = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Dang", "Bui"]
    rows = []
    today = date(2026, 6, 10)
    for i in range(1, n + 1):
        segment = pick_segment()
        province, district = random.choice(provinces)
        age = random.randint(18, 70)
        dob = today - timedelta(days=age * 365 + random.randint(0, 364))
        rows.append({
            "customer_id": f"CUST_{i:06d}",
            "full_name": f"{random.choice(last)} {random.choice(middle)} {random.choice(first)}",
            "gender": random.choice(["MALE", "FEMALE", "OTHER"]),
            "dob": dob.isoformat(),
            "age": age,
            "province": province,
            "district": district,
            "occupation": random.choice(occupations),
            "monthly_income_vnd": income_for_segment(segment),
            "customer_segment": segment,
            "phone_hash": stable_hash(f"09{i:08d}"),
            "email_hash": stable_hash(f"user{i}@example.local"),
            "cccd_hash": stable_hash(f"0{i:011d}"),
            "created_at": random_datetime(2021, 2025),
            "updated_at": random_datetime(2025, 2026),
        })
    return rows


def generate_accounts(customers, branches):
    """Sinh tài khoản có FK về customer và branch."""
    rows = []
    account_no = 1
    for customer in customers:
        count = random.choices([1, 2, 3], weights=[70, 25, 5], k=1)[0]
        for _ in range(count):
            segment = customer["customer_segment"]
            base = income_for_segment(segment)
            rows.append({
                "account_id": f"ACC_{account_no:06d}",
                "customer_id": customer["customer_id"],
                "account_type": random.choice(["CASA", "SAVING", "CREDIT"]),
                "open_date": random_date(2021, 2026),
                "balance_vnd": max(0, base * random.randint(0, 8)),
                "status": random.choices(["ACTIVE", "CLOSED", "SUSPENDED"], weights=[88, 7, 5], k=1)[0],
                "branch_id": random.choice(branches)["branch_id"],
                "created_at": random_datetime(2021, 2026),
            })
            account_no += 1
    return rows


def generate_cards(customers, accounts, target_n):
    """Sinh thẻ ATM/debit/credit và chỉ lưu số thẻ masked."""
    rows = []
    account_by_customer = {}
    for account in accounts:
        account_by_customer.setdefault(account["customer_id"], []).append(account)
    eligible_customers = [c for c in customers if c["customer_id"] in account_by_customer]
    for i, customer in enumerate(random.sample(eligible_customers, min(target_n, len(eligible_customers))), start=1):
        account = random.choice(account_by_customer[customer["customer_id"]])
        issued = date.fromisoformat(random_date(2021, 2026))
        rows.append({
            "card_id": f"CARD_{i:06d}",
            "customer_id": customer["customer_id"],
            "account_id": account["account_id"],
            "card_type": random.choice(["ATM", "DEBIT", "CREDIT"]),
            "card_number_masked": f"9704********{random.randint(1000, 9999)}",
            "issued_date": issued.isoformat(),
            "expiry_date": date(issued.year + 5, issued.month, issued.day).isoformat(),
            "status": random.choices(["ACTIVE", "BLOCKED", "EXPIRED"], weights=[90, 7, 3], k=1)[0],
        })
    return rows


def generate_merchants(n):
    """Sinh merchant theo tỉnh và risk level."""
    rows = []
    for i in range(1, n + 1):
        province, _ = random.choice(provinces)
        category = random.choice(merchant_categories)
        rows.append({
            "merchant_id": f"MERC_{i:06d}",
            "merchant_name": f"Merchant {i:06d}",
            "merchant_category": category,
            "province": province,
            "risk_level": random.choices(["LOW", "MEDIUM", "HIGH"], weights=[72, 22, 6], k=1)[0],
        })
    return rows


def generate_loans(customers, n):
    """Sinh khoản vay cho Project 2 credit risk."""
    rows = []
    for i, customer in enumerate(random.sample(customers, min(n, len(customers))), start=1):
        start = date.fromisoformat(random_date(2022, 2026))
        term = random.choice([12, 24, 36, 48, 60, 120, 240])
        rows.append({
            "loan_id": f"LOAN_{i:06d}",
            "customer_id": customer["customer_id"],
            "loan_type": random.choice(["CASH_LOAN", "HOME_LOAN", "AUTO_LOAN", "CREDIT_CARD_LOAN"]),
            "loan_amount_vnd": random.randrange(20_000_000, 2_000_000_000, 1_000_000),
            "interest_rate_pct": round(random.uniform(7.5, 22.5), 2),
            "term_months": term,
            "start_date": start.isoformat(),
            "end_date": add_months(start, term).isoformat(),
            "loan_status": random.choices(["ACTIVE", "CLOSED", "DEFAULT"], weights=[74, 22, 4], k=1)[0],
        })
    return rows


def generate_repayments(loans):
    """Sinh lịch sử trả nợ theo từng khoản vay."""
    rows = []
    repayment_no = 1
    for loan in loans:
        months = min(int(loan["term_months"]), random.randint(3, 18))
        monthly_due = int(int(loan["loan_amount_vnd"]) / int(loan["term_months"]))
        start = date.fromisoformat(loan["start_date"])
        for m in range(months):
            due = add_months(start, m + 1)
            status = random.choices(["ON_TIME", "LATE", "MISSED"], weights=[78, 17, 5], k=1)[0]
            if status == "ON_TIME":
                paid = due - timedelta(days=random.randint(0, 2))
                dpd = 0
                paid_amount = monthly_due
            elif status == "LATE":
                dpd = random.randint(1, 45)
                paid = due + timedelta(days=dpd)
                paid_amount = monthly_due
            else:
                paid = ""
                dpd = random.randint(30, 120)
                paid_amount = 0
            rows.append({
                "repayment_id": f"RPM_{repayment_no:07d}",
                "loan_id": loan["loan_id"],
                "customer_id": loan["customer_id"],
                "due_date": due.isoformat(),
                "paid_date": paid if paid == "" else paid.isoformat(),
                "due_amount_vnd": monthly_due,
                "paid_amount_vnd": paid_amount,
                "days_past_due": dpd,
                "repayment_status": status,
            })
            repayment_no += 1
    return rows


def generate_transactions(accounts, customers, cards, merchants, n):
    """Sinh giao dịch lịch sử lớn nhất trong pipeline."""
    customer_by_id = {c["customer_id"]: c for c in customers}
    valid_accounts = [a for a in accounts if a["customer_id"] in customer_by_id]
    cards_by_account = {}
    for card in cards:
        cards_by_account.setdefault(card["account_id"], []).append(card)
    rows = []
    for i in range(1, n + 1):
        account = random.choice(valid_accounts)
        customer = customer_by_id[account["customer_id"]]
        merchant = random.choice(merchants)
        channel = random.choices(channels, weights=[45, 20, 15, 10, 7, 3], k=1)[0]
        card = random.choice(cards_by_account.get(account["account_id"], [{}]))
        txn_time = datetime(2026, 6, random.randint(1, 10), random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
        ingestion = txn_time + timedelta(seconds=random.randint(1, 600))
        rows.append({
            "transaction_id": f"TXN_{i:09d}",
            "account_id": account["account_id"],
            "customer_id": customer["customer_id"],
            "card_id": card.get("card_id", ""),
            "transaction_time": txn_time.strftime("%Y-%m-%d %H:%M:%S"),
            "ingestion_time": ingestion.strftime("%Y-%m-%d %H:%M:%S"),
            "amount_vnd": amount_for_segment(customer["customer_segment"]),
            "transaction_type": random.choice(transaction_types),
            "channel": channel,
            "merchant_id": merchant["merchant_id"] if channel in ["VPBANK_NEO", "NAPAS_QR", "POS"] else "",
            "merchant_category": merchant["merchant_category"] if channel in ["VPBANK_NEO", "NAPAS_QR", "POS"] else "",
            "province": merchant["province"],
            "currency": "VND",
            "status": random.choices(["SUCCESS", "FAILED", "PENDING"], weights=[91, 6, 3], k=1)[0],
            "device_id": f"DEV_{random.randint(1, 99999):05d}" if channel in ["VPBANK_NEO", "INTERNET_BANKING"] else "",
            "ip_address": f"10.{random.randint(0, 20)}.{random.randint(0, 255)}.{random.randint(1, 254)}",
            "source_system": source_for_channel(channel),
        })
    inject_transaction_issues(rows)
    return rows


def source_for_channel(channel):
    """Ánh xạ channel sang source system."""
    return {
        "VPBANK_NEO": "MOBILE_APP",
        "NAPAS_QR": "POS_NETWORK",
        "POS": "POS_NETWORK",
        "ATM": "ATM_SWITCH",
        "INTERNET_BANKING": "MOBILE_APP",
        "BRANCH": "CORE_BANKING",
    }.get(channel, "UNKNOWN")


def random_date(start_year, end_year):
    """Sinh ngày ngẫu nhiên dạng ISO."""
    start = date(start_year, 1, 1)
    end = date(end_year, 6, 10)
    return (start + timedelta(days=random.randint(0, (end - start).days))).isoformat()


def random_datetime(start_year, end_year):
    """Sinh timestamp ngẫu nhiên dạng text."""
    d = date.fromisoformat(random_date(start_year, end_year))
    dt = datetime(d.year, d.month, d.day, random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def add_months(d, months):
    """Cộng tháng không phụ thuộc thư viện ngoài."""
    month = d.month - 1 + months
    year = d.year + month // 12
    month = month % 12 + 1
    day = min(d.day, [31, 29 if year % 4 == 0 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])
    return date(year, month, day)


def generate_all(volumes=None, seed=42):
    """Sinh toàn bộ source data và ghi ra source_db/source_files."""
    random.seed(seed)
    volumes = {**default_volumes, **(volumes or {})}
    ensure_dir(source_db_dir)
    ensure_dir(source_files_dir)

    branches = generate_branches(volumes["branches"])
    customers = generate_customers(volumes["customers"])
    inject_customer_issues(customers)
    accounts = generate_accounts(customers, branches)
    inject_account_issues(accounts)
    cards = generate_cards(customers, accounts, min(int(volumes["customers"] * 0.8), len(accounts)))
    merchants = generate_merchants(volumes["merchants"])
    loans = generate_loans(customers, volumes["loans"])
    repayments = generate_repayments(loans)
    transactions = generate_transactions(accounts, customers, cards, merchants, volumes["transactions"])

    source_db = {
        "customers": customers,
        "accounts": accounts,
        "cards": cards,
        "loans": loans,
        "repayments": repayments,
        "merchants": merchants,
        "branches": branches,
    }
    for name, rows in source_db.items():
        write_csv(source_db_dir / f"{name}.csv", rows)
    write_csv(source_files_dir / "historical_transactions_2026_06.csv", transactions)
    write_csv(source_files_dir / "merchant_risk.csv", merchants)
    return {**source_db, "transactions": transactions}


def main():
    """CLI sinh dữ liệu source."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--customers", type=int, default=default_volumes["customers"])
    parser.add_argument("--transactions", type=int, default=default_volumes["transactions"])
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    generate_all({"customers": args.customers, "transactions": args.transactions}, seed=args.seed)


if __name__ == "__main__":
    main()
