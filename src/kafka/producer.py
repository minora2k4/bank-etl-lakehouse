import argparse
import json
import random
import sys
import time
from datetime import datetime
from importlib import import_module, util

from config.settings import kafka_bootstrap_servers, kafka_topics, raw_data_dir, source_db_dir
from kafka.generate import amount_for_segment, channels, source_for_channel, transaction_types
from utils.io import append_csv, read_csv

_confluent_kafka = import_module("confluent_kafka") if util.find_spec("confluent_kafka") else None
Producer = getattr(_confluent_kafka, "Producer", None)


def load_reference_data():
    """Nạp accounts/customers/merchants do simulator sinh ra."""
    accounts = read_csv(source_db_dir / "accounts.csv") if (source_db_dir / "accounts.csv").exists() else []
    customers = read_csv(source_db_dir / "customers.csv") if (source_db_dir / "customers.csv").exists() else []
    merchants = read_csv(source_db_dir / "merchants.csv") if (source_db_dir / "merchants.csv").exists() else []
    customers_by_id = {row["customer_id"]: row for row in customers}
    accounts = [row for row in accounts if row.get("customer_id") in customers_by_id]
    return accounts, customers_by_id, merchants


def build_transaction_event(sequence, accounts, customers_by_id, merchants):
    """Tạo một event giao dịch sẵn sàng gửi vào Kafka."""
    account = random.choice(accounts)
    customer = customers_by_id[account["customer_id"]]
    merchant = random.choice(merchants) if merchants else {}
    channel = random.choices(channels, weights=[45, 20, 15, 10, 7, 3], k=1)[0]
    now = datetime.now().replace(microsecond=0)
    event = {
        "transaction_id": f"TXN_STREAM_{sequence:012d}",
        "account_id": account["account_id"],
        "customer_id": account["customer_id"],
        "card_id": "",
        "transaction_time": now.isoformat(),
        "ingestion_time": now.isoformat(),
        "amount_vnd": amount_for_segment(customer.get("customer_segment", "MASS")),
        "transaction_type": random.choice(transaction_types),
        "channel": channel,
        "merchant_id": merchant.get("merchant_id", "") if channel in ["VPBANK_NEO", "NAPAS_QR", "POS"] else "",
        "merchant_category": merchant.get("merchant_category", "") if channel in ["VPBANK_NEO", "NAPAS_QR", "POS"] else "",
        "province": merchant.get("province", customer.get("province", "UNKNOWN")),
        "currency": "VND",
        "status": "SUCCESS",
        "source_system": source_for_channel(channel),
    }
    return event


def send_to_kafka(events, bootstrap_servers, topic):
    """Gửi event bằng confluent-kafka khi dependency đã được cài."""
    if Producer is None:
        raise RuntimeError("Cần cài confluent-kafka để dùng --sink kafka")

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    for event in events:
        producer.produce(
            topic,
            key=event["transaction_id"].encode("utf-8"),
            value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
        )
    producer.flush()


def write_stdout(events):
    for event in events:
        print(json.dumps(event, ensure_ascii=False), flush=True)


def write_raw_csv(events):
    append_csv(raw_data_dir / "raw_transactions.csv", events)


def produce(count, interval_seconds, sink, bootstrap_servers, topic, seed):
    """Sinh event giao dịch ra stdout, Kafka, raw CSV hoặc tất cả sink."""
    random.seed(seed)
    accounts, customers_by_id, merchants = load_reference_data()
    if not accounts:
        raise RuntimeError("Không tìm thấy account. Hãy chạy kafka.generate trước.")

    for sequence in range(1, count + 1):
        event = build_transaction_event(sequence, accounts, customers_by_id, merchants)
        if sink in ["stdout", "all"]:
            write_stdout([event])
        if sink in ["csv", "all"]:
            write_raw_csv([event])
        if sink in ["kafka", "all"]:
            send_to_kafka([event], bootstrap_servers, topic)
        if interval_seconds and sequence < count:
            time.sleep(interval_seconds)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--interval-seconds", type=float, default=0.2)
    parser.add_argument("--sink", choices=["stdout", "kafka", "csv", "all"], default="stdout")
    parser.add_argument("--bootstrap-servers", default=kafka_bootstrap_servers)
    parser.add_argument("--topic", default=kafka_topics["raw_transactions"])
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    try:
        produce(args.count, args.interval_seconds, args.sink, args.bootstrap_servers, args.topic, args.seed)
    except Exception as exc:
        print(f"producer lỗi: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
