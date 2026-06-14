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
from utils.logging import log_info

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
    """Tạo một event giao dịch ngẫu nhiên dựa trên dữ liệu tham chiếu, sẵn sàng gửi Kafka."""
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
        # Mốc thời gian mili-giây dùng để tính end-to-end latency ở khâu updater.
        "producer_ts_ms": int(time.time() * 1000),
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


def create_producer(bootstrap_servers, acks="all"):
    """Khởi tạo Producer gom message theo lô (linger + batch lớn + nén) thay vì flush từng cái.

    Bật idempotence khi acks=all để bảo đảm không trùng/không mất message phía producer.
    """
    if Producer is None:
        raise RuntimeError("Cần cài confluent-kafka để dùng --sink kafka")

    return Producer({
        "bootstrap.servers": bootstrap_servers,
        "acks": acks,
        "enable.idempotence": acks == "all",
        "linger.ms": 20,
        "batch.size": 1 << 20,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 1_000_000,
        "queue.buffering.max.kbytes": 1_048_576,
    })


def write_stdout(events):
    for event in events:
        print(json.dumps(event, ensure_ascii=False), flush=True)


def write_raw_csv(events):
    append_csv(raw_data_dir / "raw_transactions.csv", events)


def produce(count, interval_seconds, sink, bootstrap_servers, topic, seed, acks="all"):
    """Sinh `count` event giao dịch và đẩy ra sink chỉ định (stdout, Kafka, raw CSV hoặc tất cả).

    Producer Kafka chỉ tạo một lần và flush ở cuối; định kỳ log tiến độ + throughput.
    """
    random.seed(seed)
    accounts, customers_by_id, merchants = load_reference_data()
    if not accounts:
        raise RuntimeError("Không tìm thấy account. Hãy chạy kafka.generate trước.")

    producer = create_producer(bootstrap_servers, acks=acks) if sink in ["kafka", "all"] else None
    start = time.perf_counter()
    last_log = start
    sent = 0

    for sequence in range(1, count + 1):
        event = build_transaction_event(sequence, accounts, customers_by_id, merchants)
        if sink in ["stdout", "all"]:
            write_stdout([event])
        if sink in ["csv", "all"]:
            write_raw_csv([event])
        if producer is not None:
            payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
            key = event["transaction_id"].encode("utf-8")
            # Hàng đợi đầy thì poll để giải phóng rồi thử lại, tránh mất message.
            while True:
                try:
                    producer.produce(topic, key=key, value=payload)
                    break
                except BufferError:
                    producer.poll(0.5)
            producer.poll(0)
        sent += 1

        now = time.perf_counter()
        if now - last_log >= 5:
            rate = sent / (now - start) if now > start else 0
            log_info("producer_progress", sent=sent, elapsed_s=round(now - start, 2),
                     throughput_msg_s=round(rate))
            last_log = now
        if interval_seconds and sequence < count:
            time.sleep(interval_seconds)

    if producer is not None:
        producer.flush()
    elapsed = time.perf_counter() - start
    log_info("producer_done", sent=sent, elapsed_s=round(elapsed, 3),
             throughput_msg_s=round(sent / elapsed) if elapsed > 0 else 0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--interval-seconds", type=float, default=0.2)
    parser.add_argument("--sink", choices=["stdout", "kafka", "csv", "all"], default="stdout")
    parser.add_argument("--bootstrap-servers", default=kafka_bootstrap_servers)
    parser.add_argument("--topic", default=kafka_topics["raw_transactions"])
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--acks", default="all", help="acks Kafka producer (all|1|0)")
    args = parser.parse_args()
    try:
        produce(args.count, args.interval_seconds, args.sink, args.bootstrap_servers, args.topic,
                args.seed, acks=args.acks)
    except Exception as exc:
        print(f"producer lỗi: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
