import argparse
import json
import random
import time
from datetime import datetime

from base.base_job import BaseJob
from config.settings import kafka_bootstrap_servers, kafka_topics, raw_data_dir, source_db_dir
from connector.benchmark_sink import record_benchmark
from connector.kafka_connector import create_producer
from kafka.generate import amount_for_segment, channels, source_for_channel, transaction_types
from utils.exception_handler import handle_fatal_error
from utils.io import append_csv, read_csv
from utils.logging import log_info
from utils.model.kafka_config import KafkaConfig
from utils.string_constants import StringConstants as SC


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


def write_stdout(events):
    for event in events:
        print(json.dumps(event, ensure_ascii=False), flush=True)


def write_raw_csv(events):
    append_csv(raw_data_dir / "raw_transactions.csv", events)


class TransactionProducerJob(BaseJob):
    """Sinh `count` event giao dịch và đẩy ra sink chỉ định (stdout, Kafka, raw CSV hoặc tất cả).

    Producer Kafka chỉ tạo một lần ở setup và flush ở teardown; định kỳ log tiến độ + throughput.
    """

    def __init__(self, count, interval_seconds, sink, bootstrap_servers, topic, seed, acks=SC.ACKS_ALL):
        super().__init__("transaction-producer")
        self.count = count
        self.interval_seconds = interval_seconds
        self.sink = sink
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.seed = seed
        self.acks = acks
        self.producer = None
        self.start = None
        self.sent = 0

    def setup(self):
        random.seed(self.seed)
        self.accounts, self.customers_by_id, self.merchants = load_reference_data()
        if not self.accounts:
            raise RuntimeError("Không tìm thấy account. Hãy chạy kafka.generate trước.")
        if self.sink in ["kafka", "all"]:
            self.producer = create_producer(KafkaConfig(self.bootstrap_servers, self.acks))

    def execute(self):
        self.start = time.perf_counter()
        last_log = self.start
        for sequence in range(1, self.count + 1):
            event = build_transaction_event(sequence, self.accounts, self.customers_by_id, self.merchants)
            if self.sink in ["stdout", "all"]:
                write_stdout([event])
            if self.sink in ["csv", "all"]:
                write_raw_csv([event])
            if self.producer is not None:
                self._send_event(event)
            self.sent += 1

            now = time.perf_counter()
            if now - last_log >= 5:
                rate = self.sent / (now - self.start) if now > self.start else 0
                log_info("producer_progress", sent=self.sent, elapsed_s=round(now - self.start, 2),
                         throughput_msg_s=round(rate))
                last_log = now
            if self.interval_seconds and sequence < self.count:
                time.sleep(self.interval_seconds)

    def _send_event(self, event):
        payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
        key = event["transaction_id"].encode("utf-8")
        # Hàng đợi đầy thì poll để giải phóng rồi thử lại, tránh mất message.
        while True:
            try:
                self.producer.produce(self.topic, key=key, value=payload)
                break
            except BufferError:
                self.producer.poll(0.5)
        self.producer.poll(0)

    def teardown(self):
        if self.producer is not None:
            self.producer.flush()
        if self.start is not None:
            elapsed = time.perf_counter() - self.start
            throughput = round(self.sent / elapsed) if elapsed > 0 else 0
            log_info("producer_done", sent=self.sent, elapsed_s=round(elapsed, 3), throughput_msg_s=throughput)
            record_benchmark("kafka_producer", sent=self.sent, elapsed_s=round(elapsed, 3),
                             throughput_msg_s=throughput)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--interval-seconds", type=float, default=0.2)
    parser.add_argument("--sink", choices=["stdout", "kafka", "csv", "all"], default="stdout")
    parser.add_argument("--bootstrap-servers", default=kafka_bootstrap_servers)
    parser.add_argument("--topic", default=kafka_topics["raw_transactions"])
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--acks", default=SC.ACKS_ALL, help="acks Kafka producer (all|1|0)")
    args = parser.parse_args()
    try:
        TransactionProducerJob(args.count, args.interval_seconds, args.sink, args.bootstrap_servers,
                               args.topic, args.seed, acks=args.acks).run()
    except SystemExit:
        raise
    except Exception as exc:
        handle_fatal_error("transaction producer failed", exc)


if __name__ == "__main__":
    main()
