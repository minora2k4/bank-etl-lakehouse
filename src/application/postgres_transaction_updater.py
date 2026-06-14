import argparse
import json
import time
from importlib import import_module, util

from config.settings import clean_data_dir, kafka_bootstrap_servers, kafka_topics
from connector.postgres import postgres_config
from utils.io import read_csv
from utils.logging import log_info

psycopg = import_module("psycopg") if util.find_spec("psycopg") else None
_confluent_kafka = import_module("confluent_kafka") if util.find_spec("confluent_kafka") else None
Consumer = getattr(_confluent_kafka, "Consumer", None)


def connect():
    """Tạo kết nối PostgreSQL bằng psycopg khi service được bật."""
    if psycopg is None:
        raise RuntimeError("Cần cài psycopg[binary] để chạy postgres_transaction_updater")
    return psycopg.connect(**postgres_config())


def amount_delta(row):
    """Ánh xạ transaction type sang delta số dư tài khoản."""
    amount = int(float(row.get("amount_vnd") or 0))
    if row.get("transaction_type") == "DEPOSIT":
        return amount
    return -amount


def post_transaction(conn, row, kafka_meta=None, verbose=True):
    """Ghi nhận một giao dịch clean với idempotency, row lock và ledger bất biến."""
    kafka_meta = kafka_meta or {}
    transaction_id = row["transaction_id"]
    account_id = row["account_id"]
    delta = amount_delta(row)

    with conn.transaction():
        inserted = conn.execute(
            """
            INSERT INTO processed_transactions(transaction_id, kafka_topic, kafka_partition, kafka_offset, status)
            VALUES (%s, %s, %s, %s, 'PROCESSING')
            ON CONFLICT (transaction_id) DO NOTHING
            RETURNING transaction_id
            """,
            (
                transaction_id,
                kafka_meta.get("topic"),
                kafka_meta.get("partition"),
                kafka_meta.get("offset"),
            ),
        ).fetchone()
        if not inserted:
            return "DUPLICATE_SKIPPED"

        account = conn.execute(
            "SELECT balance_vnd FROM accounts WHERE account_id = %s FOR UPDATE",
            (account_id,),
        ).fetchone()
        if account is None:
            mark_rejected(conn, transaction_id, "ACCOUNT_NOT_FOUND", verbose=verbose)
            return "REJECTED"

        current_balance = int(account[0] or 0)
        next_balance = current_balance + delta
        if next_balance < 0:
            mark_rejected(conn, transaction_id, "INSUFFICIENT_FUNDS", verbose=verbose)
            return "REJECTED"

        conn.execute(
            """
            INSERT INTO transactions(transaction_id, account_id, customer_id, amount_vnd, transaction_type, transaction_time, status)
            VALUES (%s, %s, %s, %s, %s, %s, 'POSTED')
            ON CONFLICT (transaction_id) DO NOTHING
            """,
            (
                transaction_id,
                account_id,
                row.get("customer_id"),
                row.get("amount_vnd"),
                row.get("transaction_type"),
                row.get("transaction_time"),
            ),
        )
        conn.execute(
            """
            UPDATE accounts
            SET balance_vnd = %s, updated_at = now()
            WHERE account_id = %s
            """,
            (next_balance, account_id),
        )
        conn.execute(
            """
            INSERT INTO ledger_entries(transaction_id, account_id, debit_vnd, credit_vnd, balance_after_vnd, entry_type)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                transaction_id,
                account_id,
                abs(delta) if delta < 0 else 0,
                delta if delta > 0 else 0,
                next_balance,
                row.get("transaction_type"),
            ),
        )
        conn.execute(
            """
            UPDATE processed_transactions
            SET status = 'POSTED', posted_at = now()
            WHERE transaction_id = %s
            """,
            (transaction_id,),
        )
    if verbose:
        log_info("transaction_posted", transaction_id=transaction_id, account_id=account_id, delta=delta)
    return "POSTED"


def mark_rejected(conn, transaction_id, message, verbose=True):
    conn.execute(
        """
        UPDATE processed_transactions
        SET status = 'REJECTED', error_message = %s, posted_at = now()
        WHERE transaction_id = %s
        """,
        (message, transaction_id),
    )
    if verbose:
        log_info("transaction_rejected", transaction_id=transaction_id, reason=message)


def load_csv_rows():
    path = clean_data_dir / "clean_transactions.csv"
    return read_csv(path) if path.exists() else []


def percentile(values, pct):
    """Tính percentile đơn giản (nearest-rank) trên list latency."""
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(0, min(len(ordered) - 1, int(round(pct / 100.0 * len(ordered) + 0.5)) - 1))
    return ordered[rank]


def consume_kafka(bootstrap_servers, topic, log_interval_seconds=5.0, batch_size=500, batch_timeout=0.5):
    """Consume clean-transactions theo micro-batch, ghi PostgreSQL và log throughput/latency.

    Mỗi lô ghi trong một transaction DB rồi mới commit offset (at-least-once + idempotency).
    """
    if Consumer is None:
        raise RuntimeError("Cần cài confluent-kafka để dùng --source kafka")

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "postgres-transaction-updater",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        # Lấy nhiều bản ghi mỗi lần fetch để giảm round-trip tới broker.
        "fetch.min.bytes": 1 << 16,
        "fetch.wait.max.ms": 50,
        "max.partition.fetch.bytes": 8 << 20,
    })
    consumer.subscribe([topic])

    start = time.perf_counter()
    last_log = start
    total = 0
    window_count = 0
    latencies_ms = []
    with connect() as conn:
        while True:
            messages = consumer.consume(num_messages=batch_size, timeout=batch_timeout)
            now = time.perf_counter()
            if messages:
                batch_latencies = process_batch_with_retry(conn, messages)
                latencies_ms.extend(batch_latencies)
                # Offset chỉ commit SAU khi cả lô đã ghi DB thành công (đúng thứ tự
                # at-least-once). Async để không chặn vòng lặp; idempotency chống trùng.
                consumer.commit(asynchronous=True)
                total += len(messages)
                window_count += len(messages)

            # Định kỳ log throughput + end-to-end latency p50/p95/p99 để theo dõi SLA.
            if now - last_log >= log_interval_seconds and window_count:
                window = now - last_log
                log_info(
                    "updater_throughput",
                    posted_total=total,
                    window_msg=window_count,
                    batch_size=batch_size,
                    throughput_msg_s=round(window_count / window) if window > 0 else 0,
                    e2e_p50_ms=round(percentile(latencies_ms, 50), 1),
                    e2e_p95_ms=round(percentile(latencies_ms, 95), 1),
                    e2e_p99_ms=round(percentile(latencies_ms, 99), 1),
                )
                last_log = now
                window_count = 0
                latencies_ms = []


def process_batch_with_retry(conn, messages, max_retries=5):
    """Xử lý lô với cơ chế retry khi gặp deadlock.

    Dù đã sắp theo account_id để tránh deadlock, vẫn giữ retry như lớp phòng vệ: khi
    nhiều consumer chạy song song, transaction bị Postgres hủy do deadlock sẽ được rollback
    rồi thử lại. An toàn vì offset Kafka chưa commit và idempotency chống ghi trùng.
    """
    deadlock_errors = (psycopg.errors.DeadlockDetected, psycopg.errors.SerializationFailure)
    for attempt in range(max_retries):
        try:
            return process_batch(conn, messages)
        except deadlock_errors:
            if attempt == max_retries - 1:
                raise
            log_info("batch_retry", attempt=attempt + 1, size=len(messages))
            time.sleep(0.05 * (attempt + 1))


def process_batch(conn, messages):
    """Ghi cả lô message trong MỘT transaction DB: 1 lần COMMIT/fsync cho cả lô thay vì
    mỗi bản ghi một lần — đòn bẩy throughput lớn nhất trên 1 node Postgres. Mỗi bản ghi
    vẫn nằm trong savepoint riêng (idempotency + row lock + ledger giữ nguyên), và trong
    cùng transaction số dư cộng dồn đúng vì Postgres đọc được write của chính nó.

    Các bản ghi được sắp theo account_id (sort ổn định nên vẫn giữ thứ tự offset trong
    cùng account) để mọi consumer khóa account theo CÙNG thứ tự — triệt tiêu deadlock."""
    now_ms = time.time() * 1000
    parsed = []
    for message in messages:
        if message.error():
            raise RuntimeError(str(message.error()))
        row = json.loads(message.value().decode("utf-8"))
        meta = {"topic": message.topic(), "partition": message.partition(), "offset": message.offset()}
        parsed.append((row, meta))
    parsed.sort(key=lambda item: item[0].get("account_id") or "")

    latencies = []
    with conn.transaction():
        for row, meta in parsed:
            producer_ts_ms = row.get("producer_ts_ms")
            if producer_ts_ms:
                latencies.append(now_ms - float(producer_ts_ms))
            post_transaction(conn, row, meta, verbose=False)
    return latencies


def post_csv():
    with connect() as conn:
        results = {}
        for row in load_csv_rows():
            status = post_transaction(conn, row)
            results[status] = results.get(status, 0) + 1
    log_info("post_csv_done", **results)
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["csv", "kafka"], default="csv")
    parser.add_argument("--bootstrap-servers", default=kafka_bootstrap_servers)
    parser.add_argument("--topic", default=kafka_topics["clean_transactions"])
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Số bản ghi gộp vào một transaction DB mỗi lô")
    args = parser.parse_args()
    if args.source == "kafka":
        consume_kafka(args.bootstrap_servers, args.topic, batch_size=args.batch_size)
    else:
        print(json.dumps(post_csv(), sort_keys=True))


if __name__ == "__main__":
    main()
