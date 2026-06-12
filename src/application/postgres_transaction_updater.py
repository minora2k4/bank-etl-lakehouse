import argparse
import json
import os
from importlib import import_module, util

from config.settings import clean_data_dir, kafka_bootstrap_servers, kafka_topics
from utils.io import read_csv

psycopg = import_module("psycopg") if util.find_spec("psycopg") else None
_confluent_kafka = import_module("confluent_kafka") if util.find_spec("confluent_kafka") else None
Consumer = getattr(_confluent_kafka, "Consumer", None)


def connect():
    """Tạo kết nối PostgreSQL bằng psycopg khi service được bật."""
    if psycopg is None:
        raise RuntimeError("Cần cài psycopg[binary] để chạy postgres_transaction_updater")

    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "banking"),
        user=os.getenv("POSTGRES_USER", "banking"),
        password=os.getenv("POSTGRES_PASSWORD", "banking"),
    )


def amount_delta(row):
    """Ánh xạ transaction type sang delta số dư tài khoản."""
    amount = int(float(row.get("amount_vnd") or 0))
    if row.get("transaction_type") == "DEPOSIT":
        return amount
    return -amount


def post_transaction(conn, row, kafka_meta=None):
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
            mark_rejected(conn, transaction_id, "ACCOUNT_NOT_FOUND")
            return "REJECTED"

        current_balance = int(account[0] or 0)
        next_balance = current_balance + delta
        if next_balance < 0:
            mark_rejected(conn, transaction_id, "INSUFFICIENT_FUNDS")
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
    return "POSTED"


def mark_rejected(conn, transaction_id, message):
    conn.execute(
        """
        UPDATE processed_transactions
        SET status = 'REJECTED', error_message = %s, posted_at = now()
        WHERE transaction_id = %s
        """,
        (message, transaction_id),
    )


def load_csv_rows():
    path = clean_data_dir / "clean_transactions.csv"
    return read_csv(path) if path.exists() else []


def consume_kafka(bootstrap_servers, topic):
    if Consumer is None:
        raise RuntimeError("Cần cài confluent-kafka để dùng --source kafka")

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "postgres-transaction-updater",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])
    with connect() as conn:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            post_transaction(
                conn,
                json.loads(message.value().decode("utf-8")),
                {
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                },
            )
            consumer.commit()


def post_csv():
    with connect() as conn:
        results = {}
        for row in load_csv_rows():
            status = post_transaction(conn, row)
            results[status] = results.get(status, 0) + 1
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["csv", "kafka"], default="csv")
    parser.add_argument("--bootstrap-servers", default=kafka_bootstrap_servers)
    parser.add_argument("--topic", default=kafka_topics["clean_transactions"])
    args = parser.parse_args()
    if args.source == "kafka":
        consume_kafka(args.bootstrap_servers, args.topic)
    else:
        print(json.dumps(post_csv(), sort_keys=True))


if __name__ == "__main__":
    main()
