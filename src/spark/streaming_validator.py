import argparse
import json
import time

from application.prepare_clean_data import clean_transaction_fields, error_fields
from base.base_job import BaseJob
from config.settings import kafka_bootstrap_servers, kafka_topics, raw_data_dir, source_db_dir
from connector.benchmark_sink import record_benchmark
from connector.lakehouse_sink import write_clean_transactions, write_error_transactions, write_raw_transactions
from transform.validators import validate_accounts, validate_customers, validate_transactions
from utils.exception_handler import handle_fatal_error
from utils.io import read_csv
from utils.logging import log_info
from utils.optional_dependency import load_optional
from utils.string_constants import StringConstants as SC

_pyspark_sql = load_optional("pyspark.sql")
_pyspark_functions = load_optional("pyspark.sql.functions")
SparkSession = getattr(_pyspark_sql, "SparkSession", None)
col = getattr(_pyspark_functions, "col", None)
struct = getattr(_pyspark_functions, "struct", None)
to_json = getattr(_pyspark_functions, "to_json", None)


def load_reference_ids():
    """Kiểm tra snapshot reference và trả về account/customer IDs để kiểm tra giao dịch."""
    customers = read_csv(source_db_dir / "customers.csv") if (source_db_dir / "customers.csv").exists() else []
    customers_valid, _ = validate_customers(customers)
    customer_ids = {row["customer_id"] for row in customers_valid}

    accounts = read_csv(source_db_dir / "accounts.csv") if (source_db_dir / "accounts.csv").exists() else []
    accounts_valid, _ = validate_accounts(accounts, customer_ids)
    account_ids = {row["account_id"] for row in accounts_valid}
    return account_ids, customer_ids


def parse_json_rows(values):
    rows = []
    for value in values:
        if not value:
            continue
        row = json.loads(value) if isinstance(value, str) else value
        rows.append(row)
    return rows


def validate_and_route(rows, append=True, reference=None):
    """Kiểm tra dòng giao dịch và ghi output clean/error vào lakehouse.

    `reference` là tuple (account_ids, customer_ids) đã nạp sẵn để tránh đọc lại
    CSV mỗi micro-batch; nếu None thì nạp ngay (dùng cho luồng batch dự phòng).
    """
    account_ids, customer_ids = reference if reference is not None else load_reference_ids()
    valid, invalid = validate_transactions(rows, account_ids, customer_ids)
    write_clean_transactions(valid, clean_transaction_fields(), append=append)
    write_error_transactions(invalid, error_fields(), append=append)
    return valid, invalid


def run_batch():
    """Luồng batch dự phòng để kiểm tra local khi chưa chạy Kafka/Spark cluster."""
    path = raw_data_dir / "raw_transactions.csv"
    rows = read_csv(path) if path.exists() else []
    return validate_and_route(rows, append=False)


def publish_topic(spark, rows, bootstrap_servers, topic):
    """Đẩy row từ micro-batch ngược lại vào Kafka."""
    if not rows:
        return

    df = spark.createDataFrame(rows)
    key_column = "transaction_id" if "transaction_id" in df.columns else "error_id"
    (
        df.select(
            col(key_column).cast("string").alias("key"),
            to_json(struct("*")).alias("value"),
        )
        .write.format(SC.KAFKA_FORMAT)
        .option(SC.OPT_KAFKA_BOOTSTRAP_SERVERS, bootstrap_servers)
        .option(SC.OPT_TOPIC, topic)
        .save()
    )


class StreamingValidatorJob(BaseJob):
    """Chạy Spark Structured Streaming từ raw Kafka event sang topic/CSV clean và error."""

    def __init__(self, bootstrap_servers, raw_topic, clean_topic, error_topic, checkpoint_dir):
        super().__init__(SC.SPARK_APP_NAME)
        self.bootstrap_servers = bootstrap_servers
        self.raw_topic = raw_topic
        self.clean_topic = clean_topic
        self.error_topic = error_topic
        self.checkpoint_dir = checkpoint_dir
        self.spark = None
        self.reference = None
        self.query = None

    def setup(self):
        if SparkSession is None:
            raise RuntimeError("Cần cài pyspark để chạy streaming validator")
        self.spark = (
            SparkSession.builder.appName(SC.SPARK_APP_NAME)
            .master(SC.SPARK_MASTER)
            # Cụm demo chỉ 1 core: hạ shuffle partitions để giảm overhead lập lịch task.
            .config(SC.CONF_SHUFFLE_PARTITIONS, "8")
            .getOrCreate()
        )
        # Nạp reference (account/customer hợp lệ) một lần khi khởi động stream thay vì
        # đọc lại CSV ở mỗi micro-batch — giảm mạnh thời gian xử lý từng batch.
        self.reference = load_reference_ids()
        log_info("reference_loaded", account_ids=len(self.reference[0]), customer_ids=len(self.reference[1]))

    def execute(self):
        stream = (
            self.spark.readStream.format(SC.KAFKA_FORMAT)
            .option(SC.OPT_KAFKA_BOOTSTRAP_SERVERS, self.bootstrap_servers)
            .option(SC.OPT_SUBSCRIBE, self.raw_topic)
            .option(SC.OPT_STARTING_OFFSETS, SC.OFFSET_LATEST)
            .load()
        )
        values = stream.select(col("value").cast("string").alias("value"))
        self.query = (
            values.writeStream.foreachBatch(self._handle_batch)
            .option(SC.OPT_CHECKPOINT_LOCATION, self.checkpoint_dir)
            .start()
        )
        self.query.awaitTermination()

    def _handle_batch(self, batch_df, batch_id):
        """Xử lý một micro-batch: validate, ghi lakehouse, republish Kafka và log throughput."""
        t0 = time.perf_counter()
        raw_values = [row["value"] for row in batch_df.collect()]
        rows = parse_json_rows(raw_values)
        if not rows:
            return
        write_raw_transactions(rows, append=True)
        valid, invalid = validate_and_route(rows, append=True, reference=self.reference)
        publish_topic(self.spark, valid, self.bootstrap_servers, self.clean_topic)
        publish_topic(self.spark, invalid, self.bootstrap_servers, self.error_topic)
        duration = time.perf_counter() - t0
        duration_ms = round(duration * 1000, 1)
        throughput = round(len(rows) / duration) if duration > 0 else 0
        # Log thời gian + throughput của từng micro-batch để theo dõi SLA streaming.
        log_info(
            "spark_microbatch",
            batch_id=batch_id,
            rows=len(rows),
            valid=len(valid),
            invalid=len(invalid),
            duration_ms=duration_ms,
            throughput_events_s=throughput,
        )
        record_benchmark(
            "spark_validator",
            batch_id=batch_id,
            rows=len(rows),
            valid=len(valid),
            invalid=len(invalid),
            duration_ms=duration_ms,
            throughput_events_s=throughput,
        )

    def teardown(self):
        if self.spark is not None:
            self.spark.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["batch", "streaming"], default="batch")
    parser.add_argument("--bootstrap-servers", default=kafka_bootstrap_servers)
    parser.add_argument("--raw-topic", default=kafka_topics["raw_transactions"])
    parser.add_argument("--clean-topic", default=kafka_topics["clean_transactions"])
    parser.add_argument("--error-topic", default=kafka_topics["error_transactions"])
    parser.add_argument("--checkpoint-dir", default="/workspace/lakehouse/audit/spark-validator-checkpoint")
    args = parser.parse_args()
    try:
        if args.mode == "batch":
            run_batch()
        else:
            StreamingValidatorJob(
                args.bootstrap_servers, args.raw_topic, args.clean_topic, args.error_topic, args.checkpoint_dir
            ).run()
    except SystemExit:
        raise
    except Exception as exc:
        handle_fatal_error("streaming validator failed", exc)


if __name__ == "__main__":
    main()
