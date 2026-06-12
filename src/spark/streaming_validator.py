import argparse
import json
from importlib import import_module, util

from application.prepare_clean_data import clean_transaction_fields, error_fields
from config.settings import kafka_bootstrap_servers, kafka_topics, raw_data_dir, source_db_dir
from spark.lakehouse_sink import write_clean_transactions, write_error_transactions, write_raw_transactions
from spark.validators import validate_accounts, validate_customers, validate_transactions
from utils.io import read_csv

_pyspark_sql = import_module("pyspark.sql") if util.find_spec("pyspark.sql") else None
_pyspark_functions = import_module("pyspark.sql.functions") if util.find_spec("pyspark.sql.functions") else None
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


def validate_and_route(rows, append=True):
    """Kiểm tra dòng giao dịch và ghi output clean/error vào lakehouse."""
    account_ids, customer_ids = load_reference_ids()
    valid, invalid = validate_transactions(rows, account_ids, customer_ids)
    write_clean_transactions(valid, clean_transaction_fields(), append=append)
    write_error_transactions(invalid, error_fields(), append=append)
    return valid, invalid


def run_batch():
    """Luồng batch dự phòng để kiểm tra local khi chưa chạy Kafka/Spark cluster."""
    path = raw_data_dir / "raw_transactions.csv"
    rows = read_csv(path) if path.exists() else []
    return validate_and_route(rows, append=False)


def run_streaming(bootstrap_servers, raw_topic, clean_topic, error_topic, checkpoint_dir):
    """Chạy Spark Structured Streaming từ raw Kafka event sang topic/CSV clean và error."""
    if SparkSession is None:
        raise RuntimeError("Cần cài pyspark để chạy streaming validator")

    spark = (
        SparkSession.builder.appName("banking-spark-streaming-validator")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", raw_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    values = stream.select(col("value").cast("string").alias("value"))

    def handle_batch(batch_df, batch_id):
        raw_values = [row["value"] for row in batch_df.collect()]
        rows = parse_json_rows(raw_values)
        if not rows:
            return
        write_raw_transactions(rows, append=True)
        valid, invalid = validate_and_route(rows, append=True)
        publish_topic(spark, valid, bootstrap_servers, clean_topic)
        publish_topic(spark, invalid, bootstrap_servers, error_topic)

    query = (
        values.writeStream.foreachBatch(handle_batch)
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )
    query.awaitTermination()


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
        .write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", topic)
        .save()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["batch", "streaming"], default="batch")
    parser.add_argument("--bootstrap-servers", default=kafka_bootstrap_servers)
    parser.add_argument("--raw-topic", default=kafka_topics["raw_transactions"])
    parser.add_argument("--clean-topic", default=kafka_topics["clean_transactions"])
    parser.add_argument("--error-topic", default=kafka_topics["error_transactions"])
    parser.add_argument("--checkpoint-dir", default="/workspace/lakehouse/audit/spark-validator-checkpoint")
    args = parser.parse_args()
    if args.mode == "batch":
        run_batch()
    else:
        run_streaming(args.bootstrap_servers, args.raw_topic, args.clean_topic, args.error_topic, args.checkpoint_dir)


if __name__ == "__main__":
    main()
