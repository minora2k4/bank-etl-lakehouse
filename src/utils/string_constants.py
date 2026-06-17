"""Hằng số chuỗi tập trung (config key, format, status lifecycle).

Mục tiêu: triệt tiêu magic string trong logic. Dùng MỘT alias nhất quán toàn project:
    from utils.string_constants import StringConstants as SC
"""


class StringConstants:
    # --- Kafka producer config key ---
    BOOTSTRAP_SERVERS = "bootstrap.servers"
    ACKS = "acks"
    ENABLE_IDEMPOTENCE = "enable.idempotence"
    LINGER_MS = "linger.ms"
    BATCH_SIZE = "batch.size"
    COMPRESSION_TYPE = "compression.type"
    QUEUE_BUFFERING_MAX_MESSAGES = "queue.buffering.max.messages"
    QUEUE_BUFFERING_MAX_KBYTES = "queue.buffering.max.kbytes"

    # --- Kafka consumer config key ---
    GROUP_ID = "group.id"
    ENABLE_AUTO_COMMIT = "enable.auto.commit"
    AUTO_OFFSET_RESET = "auto.offset.reset"
    FETCH_MIN_BYTES = "fetch.min.bytes"
    FETCH_WAIT_MAX_MS = "fetch.wait.max.ms"
    MAX_PARTITION_FETCH_BYTES = "max.partition.fetch.bytes"

    # --- Kafka config value ---
    ACKS_ALL = "all"
    COMPRESSION_LZ4 = "lz4"
    OFFSET_EARLIEST = "earliest"
    OFFSET_LATEST = "latest"
    UPDATER_GROUP_ID = "postgres-transaction-updater"

    # --- Spark Structured Streaming option ---
    KAFKA_FORMAT = "kafka"
    OPT_KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers"
    OPT_SUBSCRIBE = "subscribe"
    OPT_STARTING_OFFSETS = "startingOffsets"
    OPT_TOPIC = "topic"
    OPT_CHECKPOINT_LOCATION = "checkpointLocation"
    CONF_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"
    SPARK_APP_NAME = "banking-spark-streaming-validator"
    SPARK_MASTER = "spark://spark-master:7077"

    # --- Vòng đời bản ghi trong processed_transactions ---
    STATUS_PROCESSING = "PROCESSING"
    STATUS_POSTED = "POSTED"
    STATUS_REJECTED = "REJECTED"

    # --- Lý do từ chối giao dịch ---
    REASON_ACCOUNT_NOT_FOUND = "ACCOUNT_NOT_FOUND"
    REASON_INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS"

    # --- Mã kết quả khi post một giao dịch clean ---
    RESULT_DUPLICATE_SKIPPED = "DUPLICATE_SKIPPED"
    RESULT_REJECTED = "REJECTED"
    RESULT_POSTED = "POSTED"

    # --- Loại giao dịch ảnh hưởng số dư ---
    TXN_TYPE_DEPOSIT = "DEPOSIT"
