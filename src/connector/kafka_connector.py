"""Adapter ra Kafka: tạo Producer/Consumer với cấu hình hiệu năng đã tinh chỉnh."""

from utils.optional_dependency import load_optional
from utils.string_constants import StringConstants as SC

_confluent_kafka = load_optional("confluent_kafka")
Producer = getattr(_confluent_kafka, "Producer", None)
Consumer = getattr(_confluent_kafka, "Consumer", None)


def create_producer(config):
    """Khởi tạo Producer gom message theo lô (linger + batch lớn + nén) thay vì flush từng cái.

    Bật idempotence khi acks=all để bảo đảm không trùng/không mất message phía producer.
    """
    if Producer is None:
        raise RuntimeError("Cần cài confluent-kafka để dùng sink kafka")
    return Producer({
        SC.BOOTSTRAP_SERVERS: config.bootstrap_servers,
        SC.ACKS: config.acks,
        SC.ENABLE_IDEMPOTENCE: config.idempotence,
        SC.LINGER_MS: 20,
        SC.BATCH_SIZE: 1 << 20,
        SC.COMPRESSION_TYPE: SC.COMPRESSION_LZ4,
        SC.QUEUE_BUFFERING_MAX_MESSAGES: 1_000_000,
        SC.QUEUE_BUFFERING_MAX_KBYTES: 1_048_576,
    })


def create_consumer(bootstrap_servers, group_id=SC.UPDATER_GROUP_ID):
    """Khởi tạo Consumer lấy nhiều bản ghi mỗi lần fetch để giảm round-trip tới broker."""
    if Consumer is None:
        raise RuntimeError("Cần cài confluent-kafka để dùng source kafka")
    return Consumer({
        SC.BOOTSTRAP_SERVERS: bootstrap_servers,
        SC.GROUP_ID: group_id,
        SC.ENABLE_AUTO_COMMIT: False,
        SC.AUTO_OFFSET_RESET: SC.OFFSET_EARLIEST,
        SC.FETCH_MIN_BYTES: 1 << 16,
        SC.FETCH_WAIT_MAX_MS: 50,
        SC.MAX_PARTITION_FETCH_BYTES: 8 << 20,
    })
