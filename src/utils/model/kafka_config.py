"""DTO cấu hình Kafka (chỉ phần kết nối; tham số tuning hiệu năng nằm ở connector)."""

from dataclasses import dataclass

from utils.string_constants import StringConstants as SC


@dataclass
class KafkaConfig:
    bootstrap_servers: str = "localhost:9092"
    acks: str = SC.ACKS_ALL

    @classmethod
    def from_dict(cls, d):
        """Dựng từ dict; thiếu config thì dùng default, không KeyError."""
        if not d:
            return cls()
        return cls(
            bootstrap_servers=d.get("bootstrap_servers", "localhost:9092"),
            acks=str(d.get("acks", SC.ACKS_ALL)),
        )

    @property
    def idempotence(self):
        """Bật idempotence khi acks=all để không trùng/không mất message phía producer."""
        return self.acks == SC.ACKS_ALL
