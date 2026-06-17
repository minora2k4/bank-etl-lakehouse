"""DTO cấu hình MinIO / object store."""

import os
from dataclasses import dataclass


@dataclass
class MinioConfig:
    endpoint: str = "http://minio:9000"
    bucket: str = "banking-lakehouse"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"

    @classmethod
    def from_dict(cls, d):
        """Dựng từ dict (YAML/ENV); thiếu config thì dùng default, không KeyError."""
        if not d:
            return cls()
        return cls(
            endpoint=d.get("endpoint", "http://minio:9000"),
            bucket=d.get("bucket", "banking-lakehouse"),
            access_key=d.get("access_key", "minioadmin"),
            secret_key=d.get("secret_key", "minioadmin"),
        )

    @classmethod
    def from_env(cls):
        """Đọc cấu hình từ biến môi trường (cơ chế cấu hình hiện hành của project)."""
        env = {
            "endpoint": os.getenv("MINIO_ENDPOINT"),
            "bucket": os.getenv("MINIO_BUCKET"),
            "access_key": os.getenv("MINIO_ROOT_USER"),
            "secret_key": os.getenv("MINIO_ROOT_PASSWORD"),
        }
        # Bỏ key thiếu để default trong from_dict có hiệu lực (None != "không set").
        return cls.from_dict({k: v for k, v in env.items() if v is not None})

    @property
    def host(self):
        """Endpoint không kèm scheme, đúng dạng MinIO client cần."""
        return self.endpoint.replace("http://", "").replace("https://", "")

    @property
    def secure(self):
        """True nếu endpoint dùng HTTPS."""
        return self.endpoint.startswith("https://")
