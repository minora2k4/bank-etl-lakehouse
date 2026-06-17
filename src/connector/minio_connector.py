"""Adapter ra MinIO / object store: dựng cấu hình typed và upload file."""

from pathlib import Path

from utils.model.minio_config import MinioConfig
from utils.optional_dependency import load_optional

_minio = load_optional("minio")


def minio_settings():
    """Cấu hình MinIO dạng DTO typed, đọc từ biến môi trường."""
    return MinioConfig.from_env()


def minio_config():
    """Trả cấu hình MinIO dạng dict — giữ API cũ cho code gọi sẵn."""
    cfg = minio_settings()
    return {
        "endpoint": cfg.endpoint,
        "bucket": cfg.bucket,
        "access_key": cfg.access_key,
        "secret_key": cfg.secret_key,
    }


def get_client():
    """Tạo MinIO client từ cấu hình môi trường. Cần cài package minio."""
    if _minio is None:
        raise RuntimeError("Cần cài package minio để dùng MinIO client Python")
    cfg = minio_settings()
    return _minio.Minio(cfg.host, access_key=cfg.access_key, secret_key=cfg.secret_key, secure=cfg.secure)


def upload_file(local_path, object_name):
    """Upload một file lên bucket MinIO đã cấu hình."""
    client = get_client()
    bucket = minio_settings().bucket
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    client.fput_object(bucket, object_name, str(Path(local_path)))
