import os
from importlib import import_module, util


def minio_config():
    """Trả về cấu hình MinIO từ biến môi trường."""
    return {
        "endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        "bucket": os.getenv("MINIO_BUCKET", "banking-lakehouse"),
        "access_key": os.getenv("MINIO_ROOT_USER", "minioadmin"),
        "secret_key": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    }


def get_client():
    """Tạo MinIO client từ cấu hình môi trường. Cần cài package minio."""
    _minio = import_module("minio") if util.find_spec("minio") else None
    if _minio is None:
        raise RuntimeError("Cần cài package minio để dùng MinIO client Python")
    cfg = minio_config()
    endpoint = cfg["endpoint"].replace("http://", "").replace("https://", "")
    secure = cfg["endpoint"].startswith("https://")
    return _minio.Minio(endpoint, access_key=cfg["access_key"], secret_key=cfg["secret_key"], secure=secure)


def upload_file(local_path, object_name):
    """Upload một file lên bucket MinIO đã cấu hình."""
    from pathlib import Path
    client = get_client()
    cfg = minio_config()
    bucket = cfg["bucket"]
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    client.fput_object(bucket, object_name, str(local_path))
