import os


def minio_config():
    """Trả về cấu hình MinIO từ biến môi trường."""
    return {
        "endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        "bucket": os.getenv("MINIO_BUCKET", "banking-lakehouse"),
        "access_key": os.getenv("MINIO_ROOT_USER", "minioadmin"),
    }