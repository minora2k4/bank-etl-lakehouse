import os


def postgres_config():
    """Trả về cấu hình Postgres từ biến môi trường."""
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "database": os.getenv("POSTGRES_DB", "banking"),
        "user": os.getenv("POSTGRES_USER", "banking"),
    }