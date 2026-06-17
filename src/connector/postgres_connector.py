"""Adapter ra PostgreSQL: dựng cấu hình typed và tạo kết nối psycopg."""

from utils.model.postgres_config import PostgresConfig
from utils.optional_dependency import load_optional

psycopg = load_optional("psycopg")


def postgres_settings():
    """Cấu hình Postgres dạng DTO typed, đọc từ biến môi trường."""
    return PostgresConfig.from_env()


def postgres_config():
    """Kwargs cho psycopg.connect(**kwargs) — giữ API cũ cho code gọi sẵn."""
    return postgres_settings().to_psycopg_kwargs()


def connect():
    """Tạo kết nối PostgreSQL bằng psycopg khi service được bật."""
    if psycopg is None:
        raise RuntimeError("Cần cài psycopg[binary] để kết nối PostgreSQL")
    return psycopg.connect(**postgres_config())
