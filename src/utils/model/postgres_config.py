"""DTO cấu hình PostgreSQL."""

import os
from dataclasses import dataclass


@dataclass
class PostgresConfig:
    host: str = "postgres"
    port: str = "5432"
    dbname: str = "banking"
    user: str = "banking"
    password: str = "banking"

    @classmethod
    def from_dict(cls, d):
        """Dựng từ dict (YAML/ENV); thiếu config thì dùng default, không KeyError."""
        if not d:
            return cls()
        return cls(
            host=d.get("host", "postgres"),
            port=str(d.get("port", "5432")),
            dbname=d.get("dbname", "banking"),
            user=d.get("user", "banking"),
            password=d.get("password", "banking"),
        )

    @classmethod
    def from_env(cls):
        """Đọc cấu hình từ biến môi trường (cơ chế cấu hình hiện hành của project)."""
        env = {
            "host": os.getenv("POSTGRES_HOST"),
            "port": os.getenv("POSTGRES_PORT"),
            "dbname": os.getenv("POSTGRES_DB"),
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD"),
        }
        # Bỏ key thiếu để default trong from_dict có hiệu lực (None != "không set").
        return cls.from_dict({k: v for k, v in env.items() if v is not None})

    def to_psycopg_kwargs(self):
        """Trả kwargs cho psycopg.connect(**kwargs)."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
        }
