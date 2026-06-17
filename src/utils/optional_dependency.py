"""Nạp lười các dependency nặng (pyspark/confluent_kafka/psycopg).

Giữ đúng quy ước project: module luôn import được kể cả khi thiếu dep, chỉ ném lỗi
rõ ràng khi nhánh đó thực sự được chạy.
"""

from importlib import import_module, util


def load_optional(module_name):
    """Trả về module nếu cài được, ngược lại None (không ném lỗi khi thiếu dep).

    Chỉ kiểm tra package gốc bằng find_spec để tránh ModuleNotFoundError khi dò
    submodule dạng `pyspark.sql` lúc `pyspark` chưa được cài.
    """
    root = module_name.split(".")[0]
    try:
        if util.find_spec(root) is None:
            return None
    except (ImportError, ValueError):
        return None
    return import_module(module_name)
