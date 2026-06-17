"""Khung xương Template Method cho mọi job: lifecycle bất biến, bước thay đổi mở qua override."""

import logging
from abc import ABC, abstractmethod

from utils.logging import LOGGER_NAME, setup_logging

logger = logging.getLogger(LOGGER_NAME)


class BaseJob(ABC):
    def __init__(self, app_name):
        self.app_name = app_name

    def run(self):
        """Khung BẤT BIẾN: setup → execute → teardown, bắt lỗi phân loại."""
        setup_logging()
        try:
            logger.info("event=job_start app=%s", self.app_name)
            self.setup()
            self.execute()
        except SystemExit:
            raise  # không nuốt tín hiệu thoát của orchestrator
        except KeyboardInterrupt:
            logger.info("event=job_interrupted app=%s", self.app_name)  # Ctrl-C là dừng bình thường
        except Exception as exc:
            logger.error("FATAL: job %s failed", self.app_name, exc_info=exc)
            raise SystemExit(1) from exc
        finally:
            self.teardown()  # LUÔN dọn dẹp

    @abstractmethod
    def setup(self):
        """Tạo resource (connection/session/producer...)."""

    @abstractmethod
    def execute(self):
        """Bước nghiệp vụ THAY ĐỔI theo từng job."""

    def teardown(self):  # noqa: B027 — hook tùy chọn, job con override khi cần dọn resource
        """Dọn dẹp resource — mặc định không làm gì, job con override khi cần."""
