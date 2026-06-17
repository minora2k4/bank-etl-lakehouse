"""Adapter ghi số liệu benchmark runtime ra CSV để Streamlit đọc."""

from config.settings import benchmark_dir
from utils.io import append_csv, now_ts


def record_benchmark(component, **metrics):
    """Ghi (best-effort) một dòng benchmark có timestamp.

    Lỗi I/O ở đây tuyệt đối không được làm sập job đang đo, nên nuốt mọi lỗi: benchmark
    chỉ là quan sát phụ, không thuộc đường nghiệp vụ chính.
    """
    try:
        append_csv(benchmark_dir / f"{component}.csv", [{"ts": now_ts(), **metrics}])
    except Exception:
        pass
