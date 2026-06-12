import csv
import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path


def ensure_dir(path):
    """Tạo thư mục nếu chưa tồn tại."""
    Path(path).mkdir(parents=True, exist_ok=True)


def read_csv(path):
    """Đọc CSV thành list dict."""
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, fieldnames=None):
    """Ghi list dict ra CSV ổn định schema."""
    path = Path(path)
    rows = list(rows)
    if fieldnames is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
        fieldnames = fields
    ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def append_csv(path, rows, fieldnames=None):
    """Ghi nối CSV cho các file cùng schema."""
    path = Path(path)
    rows = list(rows)
    exists = Path(path).exists()
    if fieldnames is None:
        fieldnames = list(rows[0].keys()) if rows else []
    ensure_dir(path.parent)
    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerows(rows)


def row_hash(row):
    """Tạo hash cho bản ghi để audit và reprocess."""
    payload = json.dumps(row, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def json_dumps(row):
    """Chuyển bản ghi sang JSON string cho error layer."""
    return json.dumps(row, sort_keys=True, ensure_ascii=False, default=str)


def now_ts():
    """Trả về timestamp hiện tại dạng text."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def reset_generated_dirs(paths):
    """Xóa output do pipeline sinh ra khi chạy chế độ clean."""
    for path in paths:
        p = Path(path)
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(parents=True, exist_ok=True)
