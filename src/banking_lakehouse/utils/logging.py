from datetime import datetime


def log_info(event, **fields):
    """Log một event dạng key=value ngắn gọn."""
    parts = [f"event={event}", f"ts={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"]
    for key, value in fields.items():
        parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)