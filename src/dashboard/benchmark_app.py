"""Streamlit dashboard xem benchmark Kafka / Spark / Database của pipeline.

Đọc số liệu runtime do các service ghi (`<lakehouse>/benchmark/*.csv` qua connector.benchmark_sink)
và các CSV thống kê do `dashboard.export_metrics` sinh ra (`dashboard/*.csv`).

Chạy: streamlit run src/dashboard/benchmark_app.py
"""

import pandas as pd
import streamlit as st

from config.settings import benchmark_dir, dashboard_dir

st.set_page_config(page_title="Banking Pipeline Benchmark", page_icon="📊", layout="wide")


def load_csv(path):
    """Đọc CSV thành DataFrame; trả None nếu file chưa tồn tại hoặc rỗng."""
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path)
        return df if not df.empty else None
    except Exception:
        return None


def latest(df, column, default="—"):
    """Giá trị ở dòng cuối (mới nhất) của một cột."""
    if df is None or column not in df.columns or df.empty:
        return default
    return df[column].iloc[-1]


def no_data(message="Chưa có dữ liệu. Hãy chạy `sh scripts/start.sh streaming` để sinh benchmark."):
    st.info(message)


# --- Header + nút làm mới ---
header, refresh = st.columns([6, 1])
header.title("📊 Banking Pipeline — Benchmark")
header.caption(f"benchmark: `{benchmark_dir}` · dashboard: `{dashboard_dir}`")
if refresh.button("🔄 Làm mới"):
    st.rerun()

producer_df = load_csv(benchmark_dir / "kafka_producer.csv")
spark_df = load_csv(benchmark_dir / "spark_validator.csv")
updater_df = load_csv(benchmark_dir / "db_updater.csv")
quality_df = load_csv(dashboard_dir / "quality_summary.csv")
errors_df = load_csv(dashboard_dir / "quality_errors.csv")
storage_df = load_csv(dashboard_dir / "storage_benchmark.csv")
query_df = load_csv(dashboard_dir / "query_benchmark.csv")
inventory_df = load_csv(dashboard_dir / "file_inventory.csv")

tab_overview, tab_kafka, tab_spark, tab_db, tab_quality = st.tabs(
    ["Tổng quan", "Kafka", "Spark", "Database", "Lưu trữ & Chất lượng"]
)

# --- Tổng quan ---
with tab_overview:
    st.subheader("Throughput mới nhất theo từng tầng")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Kafka producer (msg/s)", latest(producer_df, "throughput_msg_s"))
    c2.metric("Spark validator (events/s)", latest(spark_df, "throughput_events_s"))
    c3.metric("DB updater (msg/s)", latest(updater_df, "throughput_msg_s"))
    c4.metric("DB e2e p95 (ms)", latest(updater_df, "e2e_p95_ms"))

    if quality_df is not None:
        st.subheader("Chất lượng dữ liệu")
        q = quality_df.iloc[-1]
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("Customers valid", int(q.get("customers_valid", 0)))
        d2.metric("Accounts valid", int(q.get("accounts_valid", 0)))
        d3.metric("Transactions valid", int(q.get("transactions_valid", 0)))
        d4.metric("Bản ghi lỗi", int(q.get("invalid_records", 0)))

    if producer_df is None and spark_df is None and updater_df is None:
        no_data()

# --- Kafka ---
with tab_kafka:
    st.subheader("Kafka producer — throughput các lần chạy")
    if producer_df is None:
        no_data("Chưa có benchmark producer. Chạy `sh scripts/start.sh streaming`.")
    else:
        st.metric("Throughput gần nhất (msg/s)", latest(producer_df, "throughput_msg_s"))
        st.bar_chart(producer_df, x="ts", y="throughput_msg_s")
        st.dataframe(producer_df, use_container_width=True)

# --- Spark ---
with tab_spark:
    st.subheader("Spark validator — throughput & độ trễ từng micro-batch")
    if spark_df is None:
        no_data("Chưa có benchmark Spark. Chạy `sh scripts/start.sh streaming`.")
    else:
        m1, m2, m3 = st.columns(3)
        m1.metric("Events/s gần nhất", latest(spark_df, "throughput_events_s"))
        m2.metric("Duration gần nhất (ms)", latest(spark_df, "duration_ms"))
        m3.metric("Tổng micro-batch", len(spark_df))
        st.line_chart(spark_df, y="throughput_events_s")
        st.line_chart(spark_df, y="duration_ms")
        if {"valid", "invalid"}.issubset(spark_df.columns):
            st.bar_chart(spark_df, y=["valid", "invalid"])
        st.dataframe(spark_df, use_container_width=True)

# --- Database ---
with tab_db:
    st.subheader("PostgreSQL updater — throughput & end-to-end latency")
    if updater_df is None:
        no_data("Chưa có benchmark updater. Chạy `sh scripts/start.sh streaming`.")
    else:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Throughput gần nhất (msg/s)", latest(updater_df, "throughput_msg_s"))
        m2.metric("e2e p50 (ms)", latest(updater_df, "e2e_p50_ms"))
        m3.metric("e2e p95 (ms)", latest(updater_df, "e2e_p95_ms"))
        m4.metric("e2e p99 (ms)", latest(updater_df, "e2e_p99_ms"))
        st.line_chart(updater_df, y="throughput_msg_s")
        if {"e2e_p50_ms", "e2e_p95_ms", "e2e_p99_ms"}.issubset(updater_df.columns):
            st.line_chart(updater_df, y=["e2e_p50_ms", "e2e_p95_ms", "e2e_p99_ms"])
        st.dataframe(updater_df, use_container_width=True)

    if query_df is not None:
        st.subheader("Query benchmark (quét curated summary)")
        st.dataframe(query_df, use_container_width=True)

# --- Lưu trữ & Chất lượng ---
with tab_quality:
    st.subheader("Dung lượng theo layer lakehouse")
    if storage_df is None:
        no_data("Chưa có dashboard CSV. Chạy `sh scripts/start.sh` (batch) trước.")
    else:
        st.bar_chart(storage_df, x="dataset", y="size_bytes")
        st.dataframe(storage_df, use_container_width=True)

    if errors_df is not None:
        st.subheader("Lỗi chất lượng dữ liệu theo loại")
        if {"error_type", "error_count"}.issubset(errors_df.columns):
            st.bar_chart(errors_df, x="error_type", y="error_count")
        st.dataframe(errors_df, use_container_width=True)

    if inventory_df is not None:
        st.subheader("File inventory")
        st.dataframe(inventory_df, use_container_width=True)
