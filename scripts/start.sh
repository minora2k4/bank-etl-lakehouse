#!/bin/sh
set -eu

python_bin="${PYTHON_BIN:-python}"
project_home="${PROJECT_HOME:-$(pwd)}"

# --- ETL batch chạy TRONG container `pipeline` (thay scripts/pipeline.sh cũ; là CMD Dockerfile) ---
run_pipeline_stages() {
  export PYTHONPATH="${PYTHONPATH:-$project_home/src}"
  customers="${CUSTOMERS:-1000}"
  transactions="${TRANSACTIONS:-10000}"
  data_dir="${DATA_DIR:-data}"
  lakehouse_dir="${LAKEHOUSE_DIR:-lakehouse}"
  dashboard_dir="${DASHBOARD_DIR:-dashboard}"
  docs_dir="${DOCS_DIR:-docs}"
  clean_output="${CLEAN_OUTPUT:-1}"

  if [ "$clean_output" = "1" ]; then
    rm -rf "$data_dir"/* "$lakehouse_dir"/* "$dashboard_dir"/*.csv "$docs_dir"/data_quality_report.md
  fi

  "$python_bin" -m kafka.generate --customers "$customers" --transactions "$transactions"
  "$python_bin" -m application.load_sources_to_lakehouse
  "$python_bin" -m application.prepare_clean_data
  "$python_bin" -m application.build_curated_tables
  "$python_bin" -m application.build_customer_features
  "$python_bin" -m dashboard.export_metrics
}

# --- Nạp schema + source data vào PostgreSQL (thay scripts/postgres.sh) ---
load_postgres() {
  # Trên Git Bash (Windows) tắt path-conversion để /sql/... không bị đổi sang C:\...
  export MSYS_NO_PATHCONV=1
  postgres_user="${POSTGRES_USER:-banking}"
  postgres_db="${POSTGRES_DB:-banking}"
  docker compose up -d postgres
  docker compose exec -T postgres psql -U "$postgres_user" -d "$postgres_db" -f /sql/schema.sql
  docker compose exec -T postgres psql -U "$postgres_user" -d "$postgres_db" -f /sql/load_sources.sql
  docker compose exec -T postgres psql -U "$postgres_user" -d "$postgres_db" -c "SELECT 'customers' AS table_name, COUNT(*) FROM customers UNION ALL SELECT 'accounts', COUNT(*) FROM accounts UNION ALL SELECT 'cards', COUNT(*) FROM cards UNION ALL SELECT 'loans', COUNT(*) FROM loans UNION ALL SELECT 'repayments', COUNT(*) FROM repayments UNION ALL SELECT 'merchants', COUNT(*) FROM merchants UNION ALL SELECT 'branches', COUNT(*) FROM branches;"
}

# --- Mirror lakehouse local -> bucket MinIO (thay scripts/publish.sh) ---
publish_lakehouse() {
  docker compose --profile tools run --rm minio-setup
}

mode="${1:-batch}"
case "$mode" in
  pipeline)
    # Chỉ chạy ETL trong container, không gọi docker (dùng làm CMD của Dockerfile).
    run_pipeline_stages
    ;;
  batch)
    # Batch demo: chạy pipeline container rồi đẩy lakehouse lên MinIO.
    # --build để luôn dùng code mới nhất (compose KHÔNG tự rebuild image khi code đổi).
    docker compose run --rm --build pipeline
    publish_lakehouse
    ;;
  streaming)
    # Streaming demo đầy đủ: dựng nền tảng -> sinh dữ liệu tham chiếu -> nạp Postgres ->
    # bật Spark validator + updater -> bắn producer -> publish.
    sh scripts/setup.sh
    # --build ở các lệnh dưới để image producer/updater/pipeline luôn khớp code hiện tại.
    docker compose run --rm --build pipeline
    load_postgres
    docker compose up -d --build spark-streaming-validator postgres-transaction-updater
    # Producer chạy nền và bắn liên tục (STREAM_TRANSACTIONS<=0) để Kafka luôn có message mới
    # và Streamlit cập nhật benchmark theo thời gian thực.
    docker compose up -d --build transaction-producer
    publish_lakehouse
    echo "Streaming đang chạy: producer bắn liên tục -> Spark -> Postgres."
    echo "Xem benchmark: http://localhost:8501  ·  Kafka UI: http://localhost:8083"
    echo "Dừng producer: docker compose stop transaction-producer"
    ;;
  *)
    echo "Usage: sh scripts/start.sh [batch|streaming|pipeline]" >&2
    exit 1
    ;;
esac
