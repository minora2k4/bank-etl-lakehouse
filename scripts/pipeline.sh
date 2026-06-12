#!/bin/sh
set -eu

python_bin="${PYTHON_BIN:-python}"
project_home="${PROJECT_HOME:-$(pwd)}"
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
