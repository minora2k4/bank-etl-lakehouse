#!/bin/sh
set -eu

PYTHON_BIN="${PYTHON_BIN:-python}"
PROJECT_HOME="${PROJECT_HOME:-$(pwd)}"
export PYTHONPATH="${PYTHONPATH:-$PROJECT_HOME/src}"
CUSTOMERS="${CUSTOMERS:-1000}"
TRANSACTIONS="${TRANSACTIONS:-10000}"
DATA_DIR="${DATA_DIR:-data}"
LAKEHOUSE_DIR="${LAKEHOUSE_DIR:-lakehouse}"
DASHBOARD_DIR="${DASHBOARD_DIR:-dashboard}"
DOCS_DIR="${DOCS_DIR:-docs}"
CLEAN_OUTPUT="${CLEAN_OUTPUT:-1}"

if [ "$CLEAN_OUTPUT" = "1" ]; then
  rm -rf "$DATA_DIR"/* "$LAKEHOUSE_DIR"/* "$DASHBOARD_DIR"/*.csv "$DOCS_DIR"/data_quality_report.md
fi

"$PYTHON_BIN" -m banking_lakehouse.data_simulator.generate --customers "$CUSTOMERS" --transactions "$TRANSACTIONS"
"$PYTHON_BIN" -m banking_lakehouse.application.load_sources_to_lakehouse
"$PYTHON_BIN" -m banking_lakehouse.application.prepare_clean_data
"$PYTHON_BIN" -m banking_lakehouse.application.build_curated_tables
"$PYTHON_BIN" -m banking_lakehouse.application.build_customer_features
"$PYTHON_BIN" -m banking_lakehouse.dashboard.export_metrics
