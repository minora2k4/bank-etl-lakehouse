#!/bin/sh
set -eu

PYTHON_BIN="${PYTHON_BIN:-python}"
PROJECT_HOME="${PROJECT_HOME:-$(pwd)}"
export PYTHONPATH="${PYTHONPATH:-$PROJECT_HOME/src}"

"$PYTHON_BIN" -m banking_lakehouse.dashboard.export_metrics
