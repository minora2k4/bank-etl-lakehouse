#!/bin/sh
set -eu

python_bin="${PYTHON_BIN:-python}"
project_home="${PROJECT_HOME:-$(pwd)}"
export PYTHONPATH="${PYTHONPATH:-$project_home/src}"

"$python_bin" -m dashboard.export_metrics
