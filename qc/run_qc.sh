#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
PYTHON="${PYTHON:-python3}"
mkdir -p data/raw data/processed evidence reports
trap 'rm -f reports/READY.json reports/qc_report.html reports/sql_benchmark.html' ERR
rm -f reports/READY.json reports/qc_report.html reports/sql_benchmark.html evidence/sql_benchmark.json evidence/sql_benchmark.csv
"$PYTHON" -m pytest -q tests --junitxml=evidence/tests.xml
"$PYTHON" src/pipeline.py

"$PYTHON" benchmark/run_benchmark.py
