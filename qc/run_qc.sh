#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
PYTHON="${PYTHON:-python3}"
mkdir -p data/raw data/processed evidence reports
rm -f reports/READY.json reports/qc_report.html
"$PYTHON" -m pytest -q tests --junitxml=evidence/tests.xml
"$PYTHON" src/pipeline.py
