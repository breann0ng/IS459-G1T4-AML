#!/usr/bin/env bash
# Download one IBM AML dataset variant from Kaggle into data/.
# Usage: bash scripts/download_data.sh HI-Small   (HI|LI x Small|Medium|Large)
# Needs Kaggle credentials in ~/.kaggle/kaggle.json.
set -euo pipefail

NAME="${1:?usage: download_data.sh <HI-Small|HI-Medium|HI-Large|LI-Small|LI-Medium|LI-Large>}"
DATASET="ealtman2019/ibm-transactions-for-anti-money-laundering-aml"
OUT="data/${NAME}"

mkdir -p "$OUT"
for f in "${NAME}_Trans.csv" "${NAME}_Patterns.txt" "${NAME}_accounts.csv"; do
  kaggle datasets download -d "$DATASET" -f "$f" -p "$OUT"
  # Kaggle zips large single files
  if [ -f "$OUT/$f.zip" ]; then
    unzip -o "$OUT/$f.zip" -d "$OUT" && rm "$OUT/$f.zip"
  fi
done
echo "Downloaded ${NAME} to ${OUT}"
