#!/usr/bin/env bash
# Upload a downloaded dataset variant to the S3 landing zone, unchanged.
# Usage: bash scripts/upload_raw.sh HI-Large <bucket-name>
set -euo pipefail

NAME="${1:?usage: upload_raw.sh <dataset> <bucket>}"
BUCKET="${2:?usage: upload_raw.sh <dataset> <bucket>}"

aws s3 cp "data/${NAME}/" "s3://${BUCKET}/landing/dataset=${NAME}/" --recursive
