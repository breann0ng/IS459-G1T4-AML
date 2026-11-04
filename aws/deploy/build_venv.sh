#!/bin/bash
# Build the Python environment every EMR Serverless job loads (replaces the EMR bootstrap action
# local/legacy-emr/install_libs.sh). Built inside Amazon Linux so the libraries match EMR's OS.
#   AML_BUCKET=<bucket> bash aws/deploy/build_venv.sh      (needs Docker)
# Use the Python version of your EMR release and the same CPU type (x86_64 vs arm64) as the app.
set -euo pipefail
BUCKET="${AML_BUCKET:?set AML_BUCKET}"
cd "$(dirname "$0")"
docker run --rm -v "$PWD":/out amazonlinux:2023 bash -c "
  dnf install -y -q python3 python3-pip tar gzip >/dev/null &&
  python3 -m venv /venv && source /venv/bin/activate &&
  pip install -q -r /out/requirements-jobs.txt venv-pack &&
  venv-pack -f -o /out/venv.tar.gz"
aws s3 cp venv.tar.gz "s3://$BUCKET/code/venv.tar.gz"
