#!/bin/bash
# Upload the pipeline code to S3 for EMR Serverless.
#   AML_BUCKET=<bucket> bash aws/deploy/deploy.sh          (from the repo root, on main only)
# Entry scripts go to s3://<bucket>/code/jobs/<name>.py; every module also goes into one FLAT
# jobs.zip (passed with --py-files) so `import train_txn` etc. work exactly as on the old cluster.
set -euo pipefail
BUCKET="${AML_BUCKET:?set AML_BUCKET}"
export AWS_REGION="${AWS_REGION:-ap-southeast-2}"
cd "$(dirname "$0")/../.."
[ "$(git branch --show-current)" = "main" ] || { echo "deploy from main only"; exit 1; }
rm -rf build && mkdir -p build/jobs
cp aws/jobs/*/*.py aws/jobs/*/*.sh build/jobs/ 2>/dev/null || true
( cd build/jobs && zip -qj ../jobs.zip *.py )
aws s3 sync build/jobs/ "s3://$BUCKET/code/jobs/" --delete
aws s3 cp build/jobs.zip "s3://$BUCKET/code/jobs.zip"
echo "deployed $(ls build/jobs | wc -l) files from $(git rev-parse --short HEAD) to s3://$BUCKET/code/"
