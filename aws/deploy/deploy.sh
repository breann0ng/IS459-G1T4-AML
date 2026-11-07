#!/bin/bash
# Upload the pipeline code to S3 for EMR Serverless.
#   AML_BUCKET=<bucket> bash aws/deploy/deploy.sh          (from the repo root; any branch, warns if not main)
#   AML_BUCKET=<bucket> bash aws/deploy/deploy.sh clean_transactions_spark.py split_weeks_spark.py
#                                                           (only these entry scripts; jobs.zip untouched)
# Never deletes from S3: jobs that live only there (e.g. bq2_deploy.py, used by aml_bq2_weekly) stay.
# Entry scripts go to s3://<bucket>/jobs/<name>.py; every module also goes into one FLAT
# jobs.zip (passed with --py-files) so `import train_txn` etc. work exactly as on the old cluster.
set -euo pipefail
BUCKET="${AML_BUCKET:?set AML_BUCKET}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
cd "$(dirname "$0")/../.."
BRANCH="$(git branch --show-current)"
# Not blocked: the S3 code is shared, so a deploy from another branch replaces everyone's jobs.
[ "$BRANCH" = "main" ] || echo "WARNING: deploying from '$BRANCH', not main: this replaces the shared jobs in s3://$BUCKET/jobs/"
[ -z "$(git status --porcelain -- aws/jobs)" ] || echo "WARNING: aws/jobs has uncommitted changes: the deployed code matches no commit"
rm -rf build && mkdir -p build/jobs
cp aws/jobs/*/*.py aws/jobs/*/*.sh aws/jobs/*/*/*.py aws/jobs/*/*/*.sh build/jobs/ 2>/dev/null || true   # incl. per-person subfolders (bq2/tasha/)
if [ $# -gt 0 ]; then                       # selected entry scripts only
  for f in "$@"; do
    [ -f "build/jobs/$f" ] || { echo "no aws/jobs/**/$f"; exit 1; }
    aws s3 cp "build/jobs/$f" "s3://$BUCKET/jobs/$f"
  done
  echo "deployed $# file(s) from $BRANCH@$(git rev-parse --short HEAD) to s3://$BUCKET/jobs/ (jobs.zip unchanged)"
  exit 0
fi
( cd build/jobs && zip -qj ../jobs.zip *.py )
aws s3 sync build/jobs/ "s3://$BUCKET/jobs/" --exclude "jobs.zip" --exclude "venv.tar.gz"
aws s3 cp build/jobs.zip "s3://$BUCKET/jobs/jobs.zip"
echo "deployed $(ls build/jobs | wc -l) files from $BRANCH@$(git rev-parse --short HEAD) to s3://$BUCKET/jobs/"
