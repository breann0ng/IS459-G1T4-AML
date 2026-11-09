#!/bin/bash
# One-off: stream the IBM AML files from Kaggle straight into S3 raw/ (nothing on disk).
# Taken from local/legacy-emr/rebuild_final.sh. Needs ~/.kaggle/kaggle.json (legacy token).
#   AML_BUCKET=<bucket> bash load_kaggle.sh HI-Small HI-Medium HI-Large
set -o pipefail
BUCKET="${AML_BUCKET:?set AML_BUCKET}"; ROOT="s3://$BUCKET"
KAGGLE=https://www.kaggle.com/api/v1/datasets/download/ealtman2019/ibm-transactions-for-anti-money-laundering-aml
download () {   # download <kaggle file> <s3 target>
  U=$(python3 -c "import json;print(json.load(open('$HOME/.kaggle/kaggle.json'))['username'])") || return 1
  K=$(python3 -c "import json;print(json.load(open('$HOME/.kaggle/kaggle.json'))['key'])") || return 1
  curl -sfL -u "$U:$K" "$KAGGLE/$1" | aws s3 cp - "$2" --expected-size 30000000000 || return 1
  head=$(aws s3 cp "$2" - 2>/dev/null | head -c 60)
  case "$head" in Timestamp*|BEGIN*|Bank\ Name*) echo "ok $1" ;; *) echo "unexpected content in $1"; return 1 ;; esac
}
for D in "${@:-HI-Small HI-Medium HI-Large}"; do
  download "${D}_Trans.csv"    "$ROOT/raw/transactions/set=$D/${D}_Trans.csv"   || exit 1
  download "${D}_Patterns.txt" "$ROOT/raw/patterns/set=$D/${D}_Patterns.txt"   || exit 1
  download "${D}_accounts.csv" "$ROOT/raw/accounts/set=$D/${D}_accounts.csv"   || exit 1
done
