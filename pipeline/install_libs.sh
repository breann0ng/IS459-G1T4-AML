#!/bin/bash
# EMR bootstrap action: runs on EVERY node (primary + core) when the cluster starts,
# so the Spark executors can import lightgbm / pandas / pyarrow for scoring.
set -e
sudo python3 -m pip install -q pandas pyarrow numpy scikit-learn lightgbm networkx xxhash
# pip replaces the system python3-dateutil that the AWS CLI imports; put the system copy back
sudo dnf reinstall -y -q python3-dateutil || sudo dnf install -y -q python3-dateutil || true
/usr/bin/python3 -c "import dateutil" || sudo /usr/bin/python3 -m pip install -q python-dateutil
python3 -c "import lightgbm, pyarrow, pandas; print('libs ok', lightgbm.__version__, pyarrow.__version__)"
aws s3 ls > /dev/null && echo "aws cli ok" || echo "aws cli broken: run sudo dnf reinstall -y python3-dateutil"
