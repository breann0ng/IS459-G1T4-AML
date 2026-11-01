#!/bin/bash
# EMR bootstrap action: runs on EVERY node (primary + core) when the cluster starts,
# so the Spark executors can import lightgbm / pandas / pyarrow for scoring.
set -e
sudo python3 -m pip install -q pandas pyarrow numpy scikit-learn lightgbm networkx xxhash
python3 -c "import lightgbm, pyarrow, pandas; print('libs ok', lightgbm.__version__, pyarrow.__version__)"
