#!/bin/bash
# EMR bootstrap action: runs on EVERY node (primary + core) when the cluster starts,
# so the Spark executors can import lightgbm / pandas / pyarrow for scoring.
set -e
# --ignore-installed: install into /usr/local WITHOUT uninstalling the OS copies in /usr/lib
# (pip upgrading python-dateutil removed the OS copy and broke EMR's own `aws` CLI, which only
# reads /usr/lib); dateutil < 3: the aws CLI on EMR 8 requires <= 2.9
sudo python3 -m pip install -q --ignore-installed pandas pyarrow numpy scikit-learn lightgbm networkx xxhash "python-dateutil<3"
python3 -c "import lightgbm, pyarrow, pandas; print('libs ok', lightgbm.__version__, pyarrow.__version__)"
