"""
bq2_deploy.py                                                            (BQ2, gate 2)
Promotes a trained version: writes models/bq2r/<dataset>/deployed.json, which scoring and
monitoring read when given the version "deployed". Run only after a person approved the challenger.
    python3 bq2_deploy.py SAML-R v1 <approver> [deploy_from YYYY-MM-DD]
"""
import json
import sys
from datetime import datetime, timezone

import bq2_common as C

dataset, version, approver = sys.argv[1], sys.argv[2], sys.argv[3]
deploy_from = sys.argv[4] if len(sys.argv) > 4 else None
C.get_json(f"{C.ROOT}/models/bq2r/{dataset}/{version}/meta.json")      # fails if that version was never trained
rec = {"dataset": dataset, "version": version, "approved_by": approver, "deploy_from": deploy_from,
       "deployed_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
C.put_text(f"{C.ROOT}/models/bq2r/{dataset}/deployed.json", json.dumps(rec, indent=1))
print(f"[deploy] {dataset}: {version} is now deployed ({rec})")
