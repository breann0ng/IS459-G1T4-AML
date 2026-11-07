"""
deploy_branch.py                         (laptop, any branch: no main, no bash, no zip tool needed)
Upload THIS checkout's job code to your own folder in the bucket, the way aws/deploy/deploy.sh does
for main: every aws/jobs/*/*.py and *.sh as an entry script, plus one FLAT jobs.zip for --py-files.
The team's s3://<bucket>/jobs/ is never touched; the shared jobs/venv.tar.gz is reused.

    python local/runs/deploy_branch.py --profile g1t4 --dry-run        list what would be uploaded
    python local/runs/deploy_branch.py --profile g1t4                  upload to dev/enrui/jobs/

Then run with  local/runs/run_final_serverless.py --code-prefix dev/enrui/jobs ...
"""
import argparse
import io
import subprocess
import sys
import zipfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", default="is459-g1t4-aml")
    ap.add_argument("--prefix", default="dev/enrui/jobs", help="folder in the bucket (never the team's jobs/)")
    ap.add_argument("--profile", help="AWS profile for the shared account (g1t4)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    prefix = a.prefix.strip("/")
    if prefix == "jobs":
        sys.exit("jobs/ is the team's code (deploy.sh from main). Use your own folder, e.g. dev/enrui/jobs")

    files = sorted(p for p in (REPO / "aws" / "jobs").glob("*/*") if p.suffix in (".py", ".sh"))
    names = [p.name for p in files]
    dupes = {n for n in names if names.count(n) > 1}
    if dupes:
        sys.exit(f"two files share a name, jobs.zip is flat: {sorted(dupes)}")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for p in files:
            if p.suffix == ".py":
                z.write(p, p.name)
    commit = subprocess.run(["git", "-C", str(REPO), "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", str(REPO), "status", "--porcelain", "aws/jobs"], capture_output=True, text=True).stdout.strip()
    print(f"{len(files)} files + jobs.zip ({buf.tell() / 1e3:.0f} KB) from {commit}{' + uncommitted changes' if dirty else ''} "
          f"-> s3://{a.bucket}/{prefix}/")
    if a.dry_run:
        print("\n".join(f"    {n}" for n in names))
        return

    import boto3
    s3 = boto3.Session(profile_name=a.profile, region_name="us-east-1").client("s3")
    for p in files:
        s3.upload_file(str(p), a.bucket, f"{prefix}/{p.name}")
    s3.put_object(Bucket=a.bucket, Key=f"{prefix}/jobs.zip", Body=buf.getvalue())
    keep = set(names) | {"jobs.zip"}
    stale = [o["Key"] for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=a.bucket, Prefix=f"{prefix}/")
             for o in pg.get("Contents", []) if o["Key"].split("/")[-1] not in keep]
    for k in stale:                      # like deploy.sh's sync --delete, only inside your own folder
        s3.delete_object(Bucket=a.bucket, Key=k)
    print(f"uploaded; removed {len(stale)} old files. Run with --code-prefix {prefix}")


if __name__ == "__main__":
    main()
