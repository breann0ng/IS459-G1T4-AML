# Final BQ1 run on EMR Serverless (full HI-Large)

`run_final_serverless.py` submits the final pipeline as 38 EMR Serverless jobs, in order, from a laptop.
It does no data work itself; it is the stop-gap until `aws/step-functions/` runs the same steps.

## One-off setup (AWS CloudShell, us-east-1: it has the AWS CLI, Docker and fast access to S3)

```bash
git clone https://github.com/<org>/IS459-G1T4-AML.git && cd IS459-G1T4-AML   # main
# 1. raw data: upload kaggle.json (Actions > Upload file), then
mkdir -p ~/.kaggle && mv ~/kaggle.json ~/.kaggle/
AML_BUCKET=is459-g1t4-aml bash aws/jobs/prepare/load_kaggle.sh HI-Medium HI-Large   # HI-Small is already there
# 2. code: entry scripts + jobs.zip
sudo dnf install -y zip && AML_BUCKET=is459-g1t4-aml bash aws/deploy/deploy.sh
# 3. libraries: venv.tar.gz (check with Breann first, she may have built it)
AML_BUCKET=is459-g1t4-aml bash aws/deploy/build_venv.sh
```

## Laptop

```powershell
aws configure --profile g1t4        # or `aws configure sso`: shared-account login, region us-east-1
.venv\Scripts\python local\runs\run_final_serverless.py --dry-run
.venv\Scripts\python local\runs\run_final_serverless.py --profile g1t4 --only clean_HI-Small   # smoke test
.venv\Scripts\python local\runs\run_final_serverless.py --profile g1t4                         # everything
.venv\Scripts\python local\runs\run_final_serverless.py --profile g1t4 --list                  # progress
```

A failed step stops the run and prints the driver log path; fix it and re-run the same command.
Each finished step prints its minutes and vCPU-hours (for the cost section and the recording).
