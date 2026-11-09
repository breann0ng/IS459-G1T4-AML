#!/bin/bash
set -e
dd if=/dev/zero of=/swapfile bs=1M count=1024 && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
dnf install -y python3.11 python3.11-pip
python3.11 -m venv /opt/dash
/opt/dash/bin/pip install pandas pyarrow numpy 'streamlit>=1.67' 'altair>=5' boto3
cat >/etc/systemd/system/dashboard.service <<'UNIT'
[Unit]
Description=AML dashboard (BQ1 and BQ2)
After=network-online.target
[Service]
Environment=AML_ROOT=s3://is459-g1t4-aml
Environment=AWS_REGION=us-east-1
Environment=BQ2_APPROVAL_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/390402569482/aml-bq2-approvals
ExecStartPre=/usr/bin/aws s3 sync s3://is459-g1t4-aml/app-code /opt/app --delete
WorkingDirectory=/opt/app
ExecStart=/opt/dash/bin/streamlit run bq1_site.py --server.port 8501 --server.headless true
Restart=always
[Install]
WantedBy=multi-user.target
UNIT
mkdir -p /opt/app
systemctl daemon-reload
systemctl enable --now dashboard
