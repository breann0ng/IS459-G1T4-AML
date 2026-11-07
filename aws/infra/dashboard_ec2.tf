# Streamlit dashboard on one small EC2 instance (cheapest in-AWS option). The instance role carries the same
# read-only policy as the dashboard user, so no access keys are stored anywhere.
# The app code is pulled from s3://<bucket>/app-code/ every time the service starts (stop/start to redeploy).

variable "dashboard_cidr" {
  type        = string
  description = "Who may reach the dashboard on port 8501, e.g. 1.2.3.4/32"
}

variable "dashboard_instance_type" {
  type    = string
  default = "t4g.micro"
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_ssm_parameter" "al2023_arm" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

resource "aws_iam_role" "dashboard" {
  name = "aml-dashboard-ec2"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "ec2.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy" "dashboard" {
  role   = aws_iam_role.dashboard.id
  policy = local.dashboard_policy
}

resource "aws_iam_role_policy" "dashboard_code" {
  role = aws_iam_role.dashboard.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = "arn:aws:s3:::${var.bucket}/app-code/*" },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = "arn:aws:s3:::${var.bucket}",
      Condition = { StringLike = { "s3:prefix" = ["app-code/*"] } } },
    ]
  })
}

resource "aws_iam_instance_profile" "dashboard" {
  name = "aml-dashboard-ec2"
  role = aws_iam_role.dashboard.name
}

resource "aws_security_group" "dashboard" {
  name        = "aml-dashboard"
  description = "Streamlit dashboard, restricted source"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Streamlit"
    from_port   = 8501
    to_port     = 8501
    protocol    = "tcp"
    cidr_blocks = [var.dashboard_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "dashboard" {
  ami                         = data.aws_ssm_parameter.al2023_arm.value
  instance_type               = var.dashboard_instance_type
  subnet_id                   = data.aws_subnets.default.ids[0]
  associate_public_ip_address = true
  user_data_replace_on_change = true
  vpc_security_group_ids      = [aws_security_group.dashboard.id]
  iam_instance_profile        = aws_iam_instance_profile.dashboard.name

  metadata_options {
    http_tokens = "required"
  }
  root_block_device {
    volume_size = 8
    volume_type = "gp3"
  }

  user_data = <<-EOT
    #!/bin/bash
    set -e
    dd if=/dev/zero of=/swapfile bs=1M count=1024 && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
    dnf install -y python3.11 python3.11-pip
    python3.11 -m venv /opt/dash
    /opt/dash/bin/pip install pandas pyarrow numpy streamlit altair boto3
    cat >/etc/systemd/system/dashboard.service <<'UNIT'
    [Unit]
    Description=BQ2 dashboard
    After=network-online.target
    [Service]
    Environment=AML_ROOT=s3://${var.bucket}
    Environment=AWS_REGION=${var.region}
    Environment=BQ2_APPROVAL_QUEUE_URL=${aws_sqs_queue.bq2_approvals.url}
    ExecStartPre=/usr/bin/aws s3 sync s3://${var.bucket}/app-code /opt/app --delete
    WorkingDirectory=/opt/app
    ExecStart=/opt/dash/bin/streamlit run app_bq2.py --server.port 8501 --server.headless true
    Restart=always
    [Install]
    WantedBy=multi-user.target
    UNIT
    mkdir -p /opt/app
    systemctl daemon-reload
    systemctl enable --now dashboard
  EOT

  tags = { Name = "aml-dashboard" }
}

output "dashboard_url" {
  description = "Public IP changes after a stop/start; read it again with terraform output"
  value       = "http://${aws_instance.dashboard.public_ip}:8501"
}
