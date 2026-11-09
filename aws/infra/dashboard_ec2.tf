# Streamlit dashboard on one small EC2 instance (cheapest in-AWS option). The instance role carries the same
# read-only policy as the dashboard user, so no access keys are stored anywhere.
# The app code is pulled from s3://<bucket>/app-code/ every time the service starts (stop/start to redeploy).

variable "dashboard_cidr" {
  type        = string
  default     = "0.0.0.0/0" # as applied: open, so graders and teammates can reach it; the approval page needs a passcode
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
    Version   = "2012-10-17"
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

# The BQ2 approval page checks a passcode kept in SSM Parameter Store (/aml/bq2/approver_passcode).
resource "aws_iam_role_policy" "dashboard_passcode" {
  role = aws_iam_role.dashboard.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["ssm:GetParameter"], Resource = "arn:aws:ssm:${var.region}:*:parameter/aml/bq2/approver_passcode" }]
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

  # The startup script exactly as the running instance has it (CRLF line endings included): any change to
  # this file replaces the instance (user_data_replace_on_change), so edit it only to rebuild on purpose.
  user_data = file("${path.module}/dashboard_user_data.sh")

  lifecycle {
    ignore_changes = [ami] # the SSM parameter moves to each new AL2023 release; never replace the box for that
  }

  tags = { Name = "aml-dashboard" }
}

output "dashboard_url" {
  description = "Public IP changes after a stop/start; read it again with terraform output"
  value       = "http://${aws_instance.dashboard.public_ip}:8501"
}
