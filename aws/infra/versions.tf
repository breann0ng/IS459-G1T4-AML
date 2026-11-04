terraform {
  required_version = ">= 1.10"          # 1.5+ for import blocks, 1.10+ for S3 state locking without DynamoDB
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = "is459-g1t4-aml", managed_by = "terraform" }
  }
}
