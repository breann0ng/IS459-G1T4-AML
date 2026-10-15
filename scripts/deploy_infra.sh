#!/usr/bin/env bash
# Deploy / update the CloudFormation stack.
set -euo pipefail

STACK="${STACK:-is459-g1t4-aml}"
aws cloudformation deploy \
  --template-file infra/template.yaml \
  --stack-name "$STACK" \
  --capabilities CAPABILITY_NAMED_IAM
aws cloudformation describe-stacks --stack-name "$STACK" --query "Stacks[0].Outputs"
