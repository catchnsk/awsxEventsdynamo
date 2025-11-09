#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <env> [session-name]" >&2
  exit 1
fi

ENV="$1"
SESSION_NAME="${2:-webhooks-tf}"
ROLE_ARN="${TF_ROLE_PREFIX:-arn:aws:iam::123456789012:role/webhooks-${ENV}-terraform}" # override via TF_ROLE_PREFIX

CREDS=$(aws sts assume-role --role-arn "$ROLE_ARN" --role-session-name "$SESSION_NAME")

export AWS_ACCESS_KEY_ID=$(echo "$CREDS" | jq -r '.Credentials.AccessKeyId')
export AWS_SECRET_ACCESS_KEY=$(echo "$CREDS" | jq -r '.Credentials.SecretAccessKey')
export AWS_SESSION_TOKEN=$(echo "$CREDS" | jq -r '.Credentials.SessionToken')

echo "Assumed $ROLE_ARN for $ENV"
