#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR/infra/terraform"
terraform fmt -recursive
cd "$ROOT_DIR/infra/terraform/envs/dev"
terraform validate
