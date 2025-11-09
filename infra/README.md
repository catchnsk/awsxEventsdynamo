# Webhooks Platform Infrastructure

This folder hosts Terraform configurations and helper scripts to provision the AWS foundation described in `01-aws-infra-plan.md`. The layout keeps shared modules in `terraform/modules/` and uses per-environment stacks in `terraform/envs/` (dev/stage/prod). Scripts under `scripts/` contain AWS CLI helpers (role assumption, validation, etc.).

## Structure
```
infra/
  README.md
  scripts/
    assume-role.sh
    tf-validate.sh
  terraform/
    modules/
      networking/
      security/
      dynamodb/
      msk/
      eks/
      lambdas/
    envs/
      dev/
```

## Module Highlights
- `networking`: Builds a /16 VPC with three public + three private subnets, NAT gateway, and VPC endpoints (S3, DynamoDB, CloudWatch Logs, STS, Secrets Manager, KMS, ECR, AppConfig).
- `security`: Provisions dedicated KMS keys (MSK, DynamoDB, Secrets, AppConfig), core secrets, and an AppConfig environment parameter.
- `dynamodb`: Creates all platform tables (`event_schema`, `partner_event_subscription`, `event_subscription_kafka`, `event_delivery_status`, `idempotency_ledger`) with PITR + SSE-KMS.
- `eks`: Wraps the official terraform-aws-modules/eks module with IRSA enabled and private endpoint access.
- `msk`: Stands up ingress/egress MSK clusters (TLS + IAM auth) and CloudWatch log groups.
- `lambdas`: Defines IAM roles/policies for the four Lambda services (schema admin, subscription admin, event processor, webhook dispatcher).

## Scripts
- `scripts/assume-role.sh <env> [session-name]`: Uses AWS CLI + STS to assume the per-environment Terraform role and exports temporary credentials (override `TF_ROLE_PREFIX` for different account IDs).
- `scripts/tf-validate.sh`: Runs `terraform fmt -recursive` from `infra/terraform` and `terraform validate` from the dev stack (duplicate the env folder before targeting stage/prod).

## Usage
1. Copy `terraform/envs/dev` to `stage` and `prod`, adjust `backend.tf`, `terraform.tfvars`, and tag values.
2. Assume the correct AWS account role:
   ```bash
   TF_ROLE_PREFIX=arn:aws:iam::123456789012:role/webhooks-dev-terraform ./scripts/assume-role.sh dev
   ```
3. From the environment directory, run the Terraform workflow:
   ```bash
   cd infra/terraform/envs/dev
   terraform init
   terraform workspace select dev || terraform workspace new dev
   terraform plan -var-file=terraform.tfvars
   terraform apply -var-file=terraform.tfvars
   ```
4. Run `./scripts/tf-validate.sh` (or the equivalent command per environment) before committing.

> **Note**: Resource counts, CIDRs, and secret values are placeholders. Update them per environment capacity/security requirements before applying.
