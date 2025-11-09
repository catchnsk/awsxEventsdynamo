terraform {
  required_version = ">= 1.6.0"
}

locals {
  kms_keys = {
    msk       = "MSK encryption key"
    dynamodb  = "DynamoDB table encryption"
    secrets   = "Secrets Manager encryption"
    appconfig = "AppConfig encryption"
  }
}

resource "aws_kms_key" "this" {
  for_each                = local.kms_keys
  description             = each.value
  deletion_window_in_days = 30
  enable_key_rotation     = true
  multi_region            = false
  tags = merge(var.tags, {
    Name = "${var.name_prefix}-${each.key}-kms"
  })
  policy = var.kms_policy_json != null ? var.kms_policy_json : null
}

resource "aws_kms_alias" "this" {
  for_each      = aws_kms_key.this
  name          = "alias/${var.name_prefix}/${each.key}"
  target_key_id = each.value.key_id
}

resource "aws_secretsmanager_secret" "partner_credentials" {
  name        = "${var.name_prefix}/partner/credentials"
  description = "Stores partner webhook auth tokens"
  kms_key_id  = aws_kms_key.this["secrets"].arn
  tags        = var.tags
}

resource "aws_secretsmanager_secret" "webhook_signing" {
  name        = "${var.name_prefix}/webhook/signing"
  description = "Stores signing secrets for webhook dispatch"
  kms_key_id  = aws_kms_key.this["secrets"].arn
  tags        = var.tags
}

resource "aws_ssm_parameter" "appconfig_env" {
  name  = "/${var.name_prefix}/appconfig/environment"
  type  = "String"
  value = var.environment
  tags  = var.tags
}
