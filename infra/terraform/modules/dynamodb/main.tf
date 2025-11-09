terraform {
  required_version = ">= 1.6.0"
}

locals {
  common_tags = var.tags
}

resource "aws_dynamodb_table" "event_schema" {
  name         = "${var.name_prefix}-event_schema"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "PK"

  attribute { name = "PK"; type = "S" }
  attribute { name = "status"; type = "S" }
  attribute { name = "contains_sensitive"; type = "S" }

  global_secondary_index {
    name            = "status-index"
    hash_key        = "status"
    projection_type = "ALL"
  }

  global_secondary_index {
    name            = "contains_sensitive-index"
    hash_key        = "contains_sensitive"
    projection_type = "ALL"
  }

  point_in_time_recovery { enabled = true }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  tags = merge(local.common_tags, { Table = "event_schema" })
}

resource "aws_dynamodb_table" "partner_event_subscription" {
  name         = "${var.name_prefix}-partner_event_subscription"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "PK"

  attribute { name = "PK"; type = "S" }
  attribute { name = "status"; type = "S" }

  global_secondary_index {
    name            = "status-index"
    hash_key        = "status"
    projection_type = "ALL"
  }

  point_in_time_recovery { enabled = true }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  tags = merge(local.common_tags, { Table = "partner_event_subscription" })
}

resource "aws_dynamodb_table" "event_subscription_kafka" {
  name         = "${var.name_prefix}-event_subscription_kafka"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "EVENT_SUBSCRIPTION_ID"

  attribute { name = "EVENT_SUBSCRIPTION_ID"; type = "S" }

  point_in_time_recovery { enabled = true }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  tags = merge(local.common_tags, { Table = "event_subscription_kafka" })
}

resource "aws_dynamodb_table" "event_delivery_status" {
  name         = "${var.name_prefix}-event_delivery_status"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "WEBHOOK_EVENT_UUID"

  attribute { name = "WEBHOOK_EVENT_UUID"; type = "S" }
  attribute { name = "status"; type = "S" }

  global_secondary_index {
    name            = "status-index"
    hash_key        = "status"
    projection_type = "ALL"
  }

  point_in_time_recovery { enabled = true }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  tags = merge(local.common_tags, { Table = "event_delivery_status" })
}

resource "aws_dynamodb_table" "idempotency_ledger" {
  name         = "${var.name_prefix}-idempotency_ledger"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "IDEMPOTENCY_KEY"

  attribute { name = "IDEMPOTENCY_KEY"; type = "S" }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  point_in_time_recovery { enabled = true }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  tags = merge(local.common_tags, { Table = "idempotency_ledger" })
}
