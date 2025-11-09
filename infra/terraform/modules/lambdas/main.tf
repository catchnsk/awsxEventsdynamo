terraform {
  required_version = ">= 1.6.0"
}

locals {
  functions = {
    schema_admin = {
      description = "Registers and versions event schemas"
      dynamodb_actions = [
        "dynamodb:PutItem",
        "dynamodb:UpdateItem"
      ]
    }
    subscription_admin = {
      description = "Creates partner subscriptions and egress topics"
      dynamodb_actions = [
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:GetItem"
      ]
    }
    event_processor = {
      description = "Consumes ingress topics, enriches, and republishes"
      dynamodb_actions = [
        "dynamodb:GetItem",
        "dynamodb:UpdateItem"
      ]
    }
    webhook_dispatcher = {
      description = "Delivers events to partner endpoints with retry"
      dynamodb_actions = [
        "dynamodb:GetItem",
        "dynamodb:UpdateItem",
        "dynamodb:PutItem"
      ]
    }
  }
}

data "aws_iam_policy_document" "lambda_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  for_each           = local.functions
  name               = "${var.name_prefix}-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
  description        = each.value.description
  tags               = var.tags
}

resource "aws_iam_role_policy" "lambda" {
  for_each = local.functions
  name     = "${each.key}-policy"
  role     = aws_iam_role.lambda[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Logs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      },
      {
        Sid    = "DynamoAccess"
        Effect = "Allow"
        Action = each.value.dynamodb_actions
        Resource = values(var.dynamodb_table_arns)
      },
      {
        Sid    = "KafkaAccess"
        Effect = "Allow"
        Action = [
          "kafka:DescribeCluster",
          "kafka:GetBootstrapBrokers",
          "kafka-cluster:*"
        ]
        Resource = var.msk_cluster_arns
      },
      {
        Sid    = "SecretsAccess"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "kms:Decrypt",
          "kms:Encrypt"
        ]
        Resource = concat(var.secrets_arns, [var.kms_key_arn])
      }
    ]
  })
}
