output "kms_keys" {
  value = { for k, v in aws_kms_key.this : k => v.arn }
}

output "secrets" {
  value = {
    partner_credentials = aws_secretsmanager_secret.partner_credentials.arn
    webhook_signing     = aws_secretsmanager_secret.webhook_signing.arn
  }
}
