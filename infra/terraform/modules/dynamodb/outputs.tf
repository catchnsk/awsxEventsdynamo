output "table_arns" {
  value = {
    event_schema              = aws_dynamodb_table.event_schema.arn
    partner_event_subscription = aws_dynamodb_table.partner_event_subscription.arn
    event_subscription_kafka   = aws_dynamodb_table.event_subscription_kafka.arn
    event_delivery_status      = aws_dynamodb_table.event_delivery_status.arn
    idempotency_ledger         = aws_dynamodb_table.idempotency_ledger.arn
  }
}
