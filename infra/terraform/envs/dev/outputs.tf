output "vpc_id" {
  value = module.networking.vpc_id
}

output "msk_cluster_arns" {
  value = module.msk.cluster_arns
}

output "dynamodb_tables" {
  value = module.dynamodb.table_arns
}

output "lambda_role_arns" {
  value = module.lambda_roles.role_arns
}
