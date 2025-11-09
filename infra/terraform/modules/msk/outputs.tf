output "cluster_arns" {
  value = { for k, v in aws_msk_cluster.this : k => v.arn }
}

output "security_group_id" {
  value = aws_security_group.msk.id
}
