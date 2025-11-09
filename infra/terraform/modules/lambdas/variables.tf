variable "name_prefix" {
  description = "Prefix for IAM roles."
  type        = string
}

variable "dynamodb_table_arns" {
  description = "Map of DynamoDB table ARNs used by Lambdas."
  type        = map(string)
}

variable "msk_cluster_arns" {
  description = "MSK cluster ARNs the Lambdas publish/consume."
  type        = list(string)
}

variable "kms_key_arn" {
  description = "KMS key for decrypt/encrypt operations."
  type        = string
}

variable "secrets_arns" {
  description = "List of Secrets Manager ARNs Lambda can read."
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Common tags."
  type        = map(string)
  default     = {}
}
