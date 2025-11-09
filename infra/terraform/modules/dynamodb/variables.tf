variable "name_prefix" {
  description = "Prefix for DynamoDB table names."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key ARN for SSE."
  type        = string
}

variable "tags" {
  description = "Common tags."
  type        = map(string)
  default     = {}
}
