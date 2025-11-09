variable "name_prefix" {
  description = "Prefix for security resources."
  type        = string
}

variable "environment" {
  description = "Environment name (dev/stage/prod)."
  type        = string
}

variable "tags" {
  description = "Common tags."
  type        = map(string)
  default     = {}
}

variable "kms_policy_json" {
  description = "Optional custom IAM policy JSON applied to KMS keys."
  type        = string
  default     = null
}
