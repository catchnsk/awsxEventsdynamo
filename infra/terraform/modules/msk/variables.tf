variable "name_prefix" {
  description = "Prefix for MSK cluster names."
  type        = string
}

variable "vpc_id" {
  description = "VPC for security group."
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs for MSK brokers."
  type        = list(string)
}

variable "allowed_security_group_ids" {
  description = "Security group IDs allowed to connect (EKS worker nodes, Lambda ENIs)."
  type        = list(string)
}

variable "kms_key_arn" {
  description = "KMS key for MSK encryption at rest."
  type        = string
}

variable "broker_instance_type" {
  description = "Instance type for brokers."
  type        = string
  default     = "kafka.m5.large"
}

variable "number_of_broker_nodes" {
  description = "Number of brokers."
  type        = number
  default     = 3
}

variable "ebs_volume_size" {
  description = "EBS volume size for brokers (GiB)."
  type        = number
  default     = 1000
}

variable "kafka_version" {
  description = "Kafka version."
  type        = string
  default     = "3.6.0"
}

variable "log_retention_days" {
  description = "CloudWatch log retention."
  type        = number
  default     = 30
}

variable "tags" {
  description = "Common tags."
  type        = map(string)
  default     = {}
}
