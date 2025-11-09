variable "aws_region" {
  type        = string
  description = "AWS region."
  default     = "us-east-1"
}

variable "name_prefix" {
  type        = string
  description = "Prefix for resource naming."
  default     = "webhooks-dev"
}

variable "vpc_cidr" {
  type        = string
  description = "CIDR for VPC."
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  type        = list(string)
  description = "AZs used for subnets."
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

variable "public_subnet_cidrs" {
  type        = list(string)
  description = "Public subnet CIDRs."
  default     = ["10.0.0.0/24", "10.0.1.0/24", "10.0.2.0/24"]
}

variable "private_subnet_cidrs" {
  type        = list(string)
  description = "Private subnet CIDRs."
  default     = ["10.0.10.0/24", "10.0.11.0/24", "10.0.12.0/24"]
}

variable "default_tags" {
  type = map(string)
  description = "Tags applied to resources."
  default = {
    Project     = "webhooks-platform"
    Environment = "dev"
    ManagedBy   = "terraform"
  }
}

variable "msk_broker_instance_type" {
  type        = string
  description = "MSK broker instance type."
  default     = "kafka.m5.large"
}

variable "msk_broker_count" {
  type        = number
  description = "Number of brokers per cluster."
  default     = 3
}

variable "msk_volume_size" {
  type        = number
  description = "Broker EBS size."
  default     = 1000
}
