variable "name" {
  description = "Name prefix for resources."
  type        = string
}

variable "region" {
  description = "AWS region for endpoint services."
  type        = string
}

variable "cidr_block" {
  description = "CIDR block for VPC."
  type        = string
}

variable "availability_zones" {
  description = "List of AZs for subnets (matching subnet CIDR counts)."
  type        = list(string)
}

variable "public_subnet_cidrs" {
  description = "CIDRs for public subnets."
  type        = list(string)
}

variable "private_subnet_cidrs" {
  description = "CIDRs for private subnets."
  type        = list(string)
}

variable "enable_nat" {
  description = "Whether to create a NAT gateway."
  type        = bool
  default     = true
}

variable "gateway_endpoints" {
  description = "Gateway endpoint services."
  type        = list(string)
  default     = ["s3", "dynamodb"]
}

variable "interface_endpoints" {
  description = "Interface endpoint services."
  type        = list(string)
  default = [
    "logs",
    "sts",
    "secretsmanager",
    "kms",
    "ecr.api",
    "ecr.dkr",
    "appconfig"
  ]
}

variable "tags" {
  description = "Common tags."
  type        = map(string)
  default     = {}
}
