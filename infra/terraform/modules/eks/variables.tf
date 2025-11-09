variable "cluster_name" {
  description = "EKS cluster name."
  type        = string
}

variable "cluster_version" {
  description = "EKS version."
  type        = string
  default     = "1.29"
}

variable "vpc_id" {
  description = "VPC ID."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for worker nodes & control plane."
  type        = list(string)
}

variable "managed_node_groups" {
  description = "Map of managed node group definitions."
  type        = any
  default = {
    default = {
      min_size     = 2
      max_size     = 6
      desired_size = 3
    }
  }
}

variable "cluster_addons" {
  description = "Cluster addons map."
  type        = map(any)
  default = {
    coredns = { most_recent = true }
    kube-proxy = { most_recent = true }
    vpc-cni = {
      most_recent = true
      configuration_values = jsonencode({
        env = {
          ENABLE_PREFIX_DELEGATION = "true"
        }
      })
    }
  }
}

variable "tags" {
  description = "Common tags."
  type        = map(string)
  default     = {}
}
