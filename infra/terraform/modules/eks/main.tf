terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.13"

  cluster_name    = var.cluster_name
  cluster_version = var.cluster_version
  vpc_id          = var.vpc_id
  subnet_ids      = var.private_subnet_ids

  enable_irsa                      = true
  cluster_endpoint_public_access   = false
  cluster_endpoint_private_access  = true
  cluster_addons                   = var.cluster_addons
  tags                             = var.tags

  eks_managed_node_group_defaults = {
    ami_type       = "AL2_x86_64"
    disk_size      = 50
    instance_types = ["m6i.large"]
  }

  eks_managed_node_groups = var.managed_node_groups
}
