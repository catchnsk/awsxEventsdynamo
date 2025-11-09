locals {
  tags = var.default_tags
}

module "networking" {
  source = "../../modules/networking"

  name                = var.name_prefix
  region              = var.aws_region
  cidr_block          = var.vpc_cidr
  availability_zones  = var.availability_zones
  public_subnet_cidrs = var.public_subnet_cidrs
  private_subnet_cidrs = var.private_subnet_cidrs
  enable_nat          = true
  tags                = local.tags
}

module "security" {
  source      = "../../modules/security"
  name_prefix = var.name_prefix
  environment = "dev"
  tags        = local.tags
}

module "dynamodb" {
  source      = "../../modules/dynamodb"
  name_prefix = var.name_prefix
  kms_key_arn = module.security.kms_keys["dynamodb"]
  tags        = local.tags
}

module "eks" {
  source              = "../../modules/eks"
  cluster_name        = "${var.name_prefix}-eks"
  cluster_version     = "1.29"
  vpc_id              = module.networking.vpc_id
  private_subnet_ids  = module.networking.private_subnet_ids
  managed_node_groups = {
    default = {
      min_size     = 3
      max_size     = 6
      desired_size = 3
    }
  }
  tags = local.tags
}

module "msk" {
  source                    = "../../modules/msk"
  name_prefix               = var.name_prefix
  vpc_id                    = module.networking.vpc_id
  subnet_ids                = module.networking.private_subnet_ids
  allowed_security_group_ids = [module.eks.cluster_primary_security_group_id]
  kms_key_arn               = module.security.kms_keys["msk"]
  broker_instance_type      = var.msk_broker_instance_type
  number_of_broker_nodes    = var.msk_broker_count
  ebs_volume_size           = var.msk_volume_size
  tags                      = local.tags
}

module "lambda_roles" {
  source               = "../../modules/lambdas"
  name_prefix          = var.name_prefix
  dynamodb_table_arns  = module.dynamodb.table_arns
  msk_cluster_arns     = values(module.msk.cluster_arns)
  kms_key_arn          = module.security.kms_keys["secrets"]
  secrets_arns         = values(module.security.secrets)
  tags                 = local.tags
}
