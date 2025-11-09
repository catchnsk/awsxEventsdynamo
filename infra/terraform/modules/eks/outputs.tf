output "cluster_name" {
  value = module.eks.cluster_name
}

output "cluster_oidc_issuer_url" {
  value = module.eks.oidc_provider
}

output "cluster_primary_security_group_id" {
  value = module.eks.cluster_security_group_id
}
