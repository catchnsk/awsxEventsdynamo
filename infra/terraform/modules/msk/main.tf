terraform {
  required_version = ">= 1.6.0"
}

locals {
  clusters = {
    ingress = "${var.name_prefix}-ingress"
    egress  = "${var.name_prefix}-egress"
  }
}

resource "aws_security_group" "msk" {
  name        = "${var.name_prefix}-msk-sg"
  description = "Allows EKS + Lambda ENIs to reach MSK brokers"
  vpc_id      = var.vpc_id

  ingress {
    description      = "Kafka TLS"
    from_port        = 9098
    to_port          = 9098
    protocol         = "tcp"
    security_groups  = var.allowed_security_group_ids
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = var.tags
}

resource "aws_cloudwatch_log_group" "this" {
  for_each = local.clusters
  name              = "/aws/msk/${each.value}"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_msk_cluster" "this" {
  for_each           = local.clusters
  cluster_name       = each.value
  kafka_version      = var.kafka_version
  number_of_broker_nodes = var.number_of_broker_nodes

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = var.subnet_ids
    security_groups = [aws_security_group.msk.id]
    storage_info {
      ebs_storage_info {
        volume_size = var.ebs_volume_size
      }
    }
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = var.kms_key_arn
  }

  client_authentication {
    sasl {
      iam = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.this[each.key].name
      }
    }
  }

  open_monitoring {
    prometheus {
      jmx_exporter { enabled_in_broker = true }
      node_exporter { enabled_in_broker = true }
    }
  }

  tags = merge(var.tags, { Cluster = each.key })
}
