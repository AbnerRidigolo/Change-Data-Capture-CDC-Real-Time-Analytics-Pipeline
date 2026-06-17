# AWS Managed Streaming for Kafka (MSK) Configuration

resource "aws_security_group" "msk" {
  name        = "msk-kafka-sg"
  description = "Allow inbound Kafka broker traffic within VPC"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Allow Plaintext and TLS brokers"
    from_port   = 9092
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_kms_key" "msk_key" {
  description = "KMS Key for MSK storage encryption"
}

resource "aws_msk_cluster" "kafka" {
  cluster_name           = "cdc-pipeline-kafka"
  kafka_version          = "3.4.0"
  number_of_broker_nodes = 2

  broker_node_group_info {
    instance_type = "kafka.t3.small" # Cost-effective instance type
    client_subnets = [
      aws_subnet.private_1.id,
      aws_subnet.private_2.id
    ]
    security_groups = [aws_security_group.msk.id]
    storage_info {
      ebs_storage_info {
        volume_size = 50
      }
    }
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = aws_kms_key.msk_key.arn
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
      in_cluster    = true
    }
  }

  tags = {
    Environment = var.environment
  }
}
