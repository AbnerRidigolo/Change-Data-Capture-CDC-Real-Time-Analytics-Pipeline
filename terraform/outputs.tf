# Terraform Outputs Configuration

output "vpc_id" {
  value       = aws_vpc.main.id
  description = "Created VPC ID"
}

output "postgres_endpoint" {
  value       = aws_db_instance.postgres.endpoint
  description = "PostgreSQL DB Ingestion connection endpoint"
}

output "kafka_bootstrap_brokers" {
  value       = aws_msk_cluster.kafka.bootstrap_brokers_tls
  description = "Kafka Bootstrap brokers connection string"
}

output "clickhouse_public_ip" {
  value       = aws_instance.clickhouse.public_ip
  description = "Public IP of ClickHouse OLAP server"
}
