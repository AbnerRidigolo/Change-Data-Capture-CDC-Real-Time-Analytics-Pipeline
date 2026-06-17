# AWS RDS PostgreSQL Configuration (Source OLTP DB with CDC logical replication enabled)

resource "aws_db_subnet_group" "postgres" {
  name       = "postgres-subnet-group"
  subnet_ids = [aws_subnet.private_1.id, aws_subnet.private_2.id]
  tags       = { Name = "postgres-subnet-group" }
}

# DB Parameter Group is required to enable logical replication on AWS RDS Postgres
resource "aws_db_parameter_group" "postgres_cdc" {
  name   = "postgres15-cdc-params"
  family = "postgres15"

  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "postgres" {
  name        = "rds-postgres-sg"
  description = "Allow inbound PostgreSQL connections from private subnet"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Allow port 5432"
    from_port   = 5432
    to_port     = 5432
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

resource "aws_db_instance" "postgres" {
  identifier             = "ecom-transactional-db"
  allocated_storage      = 20
  max_allocated_storage  = 100
  engine                 = "postgres"
  engine_version         = "15.4"
  instance_class         = "db.t4g.micro" # Cost-effective instance for small-mid workloads
  db_name                = "postgres"
  username               = "postgres"
  password               = var.db_password
  db_subnet_group_name   = aws_db_subnet_group.postgres.name
  parameter_group_name   = aws_db_parameter_group.postgres_cdc.name
  vpc_security_group_ids = [aws_security_group.postgres.id]
  skip_final_snapshot    = true
  publicLY_accessible    = false

  # CRITICAL: Backups must be enabled (retention > 0) to support logical replication
  backup_retention_period = 7
  backup_window           = "03:00-04:00"

  tags = {
    Name = "ecom-postgres-source"
  }
}
