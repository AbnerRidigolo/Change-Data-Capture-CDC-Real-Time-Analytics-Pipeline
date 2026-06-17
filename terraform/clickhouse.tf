# ClickHouse OLAP Instance Configuration (EC2 with ClickHouse installation)

resource "aws_security_group" "clickhouse" {
  name        = "clickhouse-olap-sg"
  description = "Allow inbound connections to ClickHouse OLAP DB"
  vpc_id      = aws_vpc.main.id

  # Public access to HTTP port (secured by credentials in production)
  ingress {
    description = "HTTP Client port"
    from_port   = 8123
    to_port     = 8123
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Native Client port"
    from_port   = 9000
    to_port     = 9000
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  ingress {
    description = "SSH Access"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Limit to your IP in real production
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Find latest Amazon Linux 2 AMI
data "aws_ami" "amazon_linux" {
  most_recent = true
  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }
  owners = ["amazon"]
}

resource "aws_instance" "clickhouse" {
  ami           = data.aws_ami.amazon_linux.id
  instance_type = "t3.medium" # ClickHouse requires at least 4GB of RAM to run comfortably
  subnet_id     = aws_subnet.public_1.id
  vpc_security_group_ids = [aws_security_group.clickhouse.id]
  associate_public_ip_address = true

  # Auto-install ClickHouse Server on startup
  user_data = <<-EOF
              #!/bin/bash
              sudo yum update -y
              # Install Clickhouse repos
              sudo yum install -y yum-utils
              sudo rpm --import https://packages.clickhouse.com/rpm/lts/REPKEY.txt
              sudo yum-config-manager --add-repo https://packages.clickhouse.com/rpm/lts/clickhouse.repo
              # Install server and client packages
              sudo yum install -y clickhouse-server clickhouse-client
              # Enable network access in config.xml
              sudo sed -i 's/<!-- <listen_host>0.0.0.0<\/listen_host> -->/<listen_host>0.0.0.0<\/listen_host>/g' /etc/clickhouse-server/config.xml
              # Start Clickhouse Service
              sudo systemctl start clickhouse-server
              sudo systemctl enable clickhouse-server
              EOF

  tags = {
    Name = "clickhouse-olap-server"
  }
}
