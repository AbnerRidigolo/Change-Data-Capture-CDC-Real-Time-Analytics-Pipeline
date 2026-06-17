# Terraform Variables Configuration

variable "aws_region" {
  type        = string
  description = "Target AWS Region"
  default     = "us-east-1"
}

variable "environment" {
  type        = string
  description = "Target deployment environment"
  default     = "production"
}

variable "db_password" {
  type        = string
  description = "Password for PostgreSQL database"
  default     = "SuperSecurePassword123!"
  sensitive   = true
}
