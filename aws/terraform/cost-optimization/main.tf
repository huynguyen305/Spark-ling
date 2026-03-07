# ═══════════════════════════════════════════════════════════
# Spark-ling — EC2 Cost Optimization (Terraform)
# ═══════════════════════════════════════════════════════════
# Usage:
#   cd aws/terraform/cost-optimization
#   terraform init
#   terraform plan -var="instance_id=i-0123456789abcdef0"
#   terraform apply -var="instance_id=i-0123456789abcdef0"
# ═══════════════════════════════════════════════════════════

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Look up current account and region
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
