terraform {
  backend "s3" {
    encrypt = true
  }
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  default_tags {
    tags = local.default_tags
  }
  ignore_tags {
    key_prefixes = ["gsfc-ngap"]
  }
  region  = var.aws_region
}

# Data sources
data "aws_caller_identity" "current" {}

data "aws_ecr_repository" "downloader" {
  name = "${var.prefix}-downloader"
}

data "aws_efs_file_system" "aws_efs_generate" {
  creation_token = var.prefix
}

data "aws_cloudwatch_log_group" "cw_log_group" {
  name = "/aws/batch/job/${var.prefix}-downloader/"
}

data "aws_iam_policy" "aws_batch_job_policy" {
  name = "${var.prefix}-batch-job-policy"
}

data "aws_security_groups" "vpc_default_sg" {
  filter {
    name   = "group-name"
    values = ["default"]
  }
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.application_vpc.id]
  }
}

data "aws_subnets" "private_application_subnets" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.application_vpc.id]
  }
  filter {
    name   = "tag:Name"
    values = ["Private application*"]
  }
}

data "aws_vpc" "application_vpc" {
  tags = {
    "Name" : "Application VPC"
  }
}

# Local variables
locals {
  account_id = data.aws_caller_identity.current.account_id
  default_tags = length(var.default_tags) == 0 ? {
    application : var.app_name,
    environment : var.environment,
    version : var.app_version
  } : var.default_tags
}