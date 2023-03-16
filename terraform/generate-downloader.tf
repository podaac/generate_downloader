# ECR
resource "aws_ecr_repository" "downloader" {
  name = "${var.prefix}-downloader"
  image_scanning_configuration {
    scan_on_push = true
  }
  image_tag_mutability = "MUTABLE"
  encryption_configuration {
    encryption_type = "AES256"
  }
}

# Job Definition
resource "aws_batch_job_definition" "generate_batch_jd_downloader" {
  name                  = "${var.prefix}-downloader"
  type                  = "container"
  container_properties  = <<CONTAINER_PROPERTIES
  {
    "image": "${aws_ecr_repository.downloader.repository_url}:latest",
    "jobRoleArn": "${aws_iam_role.batch_job_role_downloader.arn}",
    "logConfiguration": {
        "logDriver" : "awslogs",
        "options": {
            "awslogs-group" : "${data.aws_cloudwatch_log_group.cw_log_group.name}"
        }
    },
    "environment" : [
      { "name" : "AWS_DEFAULT_REGION", "value" : "us-west-2" }
    ],
    "mountPoints": [
        {
            "sourceVolume": "downloader",
            "containerPath": "/data",
            "readOnly": false
        },
        {
            "sourceVolume": "combiner",
            "containerPath": "/data/output",
            "readOnly": false
        }
    ],
    "resourceRequirements" : [
        { "type": "MEMORY", "value": "1024"},
        { "type": "VCPU", "value": "1024" }
    ],
    "volumes": [
        {
            "name": "downloader",
            "efsVolumeConfiguration": {
            "fileSystemId": "${data.aws_efs_file_system.aws_efs_generate.file_system_id}",
            "rootDirectory": "/downloader"
            }
        },
        {
            "name": "combiner",
            "efsVolumeConfiguration": {
            "fileSystemId": "${data.aws_efs_file_system.aws_efs_generate.file_system_id}",
            "rootDirectory": "/combiner/downloads"
            }
        }
    ]
  }
  CONTAINER_PROPERTIES
  platform_capabilities = ["EC2"]
  propagate_tags        = true
  retry_strategy {
    attempts = 3
  }
  timeout {
    attempt_duration_seconds = 86400
  }
}

# AWS SSM Parameter Store EDL
resource "aws_ssm_parameter" "aws_ssm_parameter_edl_username" {
  name        = "generate-edl-username"
  description = "Earthdata Login username"
  type        = "SecureString"
  value       = var.edl_username
}

# MODIS Terra
resource "aws_ssm_parameter" "aws_ssm_parameter_edl_password" {
  name        = "generate-edl-password"
  description = "Earthdata Login password"
  type        = "SecureString"
  value       = var.edl_password
}

# AWS Batch downloader job role
resource "aws_iam_role" "batch_job_role_downloader" {
  name = "${var.prefix}-batch-job-role-downloader"
  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "ecs-tasks.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })
  permissions_boundary = "arn:aws:iam::${local.account_id}:policy/NGAPShRoleBoundary"
}

resource "aws_iam_role_policy_attachment" "batch_job_role_policy_attach_downloader_all" {
  role       = aws_iam_role.batch_job_role_downloader.name
  policy_arn = data.aws_iam_policy.aws_batch_job_policy.arn
}

resource "aws_iam_role_policy_attachment" "batch_job_role_policy_attach_downloader_ssm" {
  role       = aws_iam_role.batch_job_role_downloader.name
  policy_arn = aws_iam_policy.batch_job_role_policy_downloader.arn
}

resource "aws_iam_policy" "batch_job_role_policy_downloader" {
  name        = "${var.prefix}-batch-job-policy-downloader"
  description = "Amazon EC2 Role policy for Amazon EC2 Container Service"
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "ssm:GetParameter"
        ],
        "Resource" : [ 
          "${aws_ssm_parameter.aws_ssm_parameter_edl_username.arn}",
          "${aws_ssm_parameter.aws_ssm_parameter_edl_password.arn}"
        ]
      }
    ]
  })
}