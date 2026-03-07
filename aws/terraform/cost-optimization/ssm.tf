# ═══════════════════════════════════════════════════════════
# Layer 2 — SSM Maintenance Windows (schedule-based)
# ═══════════════════════════════════════════════════════════

# ── IAM Role for SSM Automation ────────────────────────────
resource "aws_iam_role" "ssm_automation" {
  name = "sparkling-ssm-cost-opt-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ssm.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Project = var.project_tag
  }
}

resource "aws_iam_role_policy" "ec2_stop_start" {
  name = "sparkling-ec2-stop-start"
  role = aws_iam_role.ssm_automation.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ec2:StopInstances",
          "ec2:StartInstances"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "aws:ResourceTag/Project" = var.project_tag
          }
        }
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:DescribeInstances",
          "ec2:DescribeInstanceStatus"
        ]
        Resource = "*"
      }
    ]
  })
}

# ── Nightly Stop Window ────────────────────────────────────
resource "aws_ssm_maintenance_window" "stop" {
  name              = "sparkling-nightly-stop"
  description       = "Automatically stop Spark-ling EC2 instances at night"
  schedule          = var.stop_cron
  schedule_timezone = "UTC"
  duration          = 1
  cutoff            = 0

  allow_unassociated_targets = false

  tags = {
    Project = var.project_tag
  }
}

resource "aws_ssm_maintenance_window_target" "stop" {
  window_id     = aws_ssm_maintenance_window.stop.id
  resource_type = "INSTANCE"
  name          = "sparkling-stop-target"
  description   = "Target instance for nightly stop"

  targets {
    key    = "InstanceIds"
    values = [var.instance_id]
  }
}

resource "aws_ssm_maintenance_window_task" "stop" {
  window_id        = aws_ssm_maintenance_window.stop.id
  task_type        = "AUTOMATION"
  task_arn         = "AWS-StopEC2Instance"
  service_role_arn = aws_iam_role.ssm_automation.arn
  max_concurrency  = "1"
  max_errors       = "1"
  priority         = 1

  targets {
    key    = "WindowTargetIds"
    values = [aws_ssm_maintenance_window_target.stop.id]
  }

  task_invocation_parameters {
    automation_parameters {
      parameter {
        name   = "InstanceId"
        values = ["{{TARGET_ID}}"]
      }
    }
  }
}

# ── Morning Start Window ──────────────────────────────────
resource "aws_ssm_maintenance_window" "start" {
  name              = "sparkling-morning-start"
  description       = "Automatically start Spark-ling EC2 instances in the morning (weekdays)"
  schedule          = var.start_cron
  schedule_timezone = "UTC"
  duration          = 1
  cutoff            = 0

  allow_unassociated_targets = false

  tags = {
    Project = var.project_tag
  }
}

resource "aws_ssm_maintenance_window_target" "start" {
  window_id     = aws_ssm_maintenance_window.start.id
  resource_type = "INSTANCE"
  name          = "sparkling-start-target"
  description   = "Target instance for morning start"

  targets {
    key    = "InstanceIds"
    values = [var.instance_id]
  }
}

resource "aws_ssm_maintenance_window_task" "start" {
  window_id        = aws_ssm_maintenance_window.start.id
  task_type        = "AUTOMATION"
  task_arn         = "AWS-StartEC2Instance"
  service_role_arn = aws_iam_role.ssm_automation.arn
  max_concurrency  = "1"
  max_errors       = "1"
  priority         = 1

  targets {
    key    = "WindowTargetIds"
    values = [aws_ssm_maintenance_window_target.start.id]
  }

  task_invocation_parameters {
    automation_parameters {
      parameter {
        name   = "InstanceId"
        values = ["{{TARGET_ID}}"]
      }
    }
  }
}
