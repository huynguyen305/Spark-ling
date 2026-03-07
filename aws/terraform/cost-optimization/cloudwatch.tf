# ═══════════════════════════════════════════════════════════
# Layer 1 — CloudWatch Alarm (activity-based auto-stop)
# ═══════════════════════════════════════════════════════════

locals {
  evaluation_periods = var.idle_period_minutes / 5
  has_notification   = var.notification_email != ""
}

# ── SNS Topic (optional) ──────────────────────────────────
resource "aws_sns_topic" "idle_alerts" {
  count        = local.has_notification ? 1 : 0
  name         = "sparkling-idle-instance-alerts"
  display_name = "Spark-ling Idle Instance Alerts"

  tags = {
    Project = var.project_tag
  }
}

resource "aws_sns_topic_subscription" "email" {
  count     = local.has_notification ? 1 : 0
  topic_arn = aws_sns_topic.idle_alerts[0].arn
  protocol  = "email"
  endpoint  = var.notification_email
}

# ── CloudWatch Metric Alarm ───────────────────────────────
resource "aws_cloudwatch_metric_alarm" "cpu_idle" {
  alarm_name        = "sparkling-idle-stop-${var.instance_id}"
  alarm_description = <<-EOT
    Stops EC2 instance ${var.instance_id} when average CPU utilization
    stays below ${var.cpu_threshold}% for ${var.idle_period_minutes} minutes.
  EOT

  namespace           = "AWS/EC2"
  metric_name         = "CPUUtilization"
  statistic           = "Average"
  period              = 300 # 5-minute data points
  evaluation_periods  = local.evaluation_periods
  threshold           = var.cpu_threshold
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    InstanceId = var.instance_id
  }

  # Native EC2 stop action — no Lambda needed
  alarm_actions = concat(
    ["arn:aws:automate:${data.aws_region.current.name}:ec2:stop"],
    local.has_notification ? [aws_sns_topic.idle_alerts[0].arn] : []
  )

  ok_actions = local.has_notification ? [aws_sns_topic.idle_alerts[0].arn] : []

  tags = {
    Project = var.project_tag
  }
}
