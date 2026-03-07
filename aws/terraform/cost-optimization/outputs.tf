# ═══════════════════════════════════════════════════════════
# Outputs
# ═══════════════════════════════════════════════════════════

output "alarm_arn" {
  description = "ARN of the CloudWatch idle-CPU alarm"
  value       = aws_cloudwatch_metric_alarm.cpu_idle.arn
}

output "alarm_console_url" {
  description = "Direct link to the alarm in CloudWatch console"
  value       = "https://${data.aws_region.current.name}.console.aws.amazon.com/cloudwatch/home?region=${data.aws_region.current.name}#alarmsV2:alarm/${aws_cloudwatch_metric_alarm.cpu_idle.alarm_name}"
}

output "stop_window_id" {
  description = "SSM Maintenance Window ID for nightly stop"
  value       = aws_ssm_maintenance_window.stop.id
}

output "start_window_id" {
  description = "SSM Maintenance Window ID for morning start"
  value       = aws_ssm_maintenance_window.start.id
}

output "sns_topic_arn" {
  description = "SNS topic ARN for idle-instance alerts (empty if notifications disabled)"
  value       = length(aws_sns_topic.idle_alerts) > 0 ? aws_sns_topic.idle_alerts[0].arn : ""
}

output "ssm_console_url" {
  description = "Direct link to Maintenance Windows in SSM console"
  value       = "https://${data.aws_region.current.name}.console.aws.amazon.com/systems-manager/maintenance-windows?region=${data.aws_region.current.name}"
}
