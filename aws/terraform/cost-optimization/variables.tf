# ═══════════════════════════════════════════════════════════
# Variables
# ═══════════════════════════════════════════════════════════

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "ap-southeast-1"
}

variable "instance_id" {
  description = "EC2 instance ID to monitor and schedule"
  type        = string

  validation {
    condition     = can(regex("^i-[a-f0-9]{8,17}$", var.instance_id))
    error_message = "Must be a valid EC2 instance ID (e.g. i-0123456789abcdef0)."
  }
}

variable "cpu_threshold" {
  description = "CPU utilization % below which the instance is considered idle"
  type        = number
  default     = 5

  validation {
    condition     = var.cpu_threshold >= 1 && var.cpu_threshold <= 100
    error_message = "Must be between 1 and 100."
  }
}

variable "idle_period_minutes" {
  description = "Minutes of sustained low CPU before auto-stop (must be multiple of 5)"
  type        = number
  default     = 60

  validation {
    condition     = var.idle_period_minutes >= 5 && var.idle_period_minutes % 5 == 0
    error_message = "Must be >= 5 and a multiple of 5."
  }
}

variable "stop_cron" {
  description = "SSM cron for nightly stop (UTC). Default: 15:00 UTC = 22:00 ICT"
  type        = string
  default     = "cron(0 15 ? * * *)"
}

variable "start_cron" {
  description = "SSM cron for morning start (UTC). Default: 01:00 UTC = 08:00 ICT weekdays"
  type        = string
  default     = "cron(0 1 ? * MON-FRI *)"
}

variable "notification_email" {
  description = "Email for stop/start notifications. Leave empty to skip."
  type        = string
  default     = ""
}

variable "project_tag" {
  description = "Value of the 'Project' tag used to target instances"
  type        = string
  default     = "Spark-ling"
}
