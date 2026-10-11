variable "aws_region" {
  description = "AWS region for the data lake and analytics services."
  type        = string
  default     = "ap-south-1"
}

variable "project_name" {
  description = "Lowercase prefix used in resource names."
  type        = string
  default     = "cloud-event-analytics"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,22}$", var.project_name))
    error_message = "Use 3-23 lowercase letters, digits, and hyphens; begin with a letter."
  }
}

variable "enable_daily_schedule" {
  description = "Create an 08:00 UTC daily Glue trigger. Scheduled runs incur AWS charges."
  type        = bool
  default     = false
}

variable "max_reject_rate" {
  description = "Fail a Glue run if the rejected-row ratio exceeds this value."
  type        = number
  default     = 0.05

  validation {
    condition     = var.max_reject_rate >= 0 && var.max_reject_rate <= 1
    error_message = "max_reject_rate must be between 0 and 1."
  }
}
