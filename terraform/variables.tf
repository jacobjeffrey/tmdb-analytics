variable "project_id" {
  description = "The ID of the GCP project"
  type        = string
}

variable "region" {
  description = "The default GCP region for resources"
  type        = string
  default     = "us-west1" # Defaults let you skip defining this every time
}

variable "dev_user_email" {
  description = "Personal email for local development permissions (ADC)"
  type        = string
}