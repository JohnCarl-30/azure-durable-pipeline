variable "project" {
  description = "Short project slug used to name every resource."
  type        = string
  default     = "dirpipe"

  validation {
    # Storage account names are 3-24 chars, lowercase alphanumeric only, and
    # globally unique. Everything else is derived from this, so constrain it
    # here rather than discovering it on apply.
    condition     = can(regex("^[a-z0-9]{3,12}$", var.project))
    error_message = "project must be 3-12 lowercase alphanumeric characters."
  }
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "location" {
  description = "Azure region."
  type        = string
  default     = "uksouth"
}

variable "directory_base_url" {
  description = "Upstream directory to crawl."
  type        = string
  default     = "https://example.invalid"
}

variable "enrichment_base_url" {
  description = "Third-party enrichment API base URL."
  type        = string
  default     = "https://example.invalid"
}

variable "enrichment_api_key" {
  description = "Enrichment API key. Stored in Key Vault, never in app settings."
  type        = string
  sensitive   = true
  default     = ""
}

variable "log_retention_days" {
  description = "Log Analytics retention. Cost scales with this."
  type        = number
  default     = 30

  validation {
    condition     = var.log_retention_days >= 30 && var.log_retention_days <= 730
    error_message = "log_retention_days must be between 30 and 730."
  }
}

variable "maximum_instance_count" {
  description = "Upper bound on Flex Consumption scale-out. The cost ceiling."
  type        = number
  default     = 40
}

variable "tags" {
  description = "Additional tags merged onto every resource."
  type        = map(string)
  default     = {}
}
