variable "prefix" {
  description = "Prefix for every resource name (Confluent, AWS, Azure). Use your name or handle, e.g. \"jdoe\"."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9]{1,11}$", var.prefix))
    error_message = "prefix must be 2-12 lowercase letters/digits starting with a letter (Azure Key Vault names are capped at 24 chars)."
  }
}

variable "enable_csfle" {
  description = "Deploy the CSFLE demo (field-level encryption, AWS KMS)."
  type        = bool
  default     = true
}

variable "enable_cspe" {
  description = "Deploy the CSPE demo (payload encryption, Azure Key Vault)."
  type        = bool
  default     = true
}

# ---------------------------------------------------------------- Confluent

variable "confluent_cloud" {
  description = "Cloud provider hosting the Kafka cluster (AWS, AZURE or GCP)."
  type        = string
  default     = "AWS"
}

variable "confluent_region" {
  description = "Region of the Kafka cluster."
  type        = string
  default     = "eu-central-1"
}

# ---------------------------------------------------------------------- AWS

variable "aws_region" {
  description = "AWS region for the KMS key (CSFLE demo)."
  type        = string
  default     = "eu-central-1"
}

variable "aws_profile" {
  description = "AWS CLI profile. null = default credential chain (AWS_PROFILE, env vars, SSO...)."
  type        = string
  default     = null
}

# -------------------------------------------------------------------- Azure

variable "azure_subscription_id" {
  description = "Azure subscription for the Key Vault (CSPE demo). null = ARM_SUBSCRIPTION_ID / az CLI default."
  type        = string
  default     = null
}

variable "azure_location" {
  description = "Azure region for the Key Vault (CSPE demo)."
  type        = string
  default     = "westeurope"
}

# --------------------------------------------------------------------- Tags

variable "tags" {
  description = "Tags applied to every AWS and Azure resource (Confluent Cloud resources do not support tags)."
  type        = map(string)
  default     = {}
}
