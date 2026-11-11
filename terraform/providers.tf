# Confluent Cloud credentials come from the environment:
#   CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET (a Cloud resource management key).
provider "confluent" {}

# AWS hosts the KMS key used by the CSFLE demo.
provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile

  default_tags {
    tags = var.tags
  }
}

# Azure hosts the Key Vault key used by the CSPE demo.
provider "azurerm" {
  subscription_id = var.azure_subscription_id

  features {
    key_vault {
      # The vault is throwaway demo infrastructure: purge it on destroy so the
      # name can be reused and nothing lingers in the soft-deleted state.
      purge_soft_delete_on_destroy       = true
      recover_soft_deleted_key_vaults    = false
      purge_soft_deleted_keys_on_destroy = true
    }
  }
}

provider "azuread" {}
