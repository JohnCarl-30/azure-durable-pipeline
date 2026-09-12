terraform {
  required_version = ">= 1.9"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.14"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # State holds secrets and must never be local for anything shared. Configured
  # via `terraform init -backend-config=...` so the backend storage account can
  # differ per environment without editing this file.
  backend "azurerm" {}
}

provider "azurerm" {
  features {
    key_vault {
      # Soft-delete recovery is on by default; purge protection means a
      # destroyed vault is recoverable rather than gone. Keep it.
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }
  }
}
