terraform { # This is the initial process that is required as a provider configuration
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=3.0.0"
    }
  }
}

provider "azurerm" { # Provider block to initial the interaction with Azure
  features {}
}