data "azurerm_resource_group" "DataCohortLab" { # Store the resource group that has already been created within Azure
  name = "rg-data-cohort-labs"
}

data "azurerm_storage_account" "WorkSpaceLabs" {
    name = "datacohortworkspacelabs"
    resource_group_name = "rg-data-cohort-labs"
}

resource "azurerm_storage_container" "Landing1" { # Creating the Bronze container within the storage account akstrgccntqualyfi
  name                 = "landing-artin"
  storage_account_name = data.azurerm_storage_account.WorkSpaceLabs.name
}

resource "azurerm_storage_container" "Bronze1" { # Creating the Bronze container within the storage account akstrgccntqualyfi
  name                 = "bronze-artin"
  storage_account_name = data.azurerm_storage_account.WorkSpaceLabs.name
}

resource "azurerm_storage_container" "Silver1" { # Creating the Silver container within the storage account akstrgccntqualyfi
  name                 = "silver-artin"
  storage_account_name = data.azurerm_storage_account.WorkSpaceLabs.name
}

resource "azurerm_storage_container" "Gold1" { # Creating the Gold container within the storage account akstrgccntqualyfi
  name                 = "gold-artin"
  storage_account_name = data.azurerm_storage_account.WorkSpaceLabs.name
}

