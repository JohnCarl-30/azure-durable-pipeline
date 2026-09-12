# Directory enrichment pipeline on Azure Durable Functions.
#
# The security posture is the part worth reading: there are **no connection
# strings in app settings**. The Function App authenticates to Storage and
# Service Bus with a system-assigned managed identity, and the only secret in
# the system (the third-party API key) lives in Key Vault and is referenced
# rather than copied. A leaked app-settings dump therefore yields nothing.

locals {
  # Deterministic, collision-resistant naming. Storage account names are
  # globally unique across all of Azure, so a random suffix is not optional.
  suffix = random_string.suffix.result
  name   = "${var.project}-${var.environment}"

  tags = merge(
    {
      project     = var.project
      environment = var.environment
      managed_by  = "terraform"
      component   = "durable-pipeline"
    },
    var.tags
  )
}

resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

resource "azurerm_resource_group" "main" {
  name     = "rg-${local.name}"
  location = var.location
  tags     = local.tags
}

# --- Observability -----------------------------------------------------------
# Created first: everything else sends diagnostics here, so it has to exist
# before the resources that reference it.

resource "azurerm_log_analytics_workspace" "main" {
  name                = "log-${local.name}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = var.log_retention_days
  tags                = local.tags
}

resource "azurerm_application_insights" "main" {
  name                = "appi-${local.name}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  # Workspace-based rather than classic: classic App Insights is retired.
  workspace_id     = azurerm_log_analytics_workspace.main.id
  application_type = "web"
  tags             = local.tags
}

# --- Storage -----------------------------------------------------------------
# Durable Functions keeps its orchestration history here. Losing this account
# loses every in-flight orchestration, which is why it is not shared with
# anything else and why soft delete is enabled.

resource "azurerm_storage_account" "main" {
  name                = "st${var.project}${var.environment}${local.suffix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  account_tier             = "Standard"
  account_replication_type = var.environment == "prod" ? "ZRS" : "LRS"
  account_kind             = "StorageV2"

  # Refuse plaintext and legacy TLS.
  https_traffic_only_enabled      = true
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = true # required by the Durable extension

  blob_properties {
    delete_retention_policy {
      days = 7
    }
    container_delete_retention_policy {
      days = 7
    }
  }

  tags = local.tags
}

resource "azurerm_storage_container" "artifacts" {
  name                  = "artifacts"
  storage_account_id    = azurerm_storage_account.main.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "deployments" {
  name                  = "deployments"
  storage_account_id    = azurerm_storage_account.main.id
  container_access_type = "private"
}

resource "azurerm_storage_table" "records" {
  name                 = "records"
  storage_account_name = azurerm_storage_account.main.name
}

# --- Service Bus -------------------------------------------------------------
# Queue-triggered ingestion. Sessions are off: this workload has no per-entity
# ordering requirement, and sessions serialise consumption within a session id.

resource "azurerm_servicebus_namespace" "main" {
  name                = "sb-${local.name}-${local.suffix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "Standard"
  tags                = local.tags
}

resource "azurerm_servicebus_queue" "ingest" {
  name         = "ingest-requests"
  namespace_id = azurerm_servicebus_namespace.main.id

  max_delivery_count = 5
  # After five failed deliveries the message goes to the dead-letter queue
  # rather than looping forever. Without this a poison message is infinite.
  dead_lettering_on_message_expiration    = true
  default_message_ttl                     = "P14D"
  lock_duration                           = "PT5M"
  duplicate_detection_history_time_window = "PT30M"
  requires_duplicate_detection            = true
}

# --- Key Vault ---------------------------------------------------------------

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "main" {
  name                = "kv-${var.project}${var.environment}${local.suffix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  # RBAC rather than access policies: policies are per-vault ACLs that drift,
  # RBAC is the same model as everything else in the subscription.
  rbac_authorization_enabled = true
  purge_protection_enabled   = var.environment == "prod"
  soft_delete_retention_days = 7

  tags = local.tags
}

resource "azurerm_key_vault_secret" "enrichment_api_key" {
  count = var.enrichment_api_key == "" ? 0 : 1

  name         = "enrichment-api-key"
  value        = var.enrichment_api_key
  key_vault_id = azurerm_key_vault.main.id

  depends_on = [azurerm_role_assignment.deployer_kv_admin]
}

# The identity running Terraform needs vault data-plane access to write the
# secret. RBAC propagation is eventually consistent, hence the explicit
# dependency above rather than relying on graph ordering alone.
resource "azurerm_role_assignment" "deployer_kv_admin" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}

# --- Function App ------------------------------------------------------------

resource "azurerm_service_plan" "main" {
  name                = "asp-${local.name}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  os_type             = "Linux"
  # FC1 is Flex Consumption: per-instance concurrency control and no cold-start
  # cliff at scale, unlike the older Y1 consumption plan.
  sku_name = "FC1"
  tags     = local.tags
}

resource "azurerm_function_app_flex_consumption" "main" {
  name                = "func-${local.name}-${local.suffix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  service_plan_id     = azurerm_service_plan.main.id

  storage_container_type      = "blobContainer"
  storage_container_endpoint  = "${azurerm_storage_account.main.primary_blob_endpoint}${azurerm_storage_container.deployments.name}"
  storage_authentication_type = "SystemAssignedIdentity"

  runtime_name    = "python"
  runtime_version = "3.12"

  maximum_instance_count = var.maximum_instance_count
  instance_memory_in_mb  = 2048

  site_config {
    application_insights_connection_string = azurerm_application_insights.main.connection_string
    application_insights_key               = azurerm_application_insights.main.instrumentation_key
  }

  app_settings = {
    # Identity-based, not a connection string. The `__` suffixes are the
    # binding extension's convention for identity configuration.
    "AzureWebJobsStorage__accountName"              = azurerm_storage_account.main.name
    "AzureWebJobsStorage__credential"               = "managedidentity"
    "ServiceBusConnection__fullyQualifiedNamespace" = "${azurerm_servicebus_namespace.main.name}.servicebus.windows.net"
    "ServiceBusConnection__credential"              = "managedidentity"

    "DIRECTORY_BASE_URL"  = var.directory_base_url
    "ENRICHMENT_BASE_URL" = var.enrichment_base_url

    # A reference, not the value. The secret never appears in app settings,
    # in the portal, or in a `func azure functionapp fetch-app-settings` dump.
    "ENRICHMENT_API_KEY" = var.enrichment_api_key == "" ? "" : "@Microsoft.KeyVault(SecretUri=${azurerm_key_vault_secret.enrichment_api_key[0].versionless_id})"
  }

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

# --- Least-privilege RBAC ----------------------------------------------------
# Each assignment is the narrowest role that works. `Storage Blob Data Owner`
# rather than `Contributor`: the app needs data-plane access, not the ability
# to reconfigure or delete the account.

resource "azurerm_role_assignment" "func_blob" {
  scope                = azurerm_storage_account.main.id
  role_definition_name = "Storage Blob Data Owner"
  principal_id         = azurerm_function_app_flex_consumption.main.identity[0].principal_id
}

resource "azurerm_role_assignment" "func_queue" {
  scope                = azurerm_storage_account.main.id
  role_definition_name = "Storage Queue Data Contributor"
  principal_id         = azurerm_function_app_flex_consumption.main.identity[0].principal_id
}

resource "azurerm_role_assignment" "func_table" {
  scope                = azurerm_storage_account.main.id
  role_definition_name = "Storage Table Data Contributor"
  principal_id         = azurerm_function_app_flex_consumption.main.identity[0].principal_id
}

resource "azurerm_role_assignment" "func_servicebus" {
  scope                = azurerm_servicebus_namespace.main.id
  role_definition_name = "Azure Service Bus Data Receiver"
  principal_id         = azurerm_function_app_flex_consumption.main.identity[0].principal_id
}

resource "azurerm_role_assignment" "func_keyvault" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_function_app_flex_consumption.main.identity[0].principal_id
}

# --- Diagnostics -------------------------------------------------------------

resource "azurerm_monitor_diagnostic_setting" "function_app" {
  name                       = "diag-func"
  target_resource_id         = azurerm_function_app_flex_consumption.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  enabled_log {
    category = "FunctionAppLogs"
  }

  enabled_metric {
    category = "AllMetrics"
  }
}

resource "azurerm_monitor_diagnostic_setting" "servicebus" {
  name                       = "diag-sb"
  target_resource_id         = azurerm_servicebus_namespace.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  enabled_log {
    category = "OperationalLogs"
  }

  enabled_metric {
    category = "AllMetrics"
  }
}

# --- Alerting ----------------------------------------------------------------
# Dead-lettered messages mean a poison payload the retry policy could not
# clear. That is the signal worth waking someone for, not CPU.

resource "azurerm_monitor_metric_alert" "dead_letter" {
  name                = "alert-${local.name}-deadletter"
  resource_group_name = azurerm_resource_group.main.name
  scopes              = [azurerm_servicebus_namespace.main.id]
  description         = "Messages have landed in the dead-letter queue."
  severity            = 2
  frequency           = "PT5M"
  window_size         = "PT15M"

  criteria {
    metric_namespace = "Microsoft.ServiceBus/namespaces"
    metric_name      = "DeadletteredMessages"
    aggregation      = "Total"
    operator         = "GreaterThan"
    threshold        = 0
  }

  tags = local.tags
}
