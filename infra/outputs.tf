output "resource_group_name" {
  description = "Resource group containing every resource in this stack."
  value       = azurerm_resource_group.main.name
}

output "function_app_name" {
  description = "Function App name, for `func azure functionapp publish`."
  value       = azurerm_function_app_flex_consumption.main.name
}

output "function_app_hostname" {
  description = "Default hostname of the deployed Function App."
  value       = azurerm_function_app_flex_consumption.main.default_hostname
}

output "storage_account_name" {
  description = "Storage account backing Durable Functions history and records."
  value       = azurerm_storage_account.main.name
}

output "service_bus_namespace" {
  description = "Service Bus namespace hosting the ingest queue."
  value       = azurerm_servicebus_namespace.main.name
}

output "application_insights_name" {
  description = "Application Insights resource for traces and live metrics."
  value       = azurerm_application_insights.main.name
}

output "managed_identity_principal_id" {
  description = "Function App system-assigned identity, holder of every data-plane role."
  value       = azurerm_function_app_flex_consumption.main.identity[0].principal_id
}
