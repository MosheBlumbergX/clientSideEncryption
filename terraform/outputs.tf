output "confluent_environment_id" {
  value = confluent_environment.this.id
}

output "kafka_cluster_id" {
  value = confluent_kafka_cluster.this.id
}

output "schema_registry_endpoint" {
  value = local.sr_base.rest_endpoint
}

output "csfle_kms_key_arn" {
  value = var.enable_csfle ? aws_kms_key.csfle[0].arn : null
}

output "cspe_key_vault_key_id" {
  value = var.enable_cspe ? azurerm_key_vault_key.cspe[0].id : null
}

# Everything the Python clients need. `make config` writes this to
# clients/config.json (git-ignored). Contains secrets: never commit it.
output "client_config" {
  sensitive = true
  value = {
    kafka = {
      bootstrap_servers = trimprefix(confluent_kafka_cluster.this.bootstrap_endpoint, "SASL_SSL://")
      api_key           = confluent_api_key.client_kafka.id
      api_secret        = confluent_api_key.client_kafka.secret
    }
    schema_registry = {
      url        = local.sr_base.rest_endpoint
      api_key    = confluent_api_key.client_sr.id
      api_secret = confluent_api_key.client_sr.secret
    }
    group_prefix = var.prefix
    csfle = var.enable_csfle ? {
      topic        = local.csfle_topic
      kek_name     = confluent_schema_registry_kek.csfle[0].name
      kms_key_arn  = aws_kms_key.csfle[0].arn
      aws_role_arn = aws_iam_role.csfle_client[0].arn
      aws_profile  = var.aws_profile
    } : null
    cspe = var.enable_cspe ? {
      topic         = local.cspe_topic
      kek_name      = confluent_schema_registry_kek.cspe[0].name
      key_id        = azurerm_key_vault_key.cspe[0].id
      tenant_id     = data.azurerm_client_config.current[0].tenant_id
      client_id     = azuread_application.cspe_client[0].client_id
      client_secret = azuread_application_password.cspe_client[0].value
    } : null
  }
}
