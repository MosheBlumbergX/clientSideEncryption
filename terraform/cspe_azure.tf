# -----------------------------------------------------------------------------
# Use case 2: Client-Side PAYLOAD Encryption (CSPE) with Azure Key Vault
# -----------------------------------------------------------------------------
#
# The whole serialized message is encrypted, not individual fields. Use it when
# the entire event is sensitive (payments, health records) or when you do not
# want to maintain field tags. Field names, structure and sizes of individual
# values are hidden too.
#
# CSPE is an ENCODING rule: it runs AFTER the serializer turns the record into
# Avro bytes (and BEFORE the deserializer on the way back), so it works the
# same for Avro, Protobuf and JSON Schema.

locals {
  cspe_count = var.enable_cspe ? 1 : 0
  cspe_topic = "${var.prefix}-payments"
}

data "azurerm_client_config" "current" {
  count = local.cspe_count
}

resource "azurerm_resource_group" "cspe" {
  count    = local.cspe_count
  name     = "${local.name}-rg"
  location = var.azure_location
  tags     = var.tags
}

# ------------------------------------------------------- Azure Key Vault (KEK)

resource "azurerm_key_vault" "cspe" {
  count                      = local.cspe_count
  name                       = "${var.prefix}-cspe-${random_id.suffix.hex}" # globally unique, <= 24 chars
  location                   = azurerm_resource_group.cspe[0].location
  resource_group_name        = azurerm_resource_group.cspe[0].name
  tenant_id                  = data.azurerm_client_config.current[0].tenant_id
  sku_name                   = "standard"
  rbac_authorization_enabled = true
  soft_delete_retention_days = 7
  purge_protection_enabled   = false # demo only: lets `terraform destroy` purge the vault
  tags                       = var.tags
}

# Whoever runs Terraform needs data-plane rights to create the key.
resource "azurerm_role_assignment" "cspe_deployer" {
  count                = local.cspe_count
  scope                = azurerm_key_vault.cspe[0].id
  role_definition_name = "Key Vault Crypto Officer"
  principal_id         = data.azurerm_client_config.current[0].object_id
}

# Azure RBAC is eventually consistent; creating the key right away gets a 403.
resource "time_sleep" "cspe_rbac" {
  count           = local.cspe_count
  create_duration = "60s"
  depends_on      = [azurerm_role_assignment.cspe_deployer]
}

resource "azurerm_key_vault_key" "cspe" {
  count        = local.cspe_count
  name         = "${local.name}-cspe-kek"
  key_vault_id = azurerm_key_vault.cspe[0].id
  key_type     = "RSA"
  key_size     = 3072
  key_opts     = ["wrapKey", "unwrapKey", "encrypt", "decrypt"]
  tags         = var.tags

  depends_on = [time_sleep.cspe_rbac]
}

# ------------------------------------------- App identity for the Kafka clients

resource "azuread_application" "cspe_client" {
  count        = local.cspe_count
  display_name = "${local.name}-cspe-kafka-client"
  owners       = [data.azurerm_client_config.current[0].object_id]
  notes        = "Kafka producer/consumer identity for the CSPE demo. Owner: ${var.prefix}"
}

resource "azuread_service_principal" "cspe_client" {
  count     = local.cspe_count
  client_id = azuread_application.cspe_client[0].client_id
  owners    = [data.azurerm_client_config.current[0].object_id]
}

resource "azuread_application_password" "cspe_client" {
  count          = local.cspe_count
  application_id = azuread_application.cspe_client[0].id
  display_name   = "kafka-client"
}

# Least privilege: wrap/unwrap on this one key, nothing else in the vault.
resource "azurerm_role_assignment" "cspe_client" {
  count                = local.cspe_count
  scope                = azurerm_key_vault_key.cspe[0].resource_versionless_id
  role_definition_name = "Key Vault Crypto User"
  principal_id         = azuread_service_principal.cspe_client[0].object_id
}

# ------------------------------------------------------- Schema Registry: KEK

resource "confluent_schema_registry_kek" "cspe" {
  count = local.cspe_count

  schema_registry_cluster {
    id = local.sr_base.id
  }
  rest_endpoint = local.sr_base.rest_endpoint
  credentials {
    key    = local.sr_creds.key
    secret = local.sr_creds.secret
  }

  name        = "${var.prefix}-cspe-kek"
  kms_type    = "azure-kms"
  kms_key_id  = azurerm_key_vault_key.cspe[0].id
  doc         = "CSPE KEK backed by Azure Key Vault ${azurerm_key_vault.cspe[0].name}"
  shared      = false
  hard_delete = true

  depends_on = [time_sleep.manager_ready]
}

# ------------------------------------------------------- Topic + data contract

resource "confluent_kafka_topic" "payments" {
  count = local.cspe_count

  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }
  rest_endpoint = confluent_kafka_cluster.this.rest_endpoint
  credentials {
    key    = local.kafka_creds.key
    secret = local.kafka_creds.secret
  }

  topic_name       = local.cspe_topic
  partitions_count = 3

  depends_on = [time_sleep.manager_ready]
}

resource "confluent_schema" "payments" {
  count = local.cspe_count

  schema_registry_cluster {
    id = local.sr_base.id
  }
  rest_endpoint = local.sr_base.rest_endpoint
  credentials {
    key    = local.sr_creds.key
    secret = local.sr_creds.secret
  }

  subject_name = "${local.cspe_topic}-value"
  format       = "AVRO"
  schema       = file("${path.module}/schemas/payment.avsc")
  hard_delete  = true

  ruleset {
    # An ENCODING rule works on the serialized bytes: the entire payload is
    # encrypted. Note the type: ENCRYPT_PAYLOAD (CSFLE uses ENCRYPT).
    encoding_rules {
      name = "encrypt-payload"
      doc  = "Encrypt the whole payload with the Azure-Key-Vault-backed KEK"
      kind = "TRANSFORM"
      type = "ENCRYPT_PAYLOAD"
      mode = "WRITEREAD"
      params = {
        "encrypt.kek.name"      = confluent_schema_registry_kek.cspe[0].name
        "encrypt.dek.algorithm" = "AES256_GCM"
      }
      on_failure = "ERROR,NONE"
    }
  }

  depends_on = [time_sleep.manager_ready]
}
