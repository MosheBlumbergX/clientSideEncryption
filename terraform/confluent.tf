# -----------------------------------------------------------------------------
# Shared Confluent Cloud core: environment, cluster, Schema Registry, identities
# -----------------------------------------------------------------------------
#
# CSFLE and CSPE both need the Stream Governance ADVANCED package: encryption
# rules are part of Data Contracts, and the DEK Registry lives in Schema Registry.

resource "random_id" "suffix" {
  byte_length = 2
}

locals {
  name    = "${var.prefix}-cse"                         # base name for most resources
  uniq    = "${var.prefix}-cse-${random_id.suffix.hex}" # for globally-unique names
  sr_base = data.confluent_schema_registry_cluster.this
}

resource "confluent_environment" "this" {
  display_name = "${local.uniq}-env"

  stream_governance {
    package = "ADVANCED"
  }
}

resource "confluent_kafka_cluster" "this" {
  display_name = "${local.name}-cluster"
  availability = "SINGLE_ZONE"
  cloud        = var.confluent_cloud
  region       = var.confluent_region
  basic {}

  environment {
    id = confluent_environment.this.id
  }
}

# Schema Registry is provisioned automatically with the environment. Waiting on
# the cluster gives it time to come up before we look it up.
data "confluent_schema_registry_cluster" "this" {
  environment {
    id = confluent_environment.this.id
  }

  depends_on = [confluent_kafka_cluster.this]
}

# ----------------------------------------------------------- Terraform identity
# A dedicated service account that Terraform uses to manage topics, schemas,
# tags and KEKs (data-plane objects need cluster / SR scoped API keys).

resource "confluent_service_account" "manager" {
  display_name = "${local.uniq}-manager"
  description  = "Terraform manager for the client-side encryption demo"
}

resource "confluent_role_binding" "manager_env_admin" {
  principal   = "User:${confluent_service_account.manager.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.this.resource_name
}

resource "confluent_api_key" "manager_kafka" {
  display_name = "${local.name}-manager-kafka"
  owner {
    id          = confluent_service_account.manager.id
    api_version = confluent_service_account.manager.api_version
    kind        = confluent_service_account.manager.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.this.id
    api_version = confluent_kafka_cluster.this.api_version
    kind        = confluent_kafka_cluster.this.kind
    environment {
      id = confluent_environment.this.id
    }
  }
  depends_on = [confluent_role_binding.manager_env_admin]
}

resource "confluent_api_key" "manager_sr" {
  display_name = "${local.name}-manager-sr"
  owner {
    id          = confluent_service_account.manager.id
    api_version = confluent_service_account.manager.api_version
    kind        = confluent_service_account.manager.kind
  }
  managed_resource {
    id          = local.sr_base.id
    api_version = local.sr_base.api_version
    kind        = local.sr_base.kind
    environment {
      id = confluent_environment.this.id
    }
  }
  depends_on = [confluent_role_binding.manager_env_admin]
}

# RBAC and fresh API keys take a little while to propagate.
resource "time_sleep" "manager_ready" {
  create_duration = "60s"
  depends_on      = [confluent_api_key.manager_kafka, confluent_api_key.manager_sr]
}

# ------------------------------------------------------------ Client identity
# The producer/consumer apps. Note what this identity CAN do: read and write
# the topics, read the schemas, create/read DEKs. What it cannot do on its own
# is decrypt anything: that needs the KMS key, which lives in AWS / Azure.

resource "confluent_service_account" "client" {
  display_name = "${local.uniq}-client"
  description  = "Producer/consumer apps for the client-side encryption demo"
}

# Kafka access uses ACLs: Basic clusters do not support resource-scoped RBAC
# (on Standard/Dedicated you could use DeveloperRead/DeveloperWrite instead).
locals {
  client_acls = {
    topic_read     = { type = "TOPIC", op = "READ" }
    topic_write    = { type = "TOPIC", op = "WRITE" }
    topic_describe = { type = "TOPIC", op = "DESCRIBE" }
    group_read     = { type = "GROUP", op = "READ" }
  }
}

resource "confluent_kafka_acl" "client" {
  for_each = local.client_acls

  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }
  rest_endpoint = confluent_kafka_cluster.this.rest_endpoint
  credentials {
    key    = confluent_api_key.manager_kafka.id
    secret = confluent_api_key.manager_kafka.secret
  }

  resource_type = each.value.type
  resource_name = "${var.prefix}-"
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.client.id}"
  host          = "*"
  operation     = each.value.op
  permission    = "ALLOW"

  depends_on = [time_sleep.manager_ready]
}

# Schemas are registered by Terraform, so the apps only need to read them.
resource "confluent_role_binding" "client_subjects" {
  principal   = "User:${confluent_service_account.client.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.sr_base.resource_name}/subject=${var.prefix}-*"
}

# DEK Registry: producers create DEKs under a KEK, consumers read them.
resource "confluent_role_binding" "client_keks" {
  for_each    = toset(["DeveloperRead", "DeveloperWrite"])
  principal   = "User:${confluent_service_account.client.id}"
  role_name   = each.key
  crn_pattern = "${local.sr_base.resource_name}/kek=${var.prefix}-*"
}

resource "confluent_api_key" "client_kafka" {
  display_name = "${local.name}-client-kafka"
  owner {
    id          = confluent_service_account.client.id
    api_version = confluent_service_account.client.api_version
    kind        = confluent_service_account.client.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.this.id
    api_version = confluent_kafka_cluster.this.api_version
    kind        = confluent_kafka_cluster.this.kind
    environment {
      id = confluent_environment.this.id
    }
  }
}

resource "confluent_api_key" "client_sr" {
  display_name = "${local.name}-client-sr"
  owner {
    id          = confluent_service_account.client.id
    api_version = confluent_service_account.client.api_version
    kind        = confluent_service_account.client.kind
  }
  managed_resource {
    id          = local.sr_base.id
    api_version = local.sr_base.api_version
    kind        = local.sr_base.kind
    environment {
      id = confluent_environment.this.id
    }
  }
}

# ------------------------------------------------------------- Shared helpers

locals {
  kafka_creds = {
    key    = confluent_api_key.manager_kafka.id
    secret = confluent_api_key.manager_kafka.secret
  }
  sr_creds = {
    key    = confluent_api_key.manager_sr.id
    secret = confluent_api_key.manager_sr.secret
  }
}

# ------------------------------------------------------------- DEK clean-up
# Producers create DEKs at runtime, outside Terraform. Schema Registry will not
# delete a KEK that still has DEKs, so on destroy we delete them first. Each
# instance depends on its KEK, so Terraform destroys it (running the script)
# before it destroys the KEK. Deleting a DEK is irreversible: data encrypted
# with it can never be decrypted again (fine here, the topics go too).

locals {
  keks = merge(
    var.enable_csfle ? { csfle = confluent_schema_registry_kek.csfle[0].name } : {},
    var.enable_cspe ? { cspe = confluent_schema_registry_kek.cspe[0].name } : {},
  )
}

resource "terraform_data" "dek_cleanup" {
  for_each = local.keks

  input = {
    kek    = each.value
    url    = local.sr_base.rest_endpoint
    key    = confluent_api_key.manager_sr.id
    secret = confluent_api_key.manager_sr.secret
    script = "${path.module}/../scripts/delete_deks.py"
  }

  # Never replace this resource in place: replacement runs the destroy hook,
  # and deleting live DEKs makes every existing message undecryptable.
  lifecycle {
    ignore_changes = [input]
  }

  provisioner "local-exec" {
    when    = destroy
    command = "python3 \"${self.input.script}\""
    environment = {
      SR_URL    = self.input.url
      SR_KEY    = self.input.key
      SR_SECRET = self.input.secret
      KEK_NAME  = self.input.kek
    }
  }
}
