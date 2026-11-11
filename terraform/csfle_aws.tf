# -----------------------------------------------------------------------------
# Use case 1: Client-Side FIELD Level Encryption (CSFLE) with AWS KMS
# -----------------------------------------------------------------------------
#
# Only the fields tagged PII in schemas/customer.avsc are encrypted. Everything
# else stays readable, so non-sensitive consumers (analytics, routing, Flink)
# keep working while email / SSN / card number are ciphertext on the wire,
# on the broker disks and in every downstream copy.
#
#   producer --(PII fields encrypted with DEK)--> Kafka --> consumer
#        \__ DEK wrapped by KEK __ AWS KMS key __/
#
# Confluent never sees the KMS key (KEK shared = false): only clients that can
# assume the IAM role below can unwrap the DEK and read the PII.

locals {
  csfle_count = var.enable_csfle ? 1 : 0
  csfle_topic = "${var.prefix}-customers"
}

data "aws_caller_identity" "current" {
  count = local.csfle_count
}

# ------------------------------------------------------------- AWS KMS (KEK)

resource "aws_kms_key" "csfle" {
  count                   = local.csfle_count
  description             = "${local.name} CSFLE key encryption key (KEK)"
  key_usage               = "ENCRYPT_DECRYPT"
  enable_key_rotation     = true
  deletion_window_in_days = 7
}

resource "aws_kms_alias" "csfle" {
  count         = local.csfle_count
  name          = "alias/${local.name}-csfle"
  target_key_id = aws_kms_key.csfle[0].key_id
}

# The identity the Kafka clients use to reach KMS. Any principal in this
# account that is allowed sts:AssumeRole can become it (your SSO admin role,
# an EC2 instance profile, an EKS pod role...).
resource "aws_iam_role" "csfle_client" {
  count       = local.csfle_count
  name        = "${local.name}-csfle-kms-client"
  description = "Kafka producer/consumer access to the CSFLE KMS key"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current[0].account_id}:root" }
    }]
  })
}

resource "aws_iam_role_policy" "csfle_client_kms" {
  count = local.csfle_count
  name  = "kms-encrypt-decrypt"
  role  = aws_iam_role.csfle_client[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
      Resource = aws_kms_key.csfle[0].arn
    }]
  })
}

# ------------------------------------------------- Schema Registry: tag + KEK

resource "confluent_tag" "pii" {
  count = local.csfle_count

  schema_registry_cluster {
    id = local.sr_base.id
  }
  rest_endpoint = local.sr_base.rest_endpoint
  credentials {
    key    = local.sr_creds.key
    secret = local.sr_creds.secret
  }

  name        = "PII"
  description = "Personally identifiable information. Encrypted client-side."

  depends_on = [time_sleep.manager_ready]
}

# Tag deletion right after the schema that references it returns 409, so give
# Schema Registry a moment between the two on destroy.
resource "time_sleep" "pii_tag_settle" {
  count            = local.csfle_count
  destroy_duration = "30s"
  depends_on       = [confluent_tag.pii]
}

# The KEK is only a *reference* to the AWS KMS key; the key material never
# leaves AWS. shared = false means Confluent is NOT granted access to it.
resource "confluent_schema_registry_kek" "csfle" {
  count = local.csfle_count

  schema_registry_cluster {
    id = local.sr_base.id
  }
  rest_endpoint = local.sr_base.rest_endpoint
  credentials {
    key    = local.sr_creds.key
    secret = local.sr_creds.secret
  }

  name        = "${var.prefix}-csfle-kek"
  kms_type    = "aws-kms"
  kms_key_id  = aws_kms_key.csfle[0].arn
  doc         = "CSFLE KEK backed by AWS KMS (${aws_kms_alias.csfle[0].name})"
  shared      = false
  hard_delete = true

  depends_on = [time_sleep.manager_ready]
}

# ------------------------------------------------------- Topic + data contract

resource "confluent_kafka_topic" "customers" {
  count = local.csfle_count

  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }
  rest_endpoint = confluent_kafka_cluster.this.rest_endpoint
  credentials {
    key    = local.kafka_creds.key
    secret = local.kafka_creds.secret
  }

  topic_name       = local.csfle_topic
  partitions_count = 3

  depends_on = [time_sleep.manager_ready]
}

resource "confluent_schema" "customers" {
  count = local.csfle_count

  schema_registry_cluster {
    id = local.sr_base.id
  }
  rest_endpoint = local.sr_base.rest_endpoint
  credentials {
    key    = local.sr_creds.key
    secret = local.sr_creds.secret
  }

  subject_name = "${local.csfle_topic}-value"
  format       = "AVRO"
  schema       = file("${path.module}/schemas/customer.avsc")
  hard_delete  = true

  ruleset {
    # A DOMAIN rule runs on individual fields: every field tagged PII is
    # encrypted on write and decrypted on read.
    domain_rules {
      name = "encrypt-pii"
      doc  = "Encrypt every field tagged PII with the AWS-KMS-backed KEK"
      kind = "TRANSFORM"
      type = "ENCRYPT"
      mode = "WRITEREAD"
      tags = [confluent_tag.pii[0].name]
      params = {
        "encrypt.kek.name"        = confluent_schema_registry_kek.csfle[0].name
        "encrypt.dek.algorithm"   = "AES256_GCM"
        "encrypt.dek.expiry.days" = "30" # rotate the DEK monthly
      }
      # WRITE: fail hard if we cannot encrypt (never leak plaintext PII).
      # READ:  if a consumer cannot decrypt, hand it the ciphertext instead
      #        of failing, so non-privileged consumers still get the rest.
      on_failure = "ERROR,NONE"
    }
  }

  depends_on = [time_sleep.pii_tag_settle]
}
