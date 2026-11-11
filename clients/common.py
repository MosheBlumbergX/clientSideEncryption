"""Shared plumbing for the CSFLE / CSPE demo clients.

The interesting part is small: register the encryption rule executors and the
KMS drivers once, then hand each (de)serializer a `rule_conf` that tells the
KMS driver how to authenticate. Everything else (which fields to encrypt,
which KEK to use, which algorithm) comes from the data contract that Terraform
registered in Schema Registry. The application code does not change.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.rules.encryption.awskms.aws_driver import AwsKmsDriver
from confluent_kafka.schema_registry.rules.encryption.azurekms.azure_driver import AzureKmsDriver
from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import (
    EncryptionExecutor,  # ENCRYPT_PAYLOAD -> CSPE (encoding rule)
    FieldEncryptionExecutor,  # ENCRYPT         -> CSFLE (domain rule)
)

CONFIG_PATH = Path(__file__).with_name("config.json")

# The Azure SDK logs every failed token request; in --no-kms mode that is noise.
logging.getLogger("azure").setLevel(logging.CRITICAL)

# Register once per process.
FieldEncryptionExecutor.register()
EncryptionExecutor.register()
AwsKmsDriver.register()
AzureKmsDriver.register()


def load_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        raise SystemExit(f"{CONFIG_PATH} not found. Run `make apply` (or `make config`) first.")
    return json.loads(CONFIG_PATH.read_text())


def kafka_conf(cfg: dict[str, Any]) -> dict[str, Any]:
    k = cfg["kafka"]
    return {
        "bootstrap.servers": k["bootstrap_servers"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": k["api_key"],
        "sasl.password": k["api_secret"],
        "log_level": 3,  # hide librdkafka info chatter, keep errors
    }


def schema_registry(cfg: dict[str, Any]) -> SchemaRegistryClient:
    sr = cfg["schema_registry"]
    return SchemaRegistryClient(
        {"url": sr["url"], "basic.auth.user.info": f"{sr['api_key']}:{sr['api_secret']}"}
    )


# --------------------------------------------------------------------- KMS auth


def kms_rule_conf(cfg: dict[str, Any], use_case: str, with_kms: bool) -> dict[str, str]:
    """KMS credentials for the encryption executor.

    These are the ONLY encryption-specific settings an app needs. With them the
    client can unwrap the DEK and read plaintext; without them it cannot,
    no matter what Kafka / Schema Registry permissions it has.
    """
    if not with_kms:
        strip_ambient_cloud_credentials()
        if use_case == "cspe":
            # A syntactically valid identity that Azure will reject, so the
            # Azure SDK does not fall back to your `az login` session.
            c = cfg["cspe"]
            return {"tenant.id": c["tenant_id"], "client.id": c["client_id"], "client.secret": "not-the-secret"}
        return {}

    if use_case == "csfle":
        c = cfg["csfle"]
        conf = {"role.arn": c["aws_role_arn"], "role.session.name": "csfle-demo-client"}
        if c.get("aws_profile"):
            conf["profile"] = c["aws_profile"]  # base credentials used to assume the role
        return conf

    c = cfg["cspe"]
    return {"tenant.id": c["tenant_id"], "client.id": c["client_id"], "client.secret": c["client_secret"]}


def strip_ambient_cloud_credentials() -> None:
    """Simulate a machine with Kafka access but no cloud/KMS identity at all."""
    for var in list(os.environ):
        if var.startswith(("AWS_", "AZURE_", "ARM_")):
            del os.environ[var]
    empty = tempfile.mkdtemp(prefix="no-kms-")
    os.environ["AWS_CONFIG_FILE"] = os.path.join(empty, "config")
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = os.path.join(empty, "credentials")
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    os.environ["AZURE_CONFIG_DIR"] = empty


# ------------------------------------------------------------------ (De)serde


def serializer(cfg: dict[str, Any], use_case: str) -> AvroSerializer:
    return AvroSerializer(
        schema_registry(cfg),
        schema_str=None,  # use the schema + ruleset Terraform registered
        conf={"auto.register.schemas": False, "use.latest.version": True},
        rule_conf=kms_rule_conf(cfg, use_case, with_kms=True),
    )


def deserializer(cfg: dict[str, Any], use_case: str, with_kms: bool) -> AvroDeserializer:
    return AvroDeserializer(
        schema_registry(cfg),
        rule_conf=kms_rule_conf(cfg, use_case, with_kms),
    )
