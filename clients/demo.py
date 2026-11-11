#!/usr/bin/env python3
"""Produce and consume encrypted events.

    demo.py csfle produce -n 5          # PII fields encrypted with AWS KMS
    demo.py csfle consume -n 5          # decrypted (has KMS access)
    demo.py csfle consume -n 5 --no-kms # PII is ciphertext, the rest is readable

    demo.py cspe  produce -n 5          # whole payload encrypted with Azure Key Vault
    demo.py cspe  consume -n 5          # decrypted
    demo.py cspe  consume -n 5 --no-kms # nothing readable at all
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from confluent_kafka import Consumer, Producer
from confluent_kafka.serialization import MessageField, SerializationContext

import common

FIRST = ["Ada", "Grace", "Alan", "Linus", "Margaret", "Dennis", "Barbara", "Ken", "Radia", "Tim"]
LAST = ["Lovelace", "Hopper", "Turing", "Torvalds", "Hamilton", "Ritchie", "Liskov", "Thompson", "Perlman", "Berners-Lee"]
COUNTRIES = ["US", "DE", "IL", "GB", "FR", "NL", "IN", "BR"]
MERCHANTS = ["Coffee Lab", "Book Nook", "Grid Energy", "Sky Air", "Fresh Market", "Cloud Gym"]


def now_ms() -> int:
    return int(time.time() * 1000)


def fake_customer(run_id: str, i: int) -> dict[str, Any]:
    first, last = random.choice(FIRST), random.choice(LAST)
    return {
        "customer_id": f"{run_id}-{i}",
        "full_name": f"{first} {last}",
        "country": random.choice(COUNTRIES),
        "email": f"{first}.{last}@example.com".lower(),
        "ssn": f"{random.randint(100, 899):03d}-{random.randint(10, 99):02d}-{random.randint(1000, 9999):04d}",
        "card_number": "4111 " + " ".join(f"{random.randint(0, 9999):04d}" for _ in range(3)),
        "signup_ts": now_ms(),
    }


def fake_payment(run_id: str, i: int) -> dict[str, Any]:
    return {
        "payment_id": f"{run_id}-{i}",
        "customer_id": f"cust-{random.randint(1000, 9999)}",
        "amount": round(random.uniform(3, 900), 2),
        "currency": random.choice(["USD", "EUR", "GBP", "ILS"]),
        "merchant": random.choice(MERCHANTS),
        "iban": f"DE{random.randint(10, 99)}{random.randint(10**17, 10**18 - 1)}",
        "created_ts": now_ms(),
    }


GENERATORS: dict[str, Callable[[str, int], dict[str, Any]]] = {"csfle": fake_customer, "cspe": fake_payment}
KEY_FIELD = {"csfle": "customer_id", "cspe": "payment_id"}


def _jsonable(rec: dict[str, Any]) -> dict[str, Any]:
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in rec.items()}


# ------------------------------------------------------------------- produce


def produce(cfg: dict[str, Any], use_case: str, n: int, run_id: str | None = None, quiet: bool = False) -> list[dict]:
    topic = cfg[use_case]["topic"]
    run_id = run_id or uuid.uuid4().hex[:8]
    ser = common.serializer(cfg, use_case)
    producer = Producer(common.kafka_conf(cfg))
    sent = []

    errors: list[str] = []

    def on_delivery(err, msg):
        if err:
            errors.append(str(err))

    for i in range(n):
        rec = GENERATORS[use_case](run_id, i)
        # Encryption happens here. Pass a copy: CSFLE rewrites the PII fields of
        # the dict it is given in place, replacing them with ciphertext.
        value = ser(dict(rec), SerializationContext(topic, MessageField.VALUE))
        producer.produce(topic, key=rec[KEY_FIELD[use_case]], value=value, on_delivery=on_delivery)
        sent.append(rec)
        if not quiet:
            print(f"-> {json.dumps(rec)}")
    producer.flush(30)
    if errors:
        raise RuntimeError(f"delivery failed: {errors[0]}")
    if not quiet:
        print(f"\nproduced {n} {'customers' if use_case == 'csfle' else 'payments'} to {topic} (run {run_id})")
    return sent


# ------------------------------------------------------------------- consume


def consume(
    cfg: dict[str, Any],
    use_case: str,
    n: int,
    with_kms: bool,
    want_ids: set[str] | None = None,
    timeout_s: float = 60,
    quiet: bool = False,
) -> list[dict]:
    """Read from the beginning of the topic. Returns one entry per message:
    {"key", "raw" (bytes on the wire), "value" (dict) or "error" (str)}."""
    topic = cfg[use_case]["topic"]
    deser = common.deserializer(cfg, use_case, with_kms)
    consumer = Consumer(
        {
            **common.kafka_conf(cfg),
            "group.id": f"{cfg['group_prefix']}-demo-{uuid.uuid4().hex[:6]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])

    out: list[dict] = []
    deadline = time.time() + timeout_s
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(msg.error())
            key = msg.key().decode() if msg.key() else None
            if want_ids is not None and key not in want_ids:
                continue
            entry: dict[str, Any] = {"key": key, "raw": msg.value()}
            try:
                entry["value"] = _jsonable(deser(msg.value(), SerializationContext(topic, MessageField.VALUE)))
            except Exception as e:  # CSPE without KMS lands here
                entry["error"] = f"{type(e).__name__}: {e}"
            out.append(entry)
            if not quiet:
                _print_entry(entry, with_kms)
            if want_ids is not None:
                if {e["key"] for e in out} >= want_ids:
                    break
            elif len(out) >= n:
                break
    finally:
        consumer.close()
    return out


def _print_entry(entry: dict, with_kms: bool) -> None:
    if "value" in entry:
        tag = "plaintext" if with_kms else "no KMS access"
        print(f"<- [{tag}] {json.dumps(entry['value'])}")
    else:
        raw = entry["raw"]
        print(f"<- [no KMS access] cannot decrypt -> payload is unreadable ({entry['error'].split(':')[0]})")
        print(f"   raw bytes on the wire ({len(raw)} B): {raw[:48].hex(' ')} ...")


# ---------------------------------------------------------------------- main


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("use_case", choices=["csfle", "cspe"])
    p.add_argument("action", choices=["produce", "consume"])
    p.add_argument("-n", type=int, default=5, help="number of records")
    p.add_argument("--no-kms", action="store_true", help="consume without any KMS credentials")
    args = p.parse_args()

    cfg = common.load_config()
    if not cfg.get(args.use_case):
        raise SystemExit(f"{args.use_case} is not deployed (enable_{args.use_case} = false).")

    if args.action == "produce":
        produce(cfg, args.use_case, args.n)
    else:
        got = consume(cfg, args.use_case, args.n, with_kms=not args.no_kms)
        if not got:
            print("no records found: run the produce target first")
    return 0


if __name__ == "__main__":
    sys.exit(main())
