#!/usr/bin/env python3
"""End-to-end test against the deployed stack. Exits non-zero on any failure.

For each deployed use case it:
  1. produces a batch tagged with a unique run id,
  2. checks the raw bytes on the broker do NOT contain the sensitive plaintext,
  3. consumes WITH KMS access and checks every record round-trips exactly,
  4. consumes WITHOUT KMS access (separate process, no cloud credentials) and
     checks the sensitive data stays unreadable,
  5. checks the DEK was registered under the expected KEK in the DEK Registry.
"""

from __future__ import annotations

import base64
import json
import subprocess
import sys
import urllib.request
import uuid
from datetime import datetime, timezone

import common
import demo

N = 5
PII = ["email", "ssn", "card_number"]
failures: list[str] = []


def check(cond: bool, msg: str) -> None:
    print(f"  {'PASS' if cond else 'FAIL'}  {msg}")
    if not cond:
        failures.append(msg)


def norm(rec: dict) -> dict:
    """Make produced (epoch ms) and consumed (ISO datetime) timestamps comparable."""
    out = {}
    for k, v in rec.items():
        if k.endswith("_ts"):
            v = v if isinstance(v, int) else int(datetime.fromisoformat(v).astimezone(timezone.utc).timestamp() * 1000)
        out[k] = v
    return out


def intruder(use_case: str, ids: list[str]) -> list[dict]:
    """Consume in a fresh process with every cloud credential stripped."""
    res = subprocess.run(
        [sys.executable, __file__, "--intruder", use_case, *ids],
        capture_output=True, text=True, timeout=180,
    )
    if res.returncode != 0:
        raise RuntimeError(f"intruder process failed:\n{res.stderr[-2000:]}")
    return json.loads(res.stdout.strip().splitlines()[-1])


def deks_for(cfg: dict, kek: str) -> list[str]:
    sr = cfg["schema_registry"]
    req = urllib.request.Request(f"{sr['url']}/dek-registry/v1/keks/{kek}/deks")
    token = base64.b64encode(f"{sr['api_key']}:{sr['api_secret']}".encode()).decode()
    req.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def test_csfle(cfg: dict) -> None:
    print("\n=== CSFLE: field-level encryption with AWS KMS ===")
    run_id = "t" + uuid.uuid4().hex[:7]
    sent = {r["customer_id"]: r for r in demo.produce(cfg, "csfle", N, run_id, quiet=True)}
    print(f"  produced {N} customers (run {run_id})")

    got = demo.consume(cfg, "csfle", N, with_kms=True, want_ids=set(sent), quiet=True)
    check(len(got) == N, f"consumer with KMS received all {N} records")
    check(all("value" in g and norm(g["value"]) == norm(sent[g["key"]]) for g in got),
          "every record decrypts back to exactly what was produced")

    raw_ok = all(
        all(sent[g["key"]][f].encode() not in g["raw"] for f in PII) for g in got
    )
    check(raw_ok, "raw bytes on the broker contain no plaintext email / SSN / card number")
    check(all(sent[g["key"]]["full_name"].encode() in g["raw"] for g in got),
          "non-PII fields (full_name) are still plaintext on the wire (field-level, not payload)")

    bad = intruder("csfle", list(sent))
    check(len(bad) == N and all("value" in b for b in bad),
          "consumer WITHOUT KMS can still deserialize the record (onFailure NONE on read)")
    check(all(b["value"][f] != sent[b["key"]][f] for b in bad for f in PII),
          "... but every PII field it sees is ciphertext")
    check(all(b["value"]["full_name"] == sent[b["key"]]["full_name"] for b in bad),
          "... and the non-PII fields are readable")
    sample = bad[0]["value"]
    print(f"    intruder sees: email={sample['email'][:40]}...  full_name={sample['full_name']}")

    deks = deks_for(cfg, cfg["csfle"]["kek_name"])
    check(f"{cfg['csfle']['topic']}-value" in deks, f"DEK registered under KEK {cfg['csfle']['kek_name']}")


def test_cspe(cfg: dict) -> None:
    print("\n=== CSPE: payload encryption with Azure Key Vault ===")
    run_id = "t" + uuid.uuid4().hex[:7]
    sent = {r["payment_id"]: r for r in demo.produce(cfg, "cspe", N, run_id, quiet=True)}
    print(f"  produced {N} payments (run {run_id})")

    got = demo.consume(cfg, "cspe", N, with_kms=True, want_ids=set(sent), quiet=True)
    check(len(got) == N, f"consumer with KMS received all {N} records")
    check(all("value" in g and norm(g["value"]) == norm(sent[g["key"]]) for g in got),
          "every record decrypts back to exactly what was produced")
    check(all(not any(str(sent[g["key"]][f]).encode() in g["raw"] for f in ("payment_id", "merchant", "iban", "currency"))
              for g in got),
          "raw bytes contain NONE of the field values (payment_id, merchant, IBAN, currency)")

    bad = intruder("cspe", list(sent))
    check(len(bad) == N, f"consumer WITHOUT KMS still receives the {N} messages")
    leaked = [b for b in bad if "value" in b and isinstance(b["value"], dict)
              and any(b["value"].get(f) == sent[b["key"]][f] for f in ("merchant", "iban"))]
    check(not leaked, "... but cannot read a single field of the payload")
    print(f"    intruder gets: {bad[0].get('error', bad[0].get('value'))!s:.110}")

    deks = deks_for(cfg, cfg["cspe"]["kek_name"])
    check(f"{cfg['cspe']['topic']}-value" in deks, f"DEK registered under KEK {cfg['cspe']['kek_name']}")


def intruder_main(use_case: str, ids: list[str]) -> None:
    cfg = common.load_config()
    got = demo.consume(cfg, use_case, len(ids), with_kms=False, want_ids=set(ids), quiet=True, timeout_s=90)
    for g in got:
        g["raw"] = len(g["raw"])
    print(json.dumps(got, default=str))


def main() -> int:
    if len(sys.argv) > 2 and sys.argv[1] == "--intruder":
        intruder_main(sys.argv[2], sys.argv[3:])
        return 0

    cfg = common.load_config()
    ran = False
    for use_case, fn in (("csfle", test_csfle), ("cspe", test_cspe)):
        if cfg.get(use_case):
            ran = True
            fn(cfg)
    if not ran:
        print("nothing deployed")
        return 1
    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{len(failures)} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
