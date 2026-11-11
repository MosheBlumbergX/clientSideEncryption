#!/usr/bin/env python3
"""Delete every DEK under a Schema Registry KEK (soft, then permanent).

Clients create DEKs at runtime, so Terraform does not know about them, and
Schema Registry refuses to delete a KEK that still has DEKs (error 42272).
Terraform runs this from a destroy-time provisioner just before the KEK goes.

Reads SR_URL, SR_KEY, SR_SECRET and KEK_NAME from the environment.
Standard library only.
"""

import base64
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

url = os.environ["SR_URL"].rstrip("/")
kek = urllib.parse.quote(os.environ["KEK_NAME"], safe="")
auth = base64.b64encode(f"{os.environ['SR_KEY']}:{os.environ['SR_SECRET']}".encode()).decode()


def call(method: str, path: str):
    req = urllib.request.Request(f"{url}{path}", method=method)
    req.add_header("Authorization", f"Basic {auth}")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body = r.read()
            return json.loads(body) if body else None
    except urllib.error.HTTPError as e:
        if e.code == 404:  # KEK or DEK already gone
            return None
        raise SystemExit(f"{method} {path}: HTTP {e.code} {e.read().decode()[:300]}")


subjects = call("GET", f"/dek-registry/v1/keks/{kek}/deks?deleted=true") or []
for subject in subjects:
    s = urllib.parse.quote(subject, safe="")
    call("DELETE", f"/dek-registry/v1/keks/{kek}/deks/{s}?permanent=false")
    call("DELETE", f"/dek-registry/v1/keks/{kek}/deks/{s}?permanent=true")
    print(f"deleted DEKs for subject {subject} under KEK {os.environ['KEK_NAME']}")

if not subjects:
    print(f"no DEKs under KEK {os.environ['KEK_NAME']}")
sys.exit(0)
