#!/usr/bin/env python3
"""Seed the TrailForge demo using the current cookie/CSRF console API."""

from __future__ import annotations

import http.cookiejar
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8080").rstrip("/")
TENANT = "tenant-demo"
APP = "trailforge"

JAR = http.cookiejar.CookieJar()
OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(JAR))


def csrf_token() -> str:
    for cookie in JAR:
        if cookie.name == "csrf_token":
            return cookie.value
    return ""


def request(method: str, path: str, body: dict | None = None, *, tenant: bool = False) -> tuple[int, object]:
    data = None if body is None else json.dumps(body, ensure_ascii=False).encode()
    headers = {"Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    token = csrf_token()
    if token and method not in {"GET", "HEAD"}:
        headers["X-CSRF-Token"] = token
    if tenant:
        headers["X-Active-Tenant"] = TENANT
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with OPENER.open(req) as response:
            raw = response.read()
            content_type = response.headers.get("Content-Type", "")
            if raw and "json" in content_type:
                return response.status, json.loads(raw.decode())
            return response.status, raw.decode(errors="replace") if raw else None
    except urllib.error.HTTPError as error:
        raw = error.read().decode(errors="replace")
        try:
            payload = json.loads(raw) if raw else {"error": raw}
        except json.JSONDecodeError:
            payload = {"error": raw}
        return error.code, payload


def login() -> dict:
    mode = os.getenv("TRAILFORGE_AUTH", "mock").strip().lower()
    if mode == "local":
        username = os.getenv("TRAILFORGE_USERNAME", "").strip()
        password = os.getenv("TRAILFORGE_PASSWORD", "")
        if not username or not password:
            raise RuntimeError("local auth requires TRAILFORGE_USERNAME and TRAILFORGE_PASSWORD")
        status, payload = request("POST", "/api/v1/auth/local/login", {"username": username, "password": password})
        if status != 200:
            raise RuntimeError(f"local login failed: {status} {payload}")
    elif mode == "mock":
        status, payload = request("GET", "/api/v1/auth/login?provider=mock")
        if status != 200 or not isinstance(payload, dict) or not payload.get("auth_url"):
            raise RuntimeError(f"mock login is unavailable: {status} {payload}")
        with OPENER.open(BASE + str(payload["auth_url"])) as response:
            response.read()
    else:
        raise RuntimeError("TRAILFORGE_AUTH must be mock or local")

    status, me = request("GET", "/api/v1/auth/me")
    if status != 200 or not isinstance(me, dict):
        raise RuntimeError(f"read current user failed: {status} {me}")
    return me


def ensure_tenant(me: dict) -> None:
    status, payload = request("GET", "/api/v1/tenants")
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"list tenants failed: {status} {payload}")
    if any((item or {}).get("tenant_id") == TENANT for item in payload.get("tenants") or []):
        return
    if not me.get("is_system_admin"):
        raise RuntimeError(f"tenant {TENANT} does not exist and current account is not a system administrator")
    status, payload = request("POST", "/api/v1/tenants", {
        "tenant_id": TENANT,
        "display_name": "TrailForge Demo",
        "initial_admin_platform_user_id": me["platform_user_id"],
    })
    if status != 201:
        raise RuntimeError(f"create tenant failed: {status} {payload}")


def configure_model_policy() -> None:
    status, payload = request("PUT", f"/api/v1/tenant-model-policy?tenant={urllib.parse.quote(TENANT)}", {
        "tenant_id": TENANT,
        "models": [{"provider_id": "zhipu-ai", "name": "glm-5.3-flash"}],
    }, tenant=True)
    if status != 200:
        raise RuntimeError(f"configure tenant model policy failed: {status} {payload}")


def application_payload() -> dict:
    payload = json.loads((ROOT / "application.json").read_text(encoding="utf-8"))
    instruction_file = payload.pop("instruction_file", "instruction.txt")
    payload["instruction"] = (ROOT / instruction_file).read_text(encoding="utf-8").strip()
    if os.getenv("TRAILFORGE_WITH_CHANNELS", "").strip().lower() not in {"1", "true", "yes", "on"}:
        payload["channels"] = []
    return payload


def upsert_application() -> None:
    status, listed = request("GET", f"/api/v1/apps?tenant={urllib.parse.quote(TENANT)}", tenant=True)
    if status != 200 or not isinstance(listed, dict):
        raise RuntimeError(f"list apps failed: {status} {listed}")
    exists = any(((item or {}).get("Config") or {}).get("app_code") == APP for item in listed.get("applications") or [])
    path = f"/api/v1/apps/{TENANT}/{APP}" if exists else "/api/v1/apps"
    method = "PUT" if exists else "POST"
    status, result = request(method, path, application_payload(), tenant=True)
    if status not in {200, 201}:
        raise RuntimeError(f"upsert app failed: {status} {result}")
    print(f"application {TENANT}/{APP}: ok")


def ingest_knowledge() -> None:
    for path in sorted((ROOT / "knowledge").glob("*.md")):
        payload = {
            "tenant_id": TENANT,
            "app_code": APP,
            "document_id": path.stem,
            "name": path.name,
            "content": path.read_text(encoding="utf-8"),
            "chunk_size": 600,
            "overlap": 80,
            "metadata": {"source": "trailforge-example", "kind": path.stem},
        }
        status, result = request("POST", "/api/v1/knowledge", payload, tenant=True)
        if status not in {200, 201, 202, 204}:
            raise RuntimeError(f"ingest {path.name} failed: {status} {result}")
        print(f"knowledge {path.name}: accepted")


def main() -> int:
    try:
        me = login()
        ensure_tenant(me)
        configure_model_policy()
        upsert_application()
        ingest_knowledge()
    except Exception as error:
        print(f"trailforge seed failed: {error}", file=sys.stderr)
        return 1
    print("TrailForge seed complete. Run the cases in examples/trailforge/TEST.md.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
