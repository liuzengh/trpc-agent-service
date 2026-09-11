"""WV-09: same private chat, three authenticated Accounts across two Tenants.

Control owner HTTP publishes the second Account and a separate Tenant's complete
Agent/Profile/Deployment/Account. Actual Worker SDK history, immutable Session
parents, Gateway Delivery target and external bot identity are cross-checked.
No business SQL writes or group-input capability changes are used.

Harness.seed(extra_tenants=1) creates an extra Tenant for the same owner while
the initial administrator cookie is still active. The current owner session then
manages both Tenants; no old bootstrap administrator password is reused.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import uuid

from faults import one, rows
from gateway_fixture import EPOCH, INSTANCE, SCOPE
from scope_scenarios import await_route, binding_path, scope_round


FIELDS = ("tenant_id", "agent_id", "profile_id", "deployment_id", "revision_id",
          "revision_number", "manifest_id", "manifest_digest")


def publish_tenant(h, tenant_id, marker):
    """Create through the actual owner API, retaining the immutable revision."""
    base = "/v1/tenants/" + tenant_id
    agent = h.api("POST", base + "/agents", {"name": "Identity fixture agent " + marker}, status=201)["agent"]
    spec = {"schema_version": "v1", "root": "assistant",
            "requirements": {"models": {"primary": {"capabilities": ["chat"]}}, "tools": {}, "knowledge": {}},
            "nodes": {"assistant": {"kind": "llm", "instruction": "Reply with a short final answer.",
                                     "model_slot": "primary", "tool_slots": [], "knowledge_slots": []}}}
    h.api("PUT", base + "/agents/" + agent["id"] + "/draft", {"expected_revision": 1, "spec": spec})
    h.api("POST", base + "/agents/" + agent["id"] + "/versions", {"expected_revision": 2}, status=201)
    profile = h.api("POST", base + "/runtime-profiles", {"name": "Identity fixture profile " + marker}, status=201)["profile"]
    config = {"models": {"primary": {"kind": "openai_compatible", "model": h.model_name,
                                      "base_url": h.model.url + "/v1", "capabilities": ["chat"]}},
              "tools": {}, "knowledge": {}, "storage": {"session": {"kind": "postgres_state", "destination": {
                  "host": "127.0.0.1", "port": h.session_port, "database": "agent_platform", "username": "session_runtime", "sslmode": "disable"}}}}
    credentials = {"models": {"primary": {"api_key": {"action": "replace", "value": h.model.key}}},
                   "storage": {"session": {"dsn": {"action": "replace", "value": h.dsns["session_runtime"]}}}}
    h.api("PUT", base + "/runtime-profiles/" + profile["id"] + "/draft",
          {"expected_draft_revision": 1, "credential_protocol_version": "v1", "config": config, "credentials": credentials}, idem=marker + "-profile")
    h.api("POST", base + "/runtime-profiles/" + profile["id"] + "/revisions", {"expected_revision": 2}, status=201)
    deployment = h.api("POST", base + "/deployments", {"name": "Identity fixture deployment " + marker}, status=201, idem=marker + "-deployment")["deployment"]
    source = {"schema_version": "v1", "agent": {"agent_id": agent["id"], "version_number": 1},
              "profile": {"profile_id": profile["id"], "revision_number": 1}}
    checked = h.api("POST", base + "/deployments/" + deployment["id"] + "/validate", source)
    assert checked["valid"] is True, "second Tenant publication failed actual Control validation"
    publication = h.api("POST", base + "/deployments/" + deployment["id"] + "/revisions",
                        {"expected_latest_revision_number": None, "input": source}, status=201, idem=marker + "-publish")
    revision = publication["revision"]
    target = {"tenant_id": tenant_id, "agent_id": agent["id"], "profile_id": profile["id"],
              "deployment_id": deployment["id"], "revision_id": revision["id"],
              "revision_number": revision["revision_number"], "manifest_id": revision["manifest_id"],
              "manifest_digest": revision["manifest_digest"]}
    h.wait(lambda: h.sql("SELECT content_digest FROM worker.runtime_manifests WHERE tenant_id=" + h.quote(tenant_id) +
                         " AND manifest_id=" + h.quote(target["manifest_id"])) == [[target["manifest_digest"]]],
           "actual Worker receives second Tenant immutable Manifest", timeout=40)
    return target


def create_account(h, target, bot, label, created):
    base = "/v1/tenants/" + target["tenant_id"]
    value = h.api("POST", base + "/channel-accounts",
                  {"provider": "telegram", "provider_account_id": str(bot["bot_id"]), "name": "Identity fixture " + label,
                   "config": {"receive_mode": "webhook"},
                   "credentials": {"telegram.bot_token": {"action": "replace", "value": bot["token"]},
                                   "telegram.webhook_secret": {"action": "replace", "value": bot["secret"]}}},
                  status=201, idem=label + "-account")
    identity = {**target, "account_id": value["account"]["account_id"], "bot_id": bot["bot_id"], "webhook_secret": bot["secret"]}
    created.append(identity)  # even a later Binding failure leaves a managed Account to disable
    binding = h.api("POST", base + "/channel-bindings",
                    {"account_id": identity["account_id"], "target": {"deployment_id": target["deployment_id"], "revision_number": target["revision_number"]}},
                    status=201, idem=label + "-binding")
    identity["binding_id"] = binding["binding"]["binding_id"]
    assert binding["binding"]["target"]["deployment_revision_id"] == target["revision_id"]
    assert binding["binding"]["target"]["manifest_digest"] == target["manifest_digest"]
    h.api("POST", base + "/channel-accounts/" + identity["account_id"] + "/enabled",
          {"expected_account_revision": value["account"]["account_revision"], "enabled": True}, idem=label + "-account-enable")
    h.api("POST", base + "/channel-bindings/" + identity["binding_id"] + "/enabled",
          {"expected_binding_revision": binding["binding"]["binding_revision"], "enabled": True}, idem=label + "-binding-enable")
    return identity


def select_identity(h, identity):
    for field in FIELDS:
        setattr(h, field, identity[field])
    for field in ("account_id", "binding_id", "bot_id", "webhook_secret"):
        setattr(h.gateway, field, identity[field])


def await_identity(h, identity):
    view = h.api("GET", binding_path(h))
    await_route(h, view)
    query = ("SELECT provider_account_id,tenant_id,enabled,present FROM gateway.gateway_account_directory WHERE scope_id=" +
             h.quote(SCOPE) + " AND account_id=" + h.quote(identity["account_id"]))
    expected = [[str(identity["bot_id"]), identity["tenant_id"], "t", "t"]]
    h.wait(lambda: h.sql(query) == expected, "Gateway catalog fixes actual Tenant/Account/bot identity", timeout=40)
    def registered():
        expected_url = h.urls["gateway_ingress"].rstrip("/") + "/v1/telegram/" + identity["account_id"]
        return bool(rows(h, "SELECT r.owner_epoch FROM gateway.gateway_telegram_receivers r "
                         "JOIN gateway.gateway_account_directory d USING(scope_id,account_id) "
                         "JOIN gateway.gateway_account_qualifications q ON q.scope_id=r.scope_id "
                         "AND q.source_epoch=r.source_epoch AND q.instance_id=r.instance_id "
                         "AND q.instance_epoch=r.instance_epoch "
                         "WHERE r.scope_id=" + h.quote(SCOPE) + " AND r.source_epoch=" + h.quote(EPOCH) +
                         " AND r.account_id=" + h.quote(identity["account_id"]) +
                         " AND r.bot_id=" + h.quote(identity["bot_id"]) +
                         " AND r.bot_id=d.provider_account_id AND d.tenant_id=" + h.quote(identity["tenant_id"]) +
                         " AND d.source_epoch=r.source_epoch AND d.connection_revision=r.connection_revision "
                         "AND d.enabled AND d.present AND d.account_json->'config'->>'receive_mode'='webhook' "
                         "AND r.managed_revision=r.connection_revision AND r.managed_url=" + h.quote(expected_url) +
                         " AND r.owner_epoch>0 AND r.instance_id=" + h.quote(INSTANCE) +
                         " AND r.lease_until>clock_timestamp() AND r.last_reason='NONE' "
                         "AND r.next_due>clock_timestamp() AND q.enabled AND q.valid_until>clock_timestamp()"))
    h.wait(registered, "current receiver getMe/getWebhookInfo/setWebhook qualifies selected bot", timeout=40)
    return view


def disable_identity(h, identity, marker):
    base = "/v1/tenants/" + identity["tenant_id"]
    if identity.get("binding_id"):
        path = base + "/channel-bindings/" + identity["binding_id"]
        view = h.api("GET", path)
        if view["binding"]["enabled"]:
            h.api("POST", path + "/enabled", {"expected_binding_revision": view["binding"]["binding_revision"], "enabled": False}, idem=marker + "-binding-disable")
    path = base + "/channel-accounts/" + identity["account_id"]
    view = h.api("GET", path)
    if view["account"]["enabled"]:
        h.api("POST", path + "/enabled", {"expected_account_revision": view["account"]["account_revision"], "enabled": False}, idem=marker + "-account-disable")
    h.wait(lambda: h.sql("SELECT enabled FROM gateway.gateway_account_directory WHERE scope_id=" + h.quote(SCOPE) +
                         " AND account_id=" + h.quote(identity["account_id"])) == [["f"]],
           "temporary bot disabled in actual Gateway catalog", timeout=40)


def assert_identity_isolation(rounds):
    assert len(rounds) == 6, "identity gate requires two actual rounds per Account"
    first = rounds[:3]
    assert len({r["session_id"] for r in first}) == 3, "same chat crossed Account/Tenant Session identity"
    assert len({r["identity"]["account_id"] for r in first}) == 3
    assert first[0]["identity"]["tenant_id"] == first[1]["identity"]["tenant_id"]
    assert first[0]["identity"]["tenant_id"] != first[2]["identity"]["tenant_id"]
    assert first[0]["identity"]["manifest_digest"] == first[1]["identity"]["manifest_digest"]
    for index in range(3):
        a, b = rounds[index], rounds[index + 3]
        assert a["session_sequence"] == 1 and b["session_sequence"] == 2
        assert a["session_id"] == b["session_id"]
        assert b["candidate_parent_ref"] == a["completion"]["candidate_ref"]
        assert b["candidate_parent_digest"] == a["completion"]["candidate_digest"]
    assert len({r["run_id"] for r in rounds}) == len({r["delivery"]["intent_id"] for r in rounds}) == 6


def run_identity(h):
    if not getattr(h, "extra_tenant_ids", []):
        raise ValueError("identity gate requires Harness.seed(extra_tenants=1)")
    if not any(process.poll() is None for process in h.workers.values()):
        h.start_worker()
    original = {name: getattr(h, name) for name in FIELDS}
    original.update({name: getattr(h.gateway, name) for name in ("account_id", "binding_id", "bot_id", "webhook_secret")})
    original_binding = h.api("GET", binding_path(h))
    # A preceding revision-switch gate may have published revision 2. Reuse the
    # currently bound immutable revision, never assume the Deployment latest is 1.
    target = original_binding["binding"]["target"]
    assert target["deployment_revision_id"] == original["revision_id"]
    active = {**original, "revision_number": target["revision_number"]}
    fixture_before = (h.gateway_fixture.bot_id, h.gateway_fixture.token, h.gateway_fixture.secret, h.gateway_fixture.bot_ids())
    marker = "identity-" + uuid.uuid4().hex[:12]
    conversation = str(2500000000 + int(uuid.uuid4().hex[:7], 16))
    created = []
    bots = []
    evidence = {"version": "worker-v1-tenant-account-identity/v1", "criterion": "WV-09", "rounds": [],
                "conversation_id": conversation, "chat_type": "private", "product_sql_writes": 0,
                "external_fixtures": ["three authenticated Telegram bots", "model"],
                "coverage": ["same private chat and Tenant, different Account/Binding, same immutable Manifest",
                             "same private chat, different Tenant with independent Agent/Profile/Deployment/Account",
                             "return to each Account reuses only its own accepted Session history"], "restoration": {}}
    path = Path(h.artifacts) / "worker-identity-scenarios.json"
    try:
        for bot_id in (987655, 987656):
            bot = h.gateway_fixture.add_bot(bot_id)
            bots.append(bot)
            h.secrets.extend([bot["token"], bot["secret"]])
        a2 = create_account(h, active, bots[0], marker + "-a2", created)
        second = publish_tenant(h, h.extra_tenant_ids[0], marker + "-tenant-b")
        b1 = create_account(h, second, bots[1], marker + "-b1", created)
        identities = [active, a2, b1]
        evidence["identities"] = [{key: identity[key] for key in (*FIELDS, "account_id", "binding_id", "bot_id")} for identity in identities]
        history = [[], [], []]
        for index in range(6):
            group = index % 3
            identity = identities[group]
            select_identity(h, identity)
            view = await_identity(h, identity)
            text = marker + "-" + ("a1", "a2", "b1")[group] + "-" + str(index // 3 + 1)
            history[group].append(text)
            previous = None if index < 3 else evidence["rounds"][index - 3]
            result = scope_round(h, text, conversation, view, history[group], previous)
            actual_target = one(h, "SELECT target FROM gateway.gateway_delivery_intents WHERE run_id=" + h.quote(result["run_id"]))["target"]
            assert actual_target["TenantID"] == identity["tenant_id"] and actual_target["AccountID"] == identity["account_id"]
            assert actual_target["ManifestDigest"] == identity["manifest_digest"] and actual_target["ConversationID"] == conversation
            external = [message for message in h.gateway_fixture.snapshot() if message["text"] == result["delivery"]["final_text"]]
            assert len(external) == 1 and external[0]["bot_id"] == identity["bot_id"]
            result["identity"] = {key: identity[key] for key in ("tenant_id", "account_id", "binding_id", "manifest_digest", "bot_id")}
            result["delivery_target"] = actual_target
            result["external_bot_id"] = external[0]["bot_id"]
            evidence["rounds"].append(result)
        assert_identity_isolation(evidence["rounds"])
        evidence["result"] = "PASS"
        print("WORKER_IDENTITY_SCOPE=PASS actual same private chat across three authenticated Accounts/two Tenants; six SDK/Session/Final rounds preserve three isolated accepted histories and route each reply to its original bot", flush=True)
        return evidence
    except BaseException:
        evidence["result"] = "FAIL"
        raise
    finally:
        try:
            for index, identity in enumerate(reversed(created)):
                disable_identity(h, identity, marker + "-restore-" + str(index))
                evidence["restoration"][identity["account_id"]] = "disabled_through_Control_API"
        finally:
            select_identity(h, original)
            for bot in bots:
                h.gateway_fixture.remove_bot(bot["bot_id"])
            assert (h.gateway_fixture.bot_id, h.gateway_fixture.token, h.gateway_fixture.secret, h.gateway_fixture.bot_ids()) == fixture_before
            assert all(getattr(h, field) == original[field] for field in FIELDS)
            restored = h.api("GET", binding_path(h))
            assert restored["binding"] == original_binding["binding"]
            evidence["restoration"].update(h_identity_and_binding_restored=True, default_bot_unchanged=True,
                                           remaining_fixture_bot_ids=h.gateway_fixture.bot_ids())
            path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n")
            assert json.loads(path.read_text()) == evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--race", action="store_true")
    args = parser.parse_args()
    from harness import Harness
    import gateway_fixture
    h = Harness(args.root, args.artifacts, args.race)
    try:
        h.provision()
        h.control_start(gateway_fixture.prepare(h))
        h.seed(extra_tenants=1)
        h.start_worker()
        h.verify_dependencies()
        h.gateway = gateway_fixture.start(h)
        run_identity(h)
    finally:
        h.close()


if __name__ == "__main__":
    main()
