# Channel V1 HTTP contracts

These closed JSON Schemas belong to Control's ChannelAccount/ChannelBinding
module. Gateway consumes snapshot, credential-resolution and observation wires;
it does not import Control's internal Domain or Application packages.

- `account-create/update/enabled`, `credential-update`, `binding-create/target/enabled`
  describe tenant management request bodies. Paths, OWNER access, request byte
  limits, Idempotency-Key and transactional CAS are enforced by adapters/use cases.
- `account-snapshot` is a complete, unpaginated metadata snapshot. Digest input is
  exactly `{scope_id,source_epoch,snapshot_revision,accounts}`, serialized with
  RFC 8785 JCS, then SHA-256 with a `sha256:` prefix. Accounts sort by provider
  then account_id; credentials sort by purpose. Cross-field identities, uniqueness,
  required configured credentials for enabled accounts and digest integrity are
  semantic checks beyond Schema.
- `credentials-resolve-request` requires an exact consumer-specific purpose set.
  `telegram_registration` receives both purposes from one connection revision.
  The response intentionally has no schema_version field, matching the reviewed
  protocol. Raw values exist only in this authenticated, noncacheable response.
- `observations` is `{schema_version:1,observations:[...]}`. Reason codes are closed;
  free-form provider errors and credential-bearing messages are rejected.
- `common` supplies internal references. No consumer should validate against the
  permissive root of `common` as if it were a request schema.

The Go helper rejects duplicate keys, unknown/case-varied fields and invalid UTF-8.
It returns redacted classifications, not Schema errors containing submitted values.
A Schema match does not authorize a request or prove provider readiness.

Fixtures contain explicit test-only credentials. `snapshot-shape.json` tests wire
shape only; its placeholder digest is not an executable authoritative snapshot.
Actual digest validation and identity binding are tested in the owning Domain.

## Telegram diagnostic preflight

Eight independent `preflight-*.schema.json` contracts describe create/created,
view, claim/grant, resolve/resolved, and complete. The public surface exposes two
Session operations; three diagnostic mTLS operations have no normal runtime
consumer bypass. `telegram_preflight` is a workload authorization kind, **not**
a stored credential purpose. Only the dedicated resolved response carries a
`telegram.bot_token` value; no public/claim/completion object contains it.

`PreflightConfigDigest` validates canonical static origin, scope/epoch, and
redacted invalid-origin representation before hashing the frozen JCS object.
It performs no DNS/network activity. The shared golden vectors and 67-origin
matrix align with Gateway. `ValidatePreflightChecks` verifies the eight ordered,
closed check facts, their code/status/details and remote-call prerequisites;
only the first six determine outcome. The final recovery and delivery items stay
UNKNOWN, so diagnostic PASS never asserts restoration or real message delivery.
`Validate`/`Decode` also enforce these preflight cross-field semantics, digest
consistency, redacted public origin and exact path identities.

JSON Schema 2020-12 `prefixItems` closes the ordered check tuple. `const: null`
represents null exactly; `items: {}` with min/maxItems=8 does not permit a ninth
item. Authorization, current version checks, claim fencing, server deadlines,
rate limits and transactional completion remain in the consuming Application.

## Telegram receive modes

New Telegram creates default to `config.receive_mode=long_polling`; explicit
`webhook` requires both credentials. Both modes retain the two stable credential
metadata entries and generated webhook path. A disabled account can change mode
with `expected_account_revision`; saving configuration performs no Telegram I/O.

Explicit recovery of a pre-upgrade pending create uses
`X-Channel-Create-Contract: webhook-v1`, its original idempotency key, and the
original body with no `config` field and both replacement credentials. Without
that header, omitted mode means new long-polling semantics. Channel mutation
responses identify the response interpretation with
`X-Channel-Result-Contract: webhook-v1` for an original historical account result
whose config lacks mode, or `receive-modes-v1` otherwise. Historical receipts and
preflight results are decoded as historical shapes, not reinterpreted using the
account's current mode.

`telegram_receiver` requires `owner_epoch`, forbids `registration_epoch`, and
resolves only `telegram.bot_token` for either mode. Control authenticates the
consumer and exact account/version/use; Gateway separately validates actual
receiver ownership. `telegram_registration` and `telegram_webhook` are webhook
only. Delivery works for either mode. New Telegram observations include mode;
long-polling READY requires owner epoch. A standby reports CONFIG_APPLIED with
WAITING_FOR_OWNER, not READY. RECEIVER_DRAINING, WEBHOOK_CONFLICT and
POLLING_CONFLICT are closed, redacted reason codes. Missing observation mode is
legacy webhook and must not carry owner epoch.

New preflight tasks freeze `receive_mode` and
`diagnostic_policy=telegram-receive-modes-v1`. Claim advertises that policy;
legacy claims consume legacy tasks only. Grants, claimed views and completions
also carry `effective_config_digest`. Completions additionally carry the fixed
`connection_revision` and global `origin_status`. QUEUED views have no effective
digest. `PreflightConfigDigest` remains the global instance config evidence.
`PreflightEffectiveConfigDigest` binds scope, source epoch, connection revision,
mode and policy, but replaces irrelevant inbound origin with NOT_APPLICABLE for
long polling. A re-lease may accept a changed global origin when that effective
digest stays equal; an existing lease does not silently replace its config.

`ValidatePreflightChecksForMode` preserves the original webhook rules. Long
polling fixes checks 3, 6 and 7 to NOT_APPLICABLE; check 4 is WEBHOOK_NONE/PASS or
WEBHOOK_BLOCKS_LONG_POLLING/FAIL (or a closed remote error). Bot token is required;
webhook secret is optional. Check 8 remains DELIVERY_NOT_TESTED/UNKNOWN. Only
applicable checks among 1–6 determine the outcome; no diagnostic clears a webhook,
starts polling or proves delivery.

The `preflight-polling-*-valid.json` and `preflight-mode-claim-valid.json`
fixtures are the exact cross-task wire examples. Their `*-invalid.json`
companions cover missing policy, wrong effective digest, an origin check pretending
to apply to polling, and a webhook-secret requirement leaking into polling.
