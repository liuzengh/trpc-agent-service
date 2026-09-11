-- Current tenant governance policy. Revision is concurrency control, not a
-- historical product: replacing a policy updates this single current row.
CREATE TABLE tenant_usage_policies (
 tenant_id text PRIMARY KEY REFERENCES tenants(id),
 revision bigint NOT NULL CHECK(revision BETWEEN 1 AND 9007199254740991),
 policy_jsonb jsonb NOT NULL CHECK(jsonb_typeof(policy_jsonb)='object'),
 updated_by text NOT NULL REFERENCES user_accounts(id),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(policy_jsonb->>'tenant_id' IS NOT DISTINCT FROM tenant_id),
 CHECK((policy_jsonb->>'revision')::bigint IS NOT DISTINCT FROM revision),
 CHECK((policy_jsonb->>'schema_version')::int IS NOT DISTINCT FROM 1)
);
CREATE TABLE tenant_usage_policy_receipts (
 tenant_id text NOT NULL REFERENCES tenants(id),
 idempotency_key text NOT NULL,
 request_digest text NOT NULL CHECK(request_digest ~ '^sha256:[0-9a-f]{64}$'),
 result_jsonb jsonb NOT NULL CHECK(jsonb_typeof(result_jsonb)='object'),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,idempotency_key)
);
