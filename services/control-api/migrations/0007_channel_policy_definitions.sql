-- Control-owned Session/Quota definitions, separate from Worker session facts
-- and consumption ledgers. Command publication and runtime projections follow.
CREATE TABLE channel_policy_definition_revisions (
    tenant_id text NOT NULL REFERENCES tenants(id),
    kind text NOT NULL CHECK (kind IN ('session','quota')),
    policy_id text NOT NULL CHECK (policy_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    revision bigint NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991),
    digest text NOT NULL CHECK (digest ~ '^sha256:[0-9a-f]{64}$'),
    document_jsonb jsonb NOT NULL CHECK (jsonb_typeof(document_jsonb)='object'),
    published_by text NOT NULL REFERENCES user_accounts(id),
    published_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id,kind,policy_id,revision),
    CHECK ((document_jsonb->>'tenant_id') IS NOT DISTINCT FROM tenant_id),
    CHECK ((document_jsonb->>'kind') IS NOT DISTINCT FROM kind),
    CHECK ((document_jsonb->>'policy_id') IS NOT DISTINCT FROM policy_id),
    CHECK ((document_jsonb->>'revision') IS NOT DISTINCT FROM revision::text),
    CHECK ((document_jsonb->>'digest') IS NOT DISTINCT FROM digest),
    CHECK ((document_jsonb->>'published_by') IS NOT DISTINCT FROM published_by)
);
CREATE FUNCTION channel_policy_definition_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION USING ERRCODE='23514', MESSAGE='CHANNEL_POLICY_DEFINITION_IMMUTABLE';
END $$;
CREATE TRIGGER channel_policy_definition_immutable
    BEFORE UPDATE OR DELETE ON channel_policy_definition_revisions
    FOR EACH ROW EXECUTE FUNCTION channel_policy_definition_immutable();

CREATE TABLE channel_policy_definition_receipts (
    tenant_id text NOT NULL REFERENCES tenants(id),
    kind text NOT NULL CHECK (kind IN ('session','quota')),
    policy_id text NOT NULL CHECK (policy_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    key_hash text NOT NULL CHECK (key_hash ~ '^[0-9a-f]{64}$'),
    mac_key_id text NOT NULL CHECK (btrim(mac_key_id)<>''),
    request_mac text NOT NULL CHECK (request_mac ~ '^[0-9a-f]{64}$'),
    result_jsonb jsonb NOT NULL CHECK (jsonb_typeof(result_jsonb)='object'),
    created_by text NOT NULL REFERENCES user_accounts(id),
    created_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id,kind,policy_id,key_hash)
);
CREATE TRIGGER channel_policy_definition_receipt_immutable
    BEFORE UPDATE OR DELETE ON channel_policy_definition_receipts
    FOR EACH ROW EXECUTE FUNCTION channel_policy_definition_immutable();
