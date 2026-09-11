-- Historical policy content and account-local continuity, NOT current authority.
-- No freshness timestamp is created by applying a historical notification.
CREATE TABLE gateway_policy_projection_heads (
 scope_id text NOT NULL,
 source_epoch text NOT NULL,
 tenant_id text NOT NULL,
 account_id text NOT NULL,
 provider text NOT NULL CHECK(provider IN ('telegram','wecom')),
 policy_id text NOT NULL,
 observed_revision bigint NOT NULL DEFAULT 0 CHECK(observed_revision BETWEEN 0 AND 9007199254740991),
 contiguous_revision bigint NOT NULL DEFAULT 0 CHECK(contiguous_revision BETWEEN 0 AND observed_revision),
 blocked_reason text NOT NULL DEFAULT '' CHECK(blocked_reason IN ('','IDENTITY_CONFLICT','REVISION_CONFLICT')),
 PRIMARY KEY(scope_id,source_epoch,account_id),
 UNIQUE(scope_id,source_epoch,tenant_id,account_id,provider,policy_id)
);
CREATE TABLE gateway_policy_projection_documents (
 scope_id text NOT NULL,
 source_epoch text NOT NULL,
 tenant_id text NOT NULL,
 account_id text NOT NULL,
 provider text NOT NULL,
 policy_id text NOT NULL,
 revision bigint NOT NULL CHECK(revision BETWEEN 1 AND 9007199254740991),
 digest text NOT NULL CHECK(digest ~ '^sha256:[0-9a-f]{64}$'),
 document_jsonb jsonb NOT NULL CHECK(jsonb_typeof(document_jsonb)='object'),
 PRIMARY KEY(scope_id,source_epoch,account_id,revision),
 FOREIGN KEY(scope_id,source_epoch,tenant_id,account_id,provider,policy_id)
 REFERENCES gateway_policy_projection_heads(scope_id,source_epoch,tenant_id,account_id,provider,policy_id),
 CHECK(document_jsonb->>'tenant_id' IS NOT DISTINCT FROM tenant_id),
 CHECK(document_jsonb->>'account_id' IS NOT DISTINCT FROM account_id),
 CHECK(document_jsonb->>'provider' IS NOT DISTINCT FROM provider),
 CHECK(document_jsonb->>'policy_id' IS NOT DISTINCT FROM policy_id),
 CHECK(document_jsonb->>'digest' IS NOT DISTINCT FROM digest),
 CHECK((document_jsonb->>'revision')::bigint IS NOT DISTINCT FROM revision)
);
CREATE FUNCTION gateway_policy_document_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'gateway policy document is immutable' USING ERRCODE='23514';
END;
$$;
CREATE TRIGGER gateway_policy_document_immutable BEFORE UPDATE OR DELETE
 ON gateway_policy_projection_documents FOR EACH ROW EXECUTE FUNCTION gateway_policy_document_immutable();
CREATE FUNCTION gateway_policy_head_fence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF ROW(NEW.scope_id,NEW.source_epoch,NEW.tenant_id,NEW.account_id,NEW.provider,NEW.policy_id)
    IS DISTINCT FROM ROW(OLD.scope_id,OLD.source_epoch,OLD.tenant_id,OLD.account_id,OLD.provider,OLD.policy_id)
    OR NEW.observed_revision < OLD.observed_revision
    OR NEW.contiguous_revision < OLD.contiguous_revision
    OR (OLD.blocked_reason <> '' AND NEW.blocked_reason <> OLD.blocked_reason) THEN
  RAISE EXCEPTION 'gateway policy head fence violation' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER gateway_policy_head_fence BEFORE UPDATE ON gateway_policy_projection_heads
 FOR EACH ROW EXECUTE FUNCTION gateway_policy_head_fence();
