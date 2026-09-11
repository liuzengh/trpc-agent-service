-- F01 append-only policy storage and account-local publication head. Runtime
-- projection/relay and owner dependency validation are wired separately.
CREATE TABLE channel_access_policies (
    tenant_id text NOT NULL,
    account_id text NOT NULL,
    provider text NOT NULL CHECK (provider IN ('telegram','wecom')),
    policy_id text NOT NULL CHECK (policy_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    latest_revision bigint NOT NULL CHECK (latest_revision BETWEEN 1 AND 9007199254740991),
    PRIMARY KEY (tenant_id, policy_id),
    UNIQUE (tenant_id, account_id),
    UNIQUE (tenant_id, policy_id, account_id, provider),
    FOREIGN KEY (tenant_id, account_id, provider) REFERENCES channel_accounts(tenant_id,id,provider)
);
CREATE TABLE channel_access_policy_revisions (
    tenant_id text NOT NULL,
    account_id text NOT NULL,
    provider text NOT NULL,
    policy_id text NOT NULL,
    revision bigint NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991),
    digest text NOT NULL CHECK (digest ~ '^sha256:[0-9a-f]{64}$'),
    document_jsonb jsonb NOT NULL CHECK (jsonb_typeof(document_jsonb)='object'),
    published_by text NOT NULL REFERENCES user_accounts(id),
    published_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id,policy_id,revision),
    UNIQUE (tenant_id,policy_id,revision,account_id,provider),
    FOREIGN KEY (tenant_id,policy_id,account_id,provider)
      REFERENCES channel_access_policies(tenant_id,policy_id,account_id,provider),
    CHECK ((document_jsonb->>'tenant_id') IS NOT DISTINCT FROM tenant_id),
    CHECK ((document_jsonb->>'account_id') IS NOT DISTINCT FROM account_id),
    CHECK ((document_jsonb->>'provider') IS NOT DISTINCT FROM provider),
    CHECK ((document_jsonb->>'policy_id') IS NOT DISTINCT FROM policy_id),
    CHECK ((document_jsonb->>'revision') IS NOT DISTINCT FROM revision::text),
    CHECK ((document_jsonb->>'digest') IS NOT DISTINCT FROM digest),
    CHECK ((document_jsonb->>'published_by') IS NOT DISTINCT FROM published_by)
);
-- A committed head must point to a real immutable row. Deferral allows inserting
-- the account head and its first revision in either statement order in one tx.
ALTER TABLE channel_access_policies ADD CONSTRAINT channel_access_policy_head_revision
    FOREIGN KEY (tenant_id,policy_id,latest_revision)
    REFERENCES channel_access_policy_revisions(tenant_id,policy_id,revision)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE channel_access_policy_principals (
    tenant_id text NOT NULL,
    account_id text NOT NULL,
    provider text NOT NULL,
    policy_id text NOT NULL,
    revision bigint NOT NULL,
    principal_id text NOT NULL,
    PRIMARY KEY (tenant_id,policy_id,revision,principal_id),
    FOREIGN KEY (tenant_id,policy_id,revision,account_id,provider)
      REFERENCES channel_access_policy_revisions(tenant_id,policy_id,revision,account_id,provider),
    FOREIGN KEY (tenant_id,principal_id,account_id,provider)
      REFERENCES channel_principal_bindings(tenant_id,principal_id,account_id,provider)
);
CREATE INDEX channel_access_policy_principal_lookup
    ON channel_access_policy_principals(tenant_id,account_id,principal_id,revision);

CREATE FUNCTION channel_access_policy_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION USING ERRCODE='23514', MESSAGE='CHANNEL_POLICY_IMMUTABLE';
END $$;
CREATE TRIGGER channel_access_policy_revision_immutable
    BEFORE UPDATE OR DELETE ON channel_access_policy_revisions
    FOR EACH ROW EXECUTE FUNCTION channel_access_policy_immutable();
CREATE TRIGGER channel_access_policy_members_immutable
    BEFORE UPDATE OR DELETE ON channel_access_policy_principals
    FOR EACH ROW EXECUTE FUNCTION channel_access_policy_immutable();
CREATE FUNCTION channel_access_policy_head_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.tenant_id,NEW.account_id,NEW.provider,NEW.policy_id)
         IS DISTINCT FROM (OLD.tenant_id,OLD.account_id,OLD.provider,OLD.policy_id)
       OR NEW.latest_revision <> OLD.latest_revision+1 THEN
        RAISE EXCEPTION USING ERRCODE='23514', MESSAGE='CHANNEL_POLICY_HEAD_CONFLICT';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER channel_access_policy_head_guard
    BEFORE UPDATE ON channel_access_policies
    FOR EACH ROW EXECUTE FUNCTION channel_access_policy_head_guard();

-- The normalized member set must exactly match the immutable canonical body at
-- commit. A half-written set or a post-publication extra member is not a revision.
CREATE FUNCTION channel_access_policy_members_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE doc jsonb; actual bigint; expected bigint;
BEGIN
    SELECT document_jsonb INTO doc FROM channel_access_policy_revisions
      WHERE tenant_id=NEW.tenant_id AND policy_id=NEW.policy_id AND revision=NEW.revision;
    expected := jsonb_array_length(doc->'body'->'allowed_principal_ids');
    SELECT count(*) INTO actual FROM channel_access_policy_principals
      WHERE tenant_id=NEW.tenant_id AND policy_id=NEW.policy_id AND revision=NEW.revision;
    IF expected IS NULL OR expected <> actual OR EXISTS (
      SELECT 1 FROM channel_access_policy_principals m
      WHERE m.tenant_id=NEW.tenant_id AND m.policy_id=NEW.policy_id AND m.revision=NEW.revision
        AND NOT ((doc->'body'->'allowed_principal_ids') ? m.principal_id)
    ) THEN
      RAISE EXCEPTION USING ERRCODE='23514', MESSAGE='CHANNEL_POLICY_MEMBERS_INCOMPLETE';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER channel_access_policy_members_complete_revision
    AFTER INSERT ON channel_access_policy_revisions DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION channel_access_policy_members_complete();
CREATE CONSTRAINT TRIGGER channel_access_policy_members_complete_member
    AFTER INSERT ON channel_access_policy_principals DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION channel_access_policy_members_complete();

-- Policy publication success receipts commit with the immutable revision and
-- outboxes. Preserve all previously registered Channel command operations.
ALTER TABLE channel_command_receipts DROP CONSTRAINT channel_command_receipts_operation_check;
ALTER TABLE channel_command_receipts ADD CONSTRAINT channel_command_receipts_operation_check
    CHECK (operation IN ('CreateChannelAccount','UpdateChannelAccount',
      'UpdateAccountCredential','SetChannelAccountEnabled','CreateChannelBinding',
      'SetChannelBindingTarget','SetChannelBindingEnabled','RegisterChannelPrincipal',
      'SetChannelPrincipalState','PublishChannelAccessPolicy'));
