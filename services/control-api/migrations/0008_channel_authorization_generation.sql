-- A database-owned account-local fence covers every principal/head/account
-- mutation, including direct SQL. Pages either share this generation or fail;
-- timestamps and independently paginated mutable rows are not a snapshot.
CREATE TABLE channel_authorization_generations (
    tenant_id text NOT NULL,
    account_id text NOT NULL,
    generation bigint NOT NULL DEFAULT 1
      CHECK (generation BETWEEN 1 AND 9007199254740991),
    PRIMARY KEY (tenant_id,account_id),
    FOREIGN KEY (tenant_id,account_id) REFERENCES channel_accounts(tenant_id,id)
);
INSERT INTO channel_authorization_generations(tenant_id,account_id)
    SELECT tenant_id,id FROM channel_accounts;

CREATE FUNCTION channel_authorization_generation_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION USING ERRCODE='23514', MESSAGE='CHANNEL_AUTHORIZATION_GENERATION_IMMUTABLE';
    END IF;
    IF (NEW.tenant_id,NEW.account_id) IS DISTINCT FROM (OLD.tenant_id,OLD.account_id)
       OR NEW.generation <> OLD.generation+1 THEN
        RAISE EXCEPTION USING ERRCODE='23514', MESSAGE='CHANNEL_AUTHORIZATION_GENERATION_CONFLICT';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER channel_authorization_generation_guard
    BEFORE UPDATE OR DELETE ON channel_authorization_generations
    FOR EACH ROW EXECUTE FUNCTION channel_authorization_generation_guard();

CREATE FUNCTION channel_authorization_changed() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner_tenant text; owner_account text;
BEGIN
    IF TG_TABLE_NAME='channel_accounts' THEN
        owner_tenant := NEW.tenant_id; owner_account := NEW.id;
        IF TG_OP='INSERT' THEN
            INSERT INTO channel_authorization_generations(tenant_id,account_id)
                VALUES(owner_tenant,owner_account);
            RETURN NULL;
        END IF;
    ELSIF TG_OP='DELETE' THEN
        owner_tenant := OLD.tenant_id; owner_account := OLD.account_id;
    ELSE
        owner_tenant := NEW.tenant_id; owner_account := NEW.account_id;
        -- Moving a principal between accounts would otherwise fail to invalidate
        -- the old account's proof. Identity moves are not supported.
        IF TG_OP='UPDATE' AND (NEW.tenant_id,NEW.account_id,NEW.provider)
              IS DISTINCT FROM (OLD.tenant_id,OLD.account_id,OLD.provider) THEN
            RAISE EXCEPTION USING ERRCODE='23514', MESSAGE='CHANNEL_AUTHORIZATION_IDENTITY_CONFLICT';
        END IF;
    END IF;
    UPDATE channel_authorization_generations SET generation=generation+1
        WHERE tenant_id=owner_tenant AND account_id=owner_account;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE='23514', MESSAGE='CHANNEL_AUTHORIZATION_GENERATION_MISSING';
    END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER channel_authorization_account_changed AFTER INSERT OR UPDATE ON channel_accounts
    FOR EACH ROW EXECUTE FUNCTION channel_authorization_changed();
CREATE TRIGGER channel_authorization_principal_changed AFTER INSERT OR UPDATE OR DELETE ON channel_principal_bindings
    FOR EACH ROW EXECUTE FUNCTION channel_authorization_changed();
CREATE TRIGGER channel_authorization_policy_changed AFTER INSERT OR UPDATE OR DELETE ON channel_access_policies
    FOR EACH ROW EXECUTE FUNCTION channel_authorization_changed();
-- Byte ordering matches the wire digest and cursor comparisons on every locale.
CREATE INDEX channel_principal_authorization_page
    ON channel_principal_bindings(tenant_id,account_id,provider,principal_id COLLATE "C");
