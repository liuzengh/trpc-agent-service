-- SPEC-03 IM providers use authenticated long connections. The old callback
-- credentials are no longer part of a Binding or any provider adapter.
ALTER TABLE platform.channel_binding
    DROP COLUMN IF EXISTS external_account_scope,
    DROP COLUMN IF EXISTS webhook_url,
    DROP COLUMN IF EXISTS token_ref,
    DROP COLUMN IF EXISTS signing_secret_ref;

ALTER TABLE platform.channel_binding
    ALTER COLUMN public_route_id DROP NOT NULL,
    ALTER COLUMN public_route_id DROP DEFAULT;

ALTER TABLE platform.channel_binding
    DROP CONSTRAINT IF EXISTS channel_binding_public_route_id_check;

CREATE OR REPLACE FUNCTION platform.bump_channel_binding_revision()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.app_id IS DISTINCT FROM OLD.app_id
       OR NEW.binding_id IS DISTINCT FROM OLD.binding_id THEN
        RAISE EXCEPTION 'channel binding scope is immutable' USING ERRCODE = '55000';
    END IF;

    IF NEW.channel IS DISTINCT FROM OLD.channel
       OR NEW.external_account IS DISTINCT FROM OLD.external_account
       OR NEW.secret_ref IS DISTINCT FROM OLD.secret_ref
       OR NEW.status IS DISTINCT FROM OLD.status
       OR NEW.public_route_id IS DISTINCT FROM OLD.public_route_id THEN
        NEW.binding_revision := OLD.binding_revision + 1;
    ELSE
        NEW.binding_revision := OLD.binding_revision;
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;
