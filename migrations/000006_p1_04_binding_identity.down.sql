DROP INDEX IF EXISTS ix_channel_binding_audit_created;
DROP TABLE IF EXISTS channel_binding_audit;
DROP INDEX IF EXISTS ix_user_identity_scope;
ALTER TABLE user_identity
    DROP CONSTRAINT IF EXISTS ck_user_identity_scope,
    DROP CONSTRAINT IF EXISTS ck_user_identity_status,
    DROP COLUMN IF EXISTS updated_at,
    DROP COLUMN IF EXISTS version,
    DROP COLUMN IF EXISTS status,
    DROP COLUMN IF EXISTS external_thread_id,
    DROP COLUMN IF EXISTS external_chat,
    DROP COLUMN IF EXISTS scope,
    DROP COLUMN IF EXISTS internal_user_id;
DROP INDEX IF EXISTS ix_channel_binding_status;
DROP INDEX IF EXISTS ux_channel_binding_provider_identity;
ALTER TABLE channel_binding
    DROP COLUMN IF EXISTS verify_token_ref;
ALTER TABLE channel_binding
    DROP CONSTRAINT IF EXISTS ck_channel_binding_target,
    DROP CONSTRAINT IF EXISTS ck_channel_binding_state,
    DROP CONSTRAINT IF EXISTS ck_channel_binding_status,
    DROP COLUMN IF EXISTS external_target_id,
    DROP COLUMN IF EXISTS external_target_type,
    DROP COLUMN IF EXISTS disabled_at,
    DROP COLUMN IF EXISTS expires_at,
    DROP COLUMN IF EXISTS version,
    DROP COLUMN IF EXISTS status;
