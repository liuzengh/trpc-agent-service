-- Preserve the effective mode of every deployed Telegram account. The catalog
-- watermark changes with the wire representation; no credential or route moves.
WITH changed AS (
    UPDATE channel_accounts
    SET config_jsonb = config_jsonb || '{"receive_mode":"webhook"}'::jsonb,
        account_revision = account_revision + 1,
        connection_revision = connection_revision + 1,
        updated_at = clock_timestamp()
    WHERE provider = 'telegram' AND NOT (config_jsonb ? 'receive_mode')
    RETURNING scope_id
)
UPDATE channel_account_catalog
SET snapshot_revision = snapshot_revision + 1
WHERE scope_id IN (SELECT scope_id FROM changed);

ALTER TABLE channel_accounts ADD CONSTRAINT channel_accounts_receive_mode CHECK (
    (provider = 'telegram' AND config_jsonb ? 'receive_mode'
      AND jsonb_typeof(config_jsonb->'receive_mode') = 'string'
      AND config_jsonb->>'receive_mode' IN ('long_polling','webhook'))
    OR (provider = 'wecom' AND NOT (config_jsonb ? 'receive_mode'))
);

-- A physical Telegram Bot is exclusive across all scopes and tenants, even
-- while disabled. Existing duplicates fail the transaction; no winner is picked.
CREATE UNIQUE INDEX channel_accounts_telegram_identity
    ON channel_accounts(provider_account_id) WHERE provider = 'telegram';

-- Old observations are webhook observations. Their original digest stays intact
-- so an old instance's identical sequence can still be acknowledged faithfully.
ALTER TABLE channel_account_observations ADD COLUMN receive_mode text;
UPDATE channel_account_observations SET receive_mode='webhook' WHERE provider='telegram';
ALTER TABLE channel_account_observations DROP CONSTRAINT channel_account_observations_check;
ALTER TABLE channel_account_observations ADD CONSTRAINT channel_observations_receive_mode CHECK (
    (provider='telegram' AND receive_mode IS NOT NULL AND receive_mode IN ('long_polling','webhook'))
    OR (provider='wecom' AND receive_mode IS NULL)
);
ALTER TABLE channel_account_observations ADD CONSTRAINT channel_observations_polling_owner CHECK (
    provider <> 'telegram' OR receive_mode <> 'long_polling' OR state <> 'READY' OR owner_epoch IS NOT NULL
);
