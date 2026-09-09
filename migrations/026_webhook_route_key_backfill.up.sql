-- Separate the callback routing capability from provider verification
-- credentials. The initial schema stored a value called webhook_token; older
-- service versions could put the provider token there. Generate a new opaque
-- route key for every binding, update both storage representations atomically,
-- and invalidate the old callback URL. Operators must update provider callback
-- URLs after this migration.
-- pgcrypto is an operator-owned prerequisite for cryptographically random
-- route capabilities. Do not fall back to random()/md5(): a webhook route
-- key is an ingress capability and must remain infeasible to guess.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE tenant_channels
    ADD COLUMN IF NOT EXISTS channel_index INTEGER,
    ADD COLUMN IF NOT EXISTS webhook_key VARCHAR(128);

WITH ranked AS (
    SELECT id,
           ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY id) - 1 AS index_value
    FROM tenant_channels
)
UPDATE tenant_channels tc
SET channel_index = ranked.index_value
FROM ranked
WHERE tc.id = ranked.id
  AND tc.channel_index IS NULL;

DO $$
DECLARE
    binding RECORD;
    config_value JSONB;
    channels_value JSONB;
    item JSONB;
    route_key TEXT;
    candidate_item JSONB;
    candidate_index INTEGER;
    candidate_count INTEGER;
    resolved_index INTEGER;
BEGIN
    FOR binding IN
        SELECT tc.id, tc.tenant_id, tc.channel_type, tc.channel_index,
               tc.webhook_token, tc.config AS channel_config
        FROM tenant_channels tc
        ORDER BY tc.tenant_id, tc.id
        FOR UPDATE OF tc
    LOOP
        IF binding.channel_index IS NULL OR binding.channel_index < 0 THEN
            RAISE EXCEPTION 'tenant channel % has no valid channel index', binding.id;
        END IF;

        -- Re-read the current tenant JSON for every binding. A tenant may own
        -- multiple channels; using the FOR cursor's initial snapshot here
        -- would let a later iteration overwrite an earlier key update.
        SELECT t.config
        INTO config_value
        FROM tenants t
        WHERE t.id = binding.tenant_id
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'tenant channel % references a missing tenant', binding.id;
        END IF;
        channels_value := config_value -> 'channels';
        IF jsonb_typeof(channels_value) <> 'array'
           OR jsonb_array_length(channels_value) = 0 THEN
            RAISE EXCEPTION 'tenant channel % cannot be mapped to tenant JSON configuration', binding.id;
        END IF;

        -- Prefer the immutable channel config to the physical row order. A
        -- row-order fallback is allowed only when the type is unique; an
        -- ambiguous history must be repaired by an operator rather than
        -- silently routing a callback to the wrong account.
        candidate_index := -1;
        candidate_count := 0;
        resolved_index := -1;
        FOR candidate_index IN 0..jsonb_array_length(channels_value) - 1 LOOP
            candidate_item := channels_value -> candidate_index;
            IF COALESCE(candidate_item ->> 'type', '') = binding.channel_type
               AND COALESCE(candidate_item -> 'config', '{}'::jsonb)
                   = COALESCE(binding.channel_config, '{}'::jsonb) THEN
                candidate_count := candidate_count + 1;
                resolved_index := candidate_index;
            END IF;
        END LOOP;
        IF candidate_count <> 1 THEN
            item := channels_value -> binding.channel_index;
            IF candidate_count = 0
               AND COALESCE(item ->> 'type', '') = binding.channel_type
               AND (SELECT count(*)
                    FROM jsonb_array_elements(channels_value) AS channel(value)
                    WHERE COALESCE(channel.value ->> 'type', '') = binding.channel_type) = 1 THEN
                resolved_index := binding.channel_index;
            ELSE
                RAISE EXCEPTION 'tenant channel % cannot be mapped uniquely to tenant JSON configuration', binding.id;
            END IF;
        END IF;
        item := channels_value -> resolved_index;

        -- Never preserve a legacy JSON webhookKey: old versions could have
        -- populated it from a provider credential. Generate a fresh opaque
        -- capability for every row, even when a key is already present.
        -- gen_random_uuid is provided by supported PostgreSQL releases without
        -- requiring provider credentials or application-side key material.
        -- Two UUIDs retain at least 244 random bits after version/variant bits.
        route_key := 'route_' || binding.id::text || '_' ||
            replace(gen_random_uuid()::text, '-', '') ||
            replace(gen_random_uuid()::text, '-', '');
        item := jsonb_set(item, '{webhookKey}', to_jsonb(route_key), true);
        channels_value := jsonb_set(channels_value, ARRAY[resolved_index::text], item, false);
        config_value := jsonb_set(config_value, '{channels}', channels_value, false);
        UPDATE tenants
        SET config = config_value,
            config_version = config_version + 1,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = binding.tenant_id;

        UPDATE tenant_channels
        SET channel_index = resolved_index,
            webhook_key = route_key,
            webhook_token = route_key
        WHERE id = binding.id;
    END LOOP;
END
$$;

ALTER TABLE tenant_channels
    ALTER COLUMN channel_index SET NOT NULL,
    ALTER COLUMN webhook_key SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_tenant_channels_webhook_key
    ON tenant_channels(webhook_key);

COMMENT ON COLUMN tenant_channels.webhook_key IS
    'Opaque callback routing capability; never a provider verification token';
