-- Preserve the deployed Telegram task tables and SQL history. A completed
-- WeCom connection probe has three provider-specific checks, not eight copied
-- Telegram checks. Missing provider/policy/consent must not pass via SQL NULL.
ALTER TABLE channel_preflights DROP CONSTRAINT channel_preflights_check14;
ALTER TABLE channel_preflights ADD CONSTRAINT channel_preflights_provider_result CHECK (
    (
        state = 'COMPLETED'
        AND jsonb_typeof(record_jsonb->'view'->'checks') = 'array'
        AND record_jsonb->'view'->>'checked_at' IS NOT NULL
        AND record_jsonb->'view'->>'expires_at' IS NOT NULL
        AND (
            (record_jsonb->'view'->>'provider' = 'telegram'
             AND jsonb_array_length(record_jsonb->'view'->'checks') = 8)
            OR
            (record_jsonb->'view'->>'provider' = 'wecom'
             AND record_jsonb->'view'->>'diagnostic_policy' = 'wecom_long_connection_v1'
             AND record_jsonb->'view'->>'receive_mode' = 'long_connection'
             AND record_jsonb->'view'->'allow_connection_probe' = 'true'::jsonb
             AND jsonb_array_length(record_jsonb->'view'->'checks') = 3)
        ) IS TRUE
    )
    OR
    (state <> 'COMPLETED'
     AND record_jsonb->'view'->'checks' = '[]'::jsonb
     AND record_jsonb->'view'->'checked_at' = 'null'::jsonb
     AND record_jsonb->'view'->'expires_at' = 'null'::jsonb)
);
