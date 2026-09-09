-- Demo seed data: one active tenant with one published app, bound to the
-- mock and testweb channels, so the all-in-one demo works with tenant
-- routing enabled.
--
-- Like init.sql, this runs only on first startup of an empty PG volume (the
-- /docker-entrypoint-initdb.d/ mechanism). Existing volumes apply it
-- manually:
--
--   psql "$TRPC_PG_DSN" -f deploy/db/seed.sql
--
-- or recreate the volume: docker compose down -v && docker compose up -d

INSERT INTO tenant (id, name, status) VALUES
    ('00000000-0000-0000-0000-000000000001', 'demo-tenant', 'active');

-- The demo app config drives the per-app Runner assembly: prompt becomes the
-- system instruction, tools.allow whitelists platform tools (the tenant's
-- tool_policy, empty here, would narrow first). Unset model fields inherit
-- tenant.model_config, then the TRPC_MODEL_* env defaults.
INSERT INTO agent_app (id, tenant_id, name, agent_type, config, version, status) VALUES
    ('00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000001',
     'assistant', 'llm',
     '{"prompt": "你是演示助手，回答简洁友好。", "tools": {"allow": ["get_weather", "delete_user_data"]}}',
     1, 'published');

-- Binding paths: the legacy /{channel}/callback paths are the env-configured
-- single-binding defaults. Tenant bindings mount at
-- /callback/{channel}/{binding_id} (design 5.3.1) and carry their own
-- token_ref/aeskey_ref, so each tenant's webhook verifies under its own keys.
INSERT INTO channel_binding (id, tenant_id, channel, app_id, webhook_path, status) VALUES
    ('00000000-0000-0000-0000-000000000201', '00000000-0000-0000-0000-000000000001',
     'mock', '00000000-0000-0000-0000-000000000101', '/mock/callback', 'active'),
    ('00000000-0000-0000-0000-000000000202', '00000000-0000-0000-0000-000000000001',
     'testweb', '00000000-0000-0000-0000-000000000101', '/testweb/callback', 'active'),
    -- token_ref/aeskey_ref resolve via the SecretResolver (data/secrets/ locally)
    ('00000000-0000-0000-0000-000000000203', '00000000-0000-0000-0000-000000000001',
     'wecom', '00000000-0000-0000-0000-000000000101', '/wecom/callback', 'active'),
    -- Multi-tenant demo: a second tenant on the same mock channel, served
    -- through the binding-scoped path. Requires TRPC_MOCK_CHANNEL=true.
    ('00000000-0000-0000-0000-000000000204', '00000000-0000-0000-0000-000000000001',
     'mock', '00000000-0000-0000-0000-000000000101', '/callback/mock/00000000-0000-0000-0000-000000000204', 'active');
