-- Admission facts and audit intents share the Receipt transaction. These facts
-- are not current authority; no Worker execution capability is granted here.
CREATE TABLE gateway_admission_authorizations (
 provider text NOT NULL,
 account_id text NOT NULL,
 event_id text NOT NULL,
 tenant_id text NOT NULL,
 decision_fact jsonb NOT NULL CHECK(jsonb_typeof(decision_fact)='object'),
 PRIMARY KEY(provider,account_id,event_id),
 FOREIGN KEY(provider,account_id,event_id) REFERENCES gateway_inbox(provider,account_id,event_id),
 CHECK((decision_fact->>'provider') IS NOT DISTINCT FROM provider),
 CHECK((decision_fact->>'account_id') IS NOT DISTINCT FROM account_id),
 CHECK((decision_fact->>'tenant_id') IS NOT DISTINCT FROM tenant_id),
 CHECK(decision_fact->>'decision' IN ('ALLOW','DENIED'))
);
CREATE TABLE gateway_authorization_audit_outbox (
 audit_event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 provider text NOT NULL,
 account_id text NOT NULL,
 event_id text NOT NULL,
 tenant_id text NOT NULL,
 decision_fact jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(provider,account_id,event_id),
 FOREIGN KEY(provider,account_id,event_id) REFERENCES gateway_admission_authorizations(provider,account_id,event_id)
);
