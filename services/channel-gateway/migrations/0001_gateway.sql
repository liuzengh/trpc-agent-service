CREATE TABLE gateway_route_projections (
 provider text NOT NULL, account_id text NOT NULL, generation bigint NOT NULL CHECK (generation>0),
 enabled boolean NOT NULL, snapshot jsonb NOT NULL, digest text NOT NULL,
 updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(provider,account_id)
);
CREATE TABLE gateway_route_receipts(event_id text PRIMARY KEY,digest text NOT NULL,received_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE gateway_inbox (
 provider text NOT NULL, account_id text NOT NULL, event_id text NOT NULL,
 source_digest text NOT NULL, receipt jsonb NOT NULL, received_at timestamptz NOT NULL,
 PRIMARY KEY(provider,account_id,event_id)
);
CREATE TABLE gateway_admissions (
 admission_id text PRIMARY KEY, run_id text UNIQUE NOT NULL, tenant_id text NOT NULL,
 provider text NOT NULL,account_id text NOT NULL,event_id text NOT NULL,
 route jsonb NOT NULL,input jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(provider,account_id,event_id),
 FOREIGN KEY(provider,account_id,event_id) REFERENCES gateway_inbox(provider,account_id,event_id)
);
CREATE TABLE gateway_outbox (
 event_id text PRIMARY KEY REFERENCES gateway_admissions(admission_id),subject text NOT NULL,payload jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),published_at timestamptz,
 claim_token text,claimed_until timestamptz,attempts integer NOT NULL DEFAULT 0,
 next_attempt_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX gateway_outbox_pending ON gateway_outbox(next_attempt_at) WHERE published_at IS NULL;
