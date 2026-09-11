-- Gateway-only replay metadata. Historical Admission rows deliberately remain NULL.
ALTER TABLE gateway_admissions ADD COLUMN reply_origin jsonb;

-- One committed logical Final is a permanent barrier for one execution Run.
CREATE TABLE gateway_delivery_intents (
 intent_id text PRIMARY KEY,
 run_id text NOT NULL UNIQUE,
 digest text NOT NULL,
 intent jsonb NOT NULL,
 target jsonb NOT NULL,
 provider text NOT NULL CHECK(provider IN ('telegram','wecom')),
 account_id text NOT NULL,
 deadline timestamptz NOT NULL,
 part_count integer NOT NULL CHECK(part_count BETWEEN 1 AND 64),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE gateway_delivery_parts (
 part_id text PRIMARY KEY,
 intent_id text NOT NULL REFERENCES gateway_delivery_intents(intent_id),
 part_index integer NOT NULL CHECK(part_index>=0),
 body text NOT NULL,
 state text NOT NULL CHECK(state IN ('PENDING','CLAIMED','CALLING','ACCEPTED','REJECTED','NOT_SENT','UNKNOWN','EXPIRED')),
 attempt_number bigint NOT NULL DEFAULT 0 CHECK(attempt_number>=0),
 preparation_attempts integer NOT NULL DEFAULT 0 CHECK(preparation_attempts>=0),
 preparation_result jsonb,
 claim_token text,
 instance_id text,
 owner_fence jsonb,
 claim_until timestamptz,
 current_attempt_id text,
 calling_until timestamptz,
 next_attempt_at timestamptz DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(intent_id,part_index),
 CHECK(state<>'CLAIMED' OR (claim_token IS NOT NULL AND instance_id IS NOT NULL AND claim_until IS NOT NULL)),
 CHECK(state<>'CALLING' OR (claim_token IS NOT NULL AND instance_id IS NOT NULL AND current_attempt_id IS NOT NULL AND calling_until IS NOT NULL))
);
CREATE INDEX gateway_delivery_due ON gateway_delivery_parts(next_attempt_at,intent_id,part_index) WHERE state IN ('PENDING','NOT_SENT');
CREATE INDEX gateway_delivery_claim_expiry ON gateway_delivery_parts(claim_until) WHERE state='CLAIMED';
CREATE INDEX gateway_delivery_call_expiry ON gateway_delivery_parts(calling_until) WHERE state='CALLING';
CREATE TABLE gateway_delivery_attempts (
 attempt_id text PRIMARY KEY,
 part_id text NOT NULL REFERENCES gateway_delivery_parts(part_id),
 attempt_number bigint NOT NULL CHECK(attempt_number>0),
 claim_token text NOT NULL,
 instance_id text NOT NULL,
 owner_fence jsonb,
 request_id text NOT NULL,
 request_digest text NOT NULL,
 evidence_hash text NOT NULL,
 calling_until timestamptz NOT NULL,
 result jsonb,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 finished_at timestamptz,
 resolved_at timestamptz,
 resolved_observation_id text,
 UNIQUE(part_id,attempt_number)
);
CREATE TABLE gateway_delivery_observations (
 observation_id text PRIMARY KEY,
 attempt_id text NOT NULL REFERENCES gateway_delivery_attempts(attempt_id),
 request_id text NOT NULL,
 request_digest text NOT NULL,
 result jsonb NOT NULL,
 observed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX gateway_delivery_attempt_observations ON gateway_delivery_observations(attempt_id,observed_at,observation_id);

ALTER TABLE gateway_delivery_attempts ADD CONSTRAINT gateway_delivery_resolution_observation_fk FOREIGN KEY(resolved_observation_id) REFERENCES gateway_delivery_observations(observation_id);
