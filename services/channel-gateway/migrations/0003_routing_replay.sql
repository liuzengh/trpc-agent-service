-- Full-history, single-subject Control routing replay. Instance identity is
-- trusted NATS StreamInfo.Created, not an event-controlled field.
CREATE TABLE gateway_route_replay_state (
 singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
 stream_name text NOT NULL,
 stream_id text NOT NULL,
 target_sequence bigint NOT NULL CHECK (target_sequence >= 0),
 contiguous_sequence bigint NOT NULL DEFAULT 0 CHECK (contiguous_sequence >= 0),
 highest_sequence bigint NOT NULL DEFAULT 0 CHECK (highest_sequence >= 0),
 last_observed_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE gateway_route_stream_receipts (
 stream_name text NOT NULL,
 stream_id text NOT NULL,
 sequence bigint NOT NULL CHECK (sequence > 0),
 event_id text,
 digest text NOT NULL,
 status text NOT NULL CHECK (status IN ('APPLIED','QUARANTINED')),
 reason text NOT NULL DEFAULT '',
 received_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (stream_name,stream_id,sequence)
);
CREATE TABLE gateway_route_quarantines (
 quarantine_id bigserial PRIMARY KEY,
 stream_name text NOT NULL,
 stream_id text NOT NULL,
 sequence bigint NOT NULL CHECK (sequence >= 0),
 reason text NOT NULL,
 digest text NOT NULL,
 observed_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE (stream_name,stream_id,sequence,reason,digest)
);
