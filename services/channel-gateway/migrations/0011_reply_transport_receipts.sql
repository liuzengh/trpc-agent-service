-- The two-transaction handoff persists Delivery first, transport terminal state
-- second. Identity comes from JetStream stream creation + sequence, never JSON.
CREATE TABLE gateway_reply_transport_receipts (
 stream_name text NOT NULL CHECK(stream_name='REPLY_INTENTS_V1'),
 stream_id text NOT NULL CHECK(length(stream_id) BETWEEN 1 AND 128),
 stream_sequence bigint NOT NULL CHECK(stream_sequence>0),
 raw_digest text NOT NULL CHECK(raw_digest ~ '^sha256:[0-9a-f]{64}$'),
 outcome text NOT NULL CHECK(outcome IN ('ACCEPTED','REJECTED')),
 reason text NOT NULL CHECK(reason IN ('','INVALID_WIRE','CONFLICT','UNAUTHORIZED','EXPIRED','UNSUPPORTED')),
 intent_id text NOT NULL DEFAULT '',
 run_id text NOT NULL DEFAULT '',
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(stream_name,stream_id,stream_sequence),
 CHECK((outcome='ACCEPTED' AND reason='' AND intent_id<>'' AND run_id<>'') OR (outcome='REJECTED' AND reason<>''))
);
