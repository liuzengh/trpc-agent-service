-- One durable fixed-window limiter shared by every Gateway replica. This row is
-- locked only after deduplication and is incremented in the acceptance transaction.
CREATE TABLE gateway_admission_budget (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 window_started_at timestamptz NOT NULL DEFAULT now(),
 new_events bigint NOT NULL DEFAULT 0 CHECK(new_events>=0)
);
INSERT INTO gateway_admission_budget(singleton) VALUES(true);
CREATE INDEX gateway_outbox_pending_created ON gateway_outbox(created_at) WHERE published_at IS NULL;
