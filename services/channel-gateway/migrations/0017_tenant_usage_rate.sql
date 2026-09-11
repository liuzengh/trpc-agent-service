-- Shared by every Gateway replica. The immutable Run event carries the exact
-- policy snapshot used for this admission; these rows only coordinate counters.
CREATE TABLE gateway_usage_rate_windows (
 tenant_id text NOT NULL,
 subject_kind text NOT NULL CHECK(subject_kind IN ('tenant','user')),
 subject_id text NOT NULL,
 window_start timestamptz NOT NULL,
 request_count bigint NOT NULL CHECK(request_count BETWEEN 1 AND 9007199254740991),
 PRIMARY KEY(tenant_id,subject_kind,subject_id,window_start)
);
CREATE INDEX gateway_usage_rate_gc ON gateway_usage_rate_windows(window_start);
