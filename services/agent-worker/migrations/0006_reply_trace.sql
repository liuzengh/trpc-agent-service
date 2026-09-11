-- Immutable observation metadata; existing rows remain NULL.
ALTER TABLE execution_reply_outbox
 ADD COLUMN traceparent text,
 ADD COLUMN tracestate text,
 ADD CONSTRAINT execution_reply_outbox_trace_bounds CHECK (
 (traceparent IS NULL OR octet_length(traceparent) <= 512) AND
 (tracestate IS NULL OR octet_length(tracestate) <= 512));
