-- Observation metadata only. Historical rows stay NULL; no business digest changes.
ALTER TABLE gateway_outbox
 ADD COLUMN traceparent text,
 ADD COLUMN tracestate text,
 ADD CONSTRAINT gateway_outbox_trace_bounds CHECK (
  (traceparent IS NULL OR octet_length(traceparent) <= 512) AND
  (tracestate IS NULL OR octet_length(tracestate) <= 512));
