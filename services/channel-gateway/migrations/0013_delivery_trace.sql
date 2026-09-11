-- Immutable observation metadata; existing rows remain NULL.
ALTER TABLE gateway_delivery_intents
 ADD COLUMN traceparent text,
 ADD COLUMN tracestate text,
 ADD CONSTRAINT gateway_delivery_intents_trace_bounds CHECK (
 (traceparent IS NULL OR octet_length(traceparent) <= 512) AND
 (tracestate IS NULL OR octet_length(tracestate) <= 512));
