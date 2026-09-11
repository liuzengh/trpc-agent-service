-- First accepted process context is immutable under receipt/run replay.
ALTER TABLE execution_runs
 ADD COLUMN traceparent text,
 ADD COLUMN tracestate text,
 ADD CONSTRAINT execution_runs_trace_bounds CHECK (
  (traceparent IS NULL OR octet_length(traceparent) <= 512) AND
  (tracestate IS NULL OR octet_length(tracestate) <= 512));
