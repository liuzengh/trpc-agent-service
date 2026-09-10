ALTER TABLE public.runtime_reply_correlation
    ADD COLUMN trace_parent TEXT NOT NULL DEFAULT ''
    CHECK (length(trace_parent) <= 512);
