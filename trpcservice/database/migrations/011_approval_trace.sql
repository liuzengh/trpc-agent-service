ALTER TABLE tool_approval
    ADD COLUMN origin_traceparent VARCHAR(255) NOT NULL DEFAULT '';
